# -*- coding: utf-8 -*-
"""Daily page quality inspection service (Issue #218).

Picks one source_page per day (unchecked-first LRU), fetches current
Wikipedia HTML, builds a compact JSON summary of our parsed data, runs
ConsensusVoter, and acts on the verdict.

Verdicts:
  All agree accurate   → mark checked, log result='ok'
  All agree inaccurate → trigger AI office builder re-parse, re-vote:
                           if now accurate → log result='reparse_ok'
                           else            → create GH issue, log result='gh_issue'
  Mixed / quorum fail  → create GH issue for manual review, log result='manual_review'

Wikimedia REST API compliance:
  - User-Agent header set on every request via wiki_session() in wiki_fetch.py.
  - Rate limiting / throttle enforced via wiki_throttle() in wiki_fetch.py.
  - Retry / backoff: 3 attempts, exponential backoff (1 s → 2 s → 4 s) on
    transient HTTP errors (429, 500, 502, 503, 504) via urllib3 Retry in
    wiki_session().
  See: https://www.mediawiki.org/wiki/API:Etiquette

OpenAI API:
  - max_completion_tokens enforced in consensus_voter.py.
  - OPENAI_API_KEY never hardcoded; always read via os.environ at runtime.

Google Gemini API:
  - max_output_tokens enforced via gemini_vitals_researcher.py.
  - GEMINI_OFFICE_HOLDER never hardcoded; always read via os.environ at runtime.

Anthropic Claude API:
  - max_tokens enforced in claude_client.py.
  - ANTHROPIC_API_KEY never hardcoded; always read via os.environ at runtime.
"""

from __future__ import annotations

import json
import logging

from src.db import page_quality_checks as db_pqc
from src.services.consensus_voter import ConsensusVoter, Verdict

logger = logging.getLogger(__name__)

_HTML_CHAR_LIMIT = 50_000
_GH_LABEL = "page-quality"


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------


def _build_prompt(page_url: str, html_snippet: str, our_data: list[dict]) -> str:
    our_json = json.dumps(our_data, default=str)
    return (
        f"You are auditing a political office holders database.\n\n"
        f"Wikipedia page URL: {page_url}\n\n"
        f"Our parsed data for this page (JSON):\n{our_json}\n\n"
        f"Current Wikipedia HTML (first {_HTML_CHAR_LIMIT:,} chars):\n{html_snippet}\n\n"
        "Question: Does our parsed data accurately represent the office holders "
        "shown in the Wikipedia table(s) on this page?\n\n"
        'Return JSON: {"is_valid": bool, "concerns": [str], "confidence": "high"|"medium"|"low"}\n'
        "is_valid=true means our data accurately matches the Wikipedia table.\n"
        "is_valid=false means there are meaningful discrepancies (missing holders, "
        "wrong names, wrong years, etc.)."
    )


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def _fetch_html(page_url: str) -> str | None:
    """Fetch current Wikipedia HTML via REST API. Returns up to _HTML_CHAR_LIMIT chars."""
    try:
        from src.scraper.wiki_fetch import wiki_session, wiki_throttle, wiki_url_to_rest_html_url

        rest_url = wiki_url_to_rest_html_url(page_url)
        if not rest_url:
            logger.warning("page_quality_inspector: cannot build REST URL for %s", page_url)
            return None
        wiki_throttle()
        resp = wiki_session().get(rest_url, timeout=30)
        if resp.status_code != 200:
            logger.warning(
                "page_quality_inspector: HTTP %d for %s", resp.status_code, rest_url
            )
            return None
        return resp.text[:_HTML_CHAR_LIMIT]
    except Exception:
        logger.exception("page_quality_inspector: failed to fetch HTML for %s", page_url)
        return None


def _load_our_data(source_page_id: int, conn) -> list[dict]:
    """Return compact list of our parsed office_terms for this source page."""
    try:
        cur = conn.execute(
            "SELECT i.full_name, i.wiki_url, ot.term_start_year, ot.term_end_year,"
            " p.name AS party"
            " FROM office_terms ot"
            " JOIN office_details od ON od.id = ot.office_details_id"
            " JOIN individuals i ON i.id = ot.individual_id"
            " LEFT JOIN parties p ON p.id = ot.party_id"
            " WHERE od.source_page_id = %s"
            " ORDER BY ot.term_start_year, i.full_name",
            (source_page_id,),
        )
        rows = cur.fetchall()
        return [
            {
                "name": r[0],
                "wiki_url": r[1],
                "term_start_year": r[2],
                "term_end_year": r[3],
                "party": r[4],
            }
            for r in rows
        ]
    except Exception:
        logger.exception(
            "page_quality_inspector: failed to load our data for source_page_id=%d",
            source_page_id,
        )
        return []


# ---------------------------------------------------------------------------
# GH issue helper
# ---------------------------------------------------------------------------


def _create_gh_issue(page_url: str, source_page_id: int, verdict_name: str, ai_summary: str) -> str | None:
    try:
        from src.services.github_client import get_github_client

        gh = get_github_client()
        if gh is None:
            return None
        title = f"[Page quality] Discrepancy detected: {page_url}"
        body = (
            f"## Page quality inspection flagged a discrepancy\n\n"
            f"**Source page URL:** {page_url}\n"
            f"**Source page ID:** {source_page_id}\n"
            f"**Verdict:** {verdict_name}\n\n"
            f"### AI votes\n{ai_summary}\n\n"
            f"Please review the Wikipedia page and re-scrape if needed."
        )
        result = gh.create_issue(title=title, body=body, labels=[_GH_LABEL])
        return result.get("html_url")
    except Exception:
        logger.exception("page_quality_inspector: failed to create GH issue")
        return None


# ---------------------------------------------------------------------------
# Re-parse trigger
# ---------------------------------------------------------------------------


def _trigger_reparse(page_url: str, conn) -> bool:
    """Trigger AI office builder re-parse for the source page. Returns True on success."""
    try:
        from src.services.orchestrator import get_ai_builder
        from src.scraper.table_cache import get_table_html_cached

        builder = get_ai_builder()
        if builder is None:
            logger.warning("page_quality_inspector: AI builder not available for re-parse")
            return False

        table_html = get_table_html_cached(page_url)
        if not table_html:
            logger.warning("page_quality_inspector: no cached HTML for re-parse of %s", page_url)
            return False

        tables_preview: dict = {}
        builder._analyze_page(page_url, tables_preview, [])
        return True
    except Exception:
        logger.exception("page_quality_inspector: re-parse failed for %s", page_url)
        return False


# ---------------------------------------------------------------------------
# Main inspection function
# ---------------------------------------------------------------------------


def inspect_one_page(conn=None) -> dict | None:
    """Pick and inspect one source_page. Returns a result summary dict or None.

    Called by the scheduled task at 09:00 UTC. Never raises — all errors
    are caught and logged.
    """
    from src.db.connection import get_connection

    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        page = db_pqc.pick_next_page(conn=conn)
        if page is None:
            logger.info("page_quality_inspector: no enabled source pages found")
            return None

        source_page_id = page["id"]
        page_url = page["url"]
        logger.info(
            "page_quality_inspector: inspecting source_page_id=%d url=%s",
            source_page_id,
            page_url,
        )

        # Fetch current Wikipedia HTML
        html = _fetch_html(page_url)
        html_char_count = len(html) if html else 0

        # Load our parsed data
        our_data = _load_our_data(source_page_id, conn)
        office_terms_count = len(our_data)

        if not html:
            # Can't inspect without HTML — log as manual_review
            gh_url = _create_gh_issue(
                page_url, source_page_id, "fetch_failed",
                "Could not fetch Wikipedia HTML."
            )
            check_id = db_pqc.insert_check(
                source_page_id=source_page_id,
                html_char_count=0,
                office_terms_count=office_terms_count,
                ai_votes=None,
                result="manual_review",
                gh_issue_url=gh_url,
                conn=conn,
            )
            db_pqc.mark_page_checked(source_page_id, conn=conn)
            return {"result": "manual_review", "source_page_id": source_page_id, "check_id": check_id}

        # Build prompt and run consensus vote
        prompt = _build_prompt(page_url, html, our_data)
        voter = ConsensusVoter()
        verdict_result = voter.vote(prompt=prompt, context={"source_page_id": source_page_id})
        verdict = verdict_result.verdict

        ai_votes_dicts = [
            {
                "provider": v.provider,
                "is_valid": v.is_valid,
                "concerns": v.concerns,
                "confidence": v.confidence,
                "error": v.error,
            }
            for v in verdict_result.votes
        ]
        ai_summary = "\n".join(
            f"- **{v['provider']}**: "
            f"{'accurate' if v['is_valid'] else 'inaccurate' if v['is_valid'] is False else 'unavailable'}"
            f"{' — ' + ', '.join(v['concerns']) if v['concerns'] else ''}"
            for v in ai_votes_dicts
        )

        result_str: str
        gh_url: str | None = None

        if verdict == Verdict.VALID:
            # All agree accurate
            result_str = "ok"
            db_pqc.mark_page_checked(source_page_id, conn=conn)

        elif verdict == Verdict.INVALID:
            # All agree inaccurate — trigger re-parse and re-vote
            logger.info(
                "page_quality_inspector: inaccuracy detected for %s — triggering re-parse",
                page_url,
            )
            _trigger_reparse(page_url, conn)

            # Re-vote with fresh data
            fresh_data = _load_our_data(source_page_id, conn)
            fresh_prompt = _build_prompt(page_url, html, fresh_data)
            fresh_verdict = voter.vote(prompt=fresh_prompt, context={"source_page_id": source_page_id})

            if fresh_verdict.verdict == Verdict.VALID:
                result_str = "reparse_ok"
            else:
                result_str = "gh_issue"
                gh_url = _create_gh_issue(page_url, source_page_id, "invalid_after_reparse", ai_summary)

            db_pqc.mark_page_checked(source_page_id, conn=conn)

        else:
            # DISAGREEMENT or INSUFFICIENT_QUORUM — manual review
            result_str = "manual_review"
            gh_url = _create_gh_issue(page_url, source_page_id, verdict.value, ai_summary)
            db_pqc.mark_page_checked(source_page_id, conn=conn)

        check_id = db_pqc.insert_check(
            source_page_id=source_page_id,
            html_char_count=html_char_count,
            office_terms_count=office_terms_count,
            ai_votes=ai_votes_dicts,
            result=result_str,
            gh_issue_url=gh_url,
            conn=conn,
        )

        logger.info(
            "page_quality_inspector: source_page_id=%d result=%s check_id=%d",
            source_page_id,
            result_str,
            check_id,
        )
        return {
            "result": result_str,
            "source_page_id": source_page_id,
            "check_id": check_id,
            "html_char_count": html_char_count,
            "office_terms_count": office_terms_count,
        }

    except Exception:
        logger.exception("page_quality_inspector: unexpected error")
        return None
    finally:
        if own_conn:
            conn.close()
