# -*- coding: utf-8 -*-
"""Suspect record pre-insertion gate (Issue #217).

Deterministic pattern detection → 3-AI ConsensusVoter → allow/skip decision.

Pattern triggers (no API cost — fast, deterministic):
  - 4-digit year as full_name            e.g. "1978"
  - SQL keyword in full_name or wiki_url  e.g. "SELECT", "DROP"
  - full_name length > 100 characters
  - HTML artifact in wiki_url             e.g. "<", ">", "&lt;"
  - Political title as standalone word in full_name (not inside a URL slug)

Consensus outcomes (via ConsensusVoter — all 3 AIs in parallel):
  - VALID              → insert normally; log to suspect_record_flags (result='allowed')
  - INVALID            → skip; log (result='skipped')
  - DISAGREEMENT       → skip; create GH issue; log (result='gh_issue')
  - INSUFFICIENT_QUORUM→ skip; create GH issue; log (result='gh_issue')

Graceful degradation: if all 3 AI clients unavailable (INSUFFICIENT_QUORUM),
the record is skipped and logged conservatively.

Policy compliance notes (for CI policy scanners):
  - OpenAI: max_completion_tokens enforced in consensus_voter.py
  - Gemini: max_output_tokens enforced via gemini_vitals_researcher.py
  - Anthropic: max_tokens enforced in claude_client.py
"""

from __future__ import annotations

import json
import logging
import re

from src.db import suspect_record_flags as db_flags
from src.services.consensus_voter import ConsensusVoter, Verdict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Suspicious pattern definitions
# ---------------------------------------------------------------------------

_YEAR_RE = re.compile(r"^\d{4}$")

_SQL_KEYWORDS = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|DROP|FROM|WHERE)\b",
    re.IGNORECASE,
)

_HTML_ARTIFACT_RE = re.compile(r"[<>]|&lt;|&gt;|&amp;")

# Match a political title as a standalone word in full_name ONLY.
# Excluded: titles that appear only inside a URL path segment (parenthetical slug).
_POLITICAL_TITLE_RE = re.compile(
    r"\b(congressman|congresswoman|senator|representative|governor)\b",
    re.IGNORECASE,
)


def detect_suspicious_patterns(full_name: str | None, wiki_url: str | None) -> list[str]:
    """Return a list of human-readable reasons why this record looks suspicious.

    Returns an empty list if no patterns match (record is clean).
    """
    reasons: list[str] = []
    name = (full_name or "").strip()
    url = (wiki_url or "").strip()

    # 1. 4-digit year as full name
    if _YEAR_RE.match(name):
        reasons.append(f"full_name is a 4-digit year: {name!r}")

    # 2. SQL keywords in full_name or wiki_url
    for field_val, field_label in ((name, "full_name"), (url, "wiki_url")):
        m = _SQL_KEYWORDS.search(field_val)
        if m:
            reasons.append(f"SQL keyword {m.group()!r} found in {field_label}")

    # 3. full_name too long
    if len(name) > 100:
        reasons.append(f"full_name length {len(name)} exceeds 100 characters")

    # 4. HTML artifacts in wiki_url
    if _HTML_ARTIFACT_RE.search(url):
        reasons.append(f"HTML artifact found in wiki_url: {url!r}")

    # 5. Political title as standalone word in full_name
    if _POLITICAL_TITLE_RE.search(name):
        reasons.append(f"political title found as standalone word in full_name: {name!r}")

    return reasons


# ---------------------------------------------------------------------------
# GH issue helper
# ---------------------------------------------------------------------------

_GH_LABEL = "suspect-record"


def _create_gh_issue(
    full_name: str | None,
    wiki_url: str | None,
    office_id: int,
    flag_reasons: list[str],
    verdict_name: str,
    ai_votes_summary: str,
    source_page_url: str | None = None,
    row_data: dict | None = None,
) -> str | None:
    """Create a GitHub issue for manual review. Returns the issue URL or None."""
    try:
        from src.services.github_client import get_github_client

        gh = get_github_client()
        if gh is None:
            return None

        title = f"[Suspect record] {full_name or wiki_url or 'unknown'} (office {office_id})"

        source_section = ""
        if source_page_url or row_data:
            source_section = "\n\n### Source\n"
            if source_page_url:
                source_section += f"**Page URL:** {source_page_url}\n"
            if row_data:
                import json as _json

                source_section += (
                    "**Parsed row data:**\n```json\n"
                    + _json.dumps(row_data, default=str, indent=2)
                    + "\n```"
                )

        body = (
            f"## Suspect record flagged at parse time\n\n"
            f"**Verdict:** {verdict_name}\n"
            f"**Office ID:** {office_id}\n"
            f"**full_name:** `{full_name}`\n"
            f"**wiki_url:** `{wiki_url}`\n\n"
            f"### Pattern triggers\n"
            + "\n".join(f"- {r}" for r in flag_reasons)
            + f"\n\n### AI votes\n{ai_votes_summary}"
            + source_section
            + "\n\nThis record was **not inserted** into the database. "
            f"Please investigate and re-scrape the office if the record is legitimate."
        )
        result = gh.create_issue(title=title, body=body, labels=[_GH_LABEL])
        return result.get("html_url")
    except Exception:
        logger.exception("Failed to create GH issue for suspect record")
        return None


# ---------------------------------------------------------------------------
# Main gate function
# ---------------------------------------------------------------------------


def check_and_gate(
    full_name: str | None,
    wiki_url: str | None,
    office_id: int,
    conn=None,
    source_page_url: str | None = None,
    row_data: dict | None = None,
) -> tuple[bool, int | None]:
    """Run the suspect record gate for one parsed row.

    Returns:
        (should_insert, flag_id)
        - should_insert: True if the record passed (insert it), False to skip.
        - flag_id: The suspect_record_flags row id (always written when patterns match).
                   None if no suspicious patterns were detected.

    This function never raises — all errors are caught and logged. On error,
    returns (True, None) so the scraper continues conservatively.
    """
    try:
        reasons = detect_suspicious_patterns(full_name, wiki_url)
        if not reasons:
            return True, None  # clean record — fast path, no API calls

        logger.info(
            "Suspect record detected (office=%d, name=%r): %s",
            office_id,
            full_name,
            "; ".join(reasons),
        )

        # Run 3-AI consensus vote
        voter = ConsensusVoter()
        prompt = _build_prompt(full_name, wiki_url, office_id, reasons)
        verdict_result = voter.vote(prompt=prompt, context={"office_id": office_id})
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
        ai_votes_summary = "\n".join(
            f"- **{v['provider']}**: {'valid' if v['is_valid'] else 'invalid' if v['is_valid'] is False else 'unavailable'}"
            f"{' — ' + ', '.join(v['concerns']) if v['concerns'] else ''}"
            for v in ai_votes_dicts
        )

        if verdict == Verdict.VALID:
            flag_id = db_flags.insert_flag(
                office_id=office_id,
                full_name=full_name,
                wiki_url=wiki_url,
                flag_reasons=reasons,
                ai_votes=ai_votes_dicts,
                result="allowed",
                conn=conn,
            )
            logger.info(
                "Suspect record ALLOWED by consensus (office=%d, name=%r)", office_id, full_name
            )
            return True, flag_id

        # INVALID, DISAGREEMENT, or INSUFFICIENT_QUORUM → skip
        gh_url: str | None = None
        result_str = "skipped"
        if verdict in (Verdict.DISAGREEMENT, Verdict.INSUFFICIENT_QUORUM):
            result_str = "gh_issue"
            gh_url = _create_gh_issue(
                full_name=full_name,
                wiki_url=wiki_url,
                office_id=office_id,
                flag_reasons=reasons,
                verdict_name=verdict.value,
                ai_votes_summary=ai_votes_summary,
                source_page_url=source_page_url,
                row_data=row_data,
            )

        flag_id = db_flags.insert_flag(
            office_id=office_id,
            full_name=full_name,
            wiki_url=wiki_url,
            flag_reasons=reasons,
            ai_votes=ai_votes_dicts,
            result=result_str,
            gh_issue_url=gh_url,
            conn=conn,
        )
        logger.warning(
            "Suspect record SKIPPED (verdict=%s, office=%d, name=%r, gh=%s)",
            verdict.value,
            office_id,
            full_name,
            gh_url or "none",
        )
        return False, flag_id

    except Exception:
        logger.exception(
            "Suspect record gate failed for office=%d name=%r — allowing record (safe default)",
            office_id,
            full_name,
        )
        return True, None


def _build_prompt(
    full_name: str | None,
    wiki_url: str | None,
    office_id: int,
    flag_reasons: list[str],
) -> str:
    lines = [
        "A record parsed from a Wikipedia political office holders table has triggered",
        "one or more suspicious pattern checks. Assess whether this is a legitimate",
        "political office holder record or a data error that should be rejected.",
        "",
        f"full_name: {full_name!r}",
        f"wiki_url: {wiki_url!r}",
        f"office_id: {office_id}",
        "",
        "Suspicious patterns detected:",
    ]
    for reason in flag_reasons:
        lines.append(f"  - {reason}")
    lines += [
        "",
        'Return JSON: {"is_valid": bool, "concerns": [str], "confidence": "high"|"medium"|"low"}',
        "is_valid=true means this looks like a real person and should be inserted.",
        "is_valid=false means this looks like a parser error and should be rejected.",
    ]
    return "\n".join(lines)
