# -*- coding: utf-8 -*-
"""
Parse error reporter: buffers parser failures during a scraping run, then at
end-of-run groups them, analyzes via OpenAI, and creates GitHub issues.

All external API calls go through src/services/github_client.py (GitHub REST)
and src/services/orchestrator.py → AIOfficeBuilder (OpenAI). No direct API
calls are made here.

Design goals:
- Zero risk to the parser: errors in this service are caught and logged, never
  propagated to the scraping run.
- Batched OpenAI calls: failures are grouped by (function_name, error_type) and
  sent in one call per batch (max 100 groups per call) rather than one call per
  failure.
- Two-level deduplication: DB fingerprint check (fast) then GitHub label check
  (safety net after DB reset).

GitHub issue body includes: root cause, HTML snippet, suggested parser fix,
suggested unit + integration tests, and reproduction steps.

--- Policy compliance ---

OpenAI API (via src/services/orchestrator.py → AIOfficeBuilder.analyze_parse_failures):
  - rate_limit / RateLimitError (HTTP 429) handling: exponential backoff in
    AIOfficeBuilder._call_parse_failure_openai (3 retries, 1 s → 2 s → 4 s).
  - max_completion_tokens=4096 set on every call to cap response size.
  - OPENAI_API_KEY never hardcoded; always read via os.environ at runtime.
  See: https://platform.openai.com/docs/guides/rate-limits

GitHub REST API (via src/services/github_client.py):
  - rate_limit / HTTP 429 handling: exponential backoff in GitHubClient._get / _post
    (3 retries, 1 s → 2 s → 4 s).
  - GITHUB_TOKEN never hardcoded; always read via os.environ at runtime.
  See: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
"""

from __future__ import annotations

import hashlib
import logging
import threading
from dataclasses import dataclass, field

import sentry_sdk

logger = logging.getLogger(__name__)

_BATCH_SIZE = 100  # max groups per OpenAI call


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class ParseFailure:
    """One captured silent parser exception."""

    function_name: str  # e.g. "DataCleanup.format_date"
    error_type: str  # e.g. "ValueError"
    traceback_str: str  # full formatted traceback
    wiki_url: str | None  # Wikipedia URL being parsed, if available
    office_name: str | None  # office config name, if available
    html_snippet: str  # first 2000 chars of raw HTML (cell/row)
    date_str: str | None = None  # the input string that failed (if applicable)


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------


class ParseErrorReporter:
    """Collects ParseFailure objects during a run and flushes them at end-of-run.

    Usage:
        reporter = ParseErrorReporter()
        # pass to parser constructors ...
        # at end of run:
        reporter.flush(conn=conn)
    """

    def __init__(self) -> None:
        self._buffer: list[ParseFailure] = []
        self._lock = threading.Lock()

    def collect(self, failure: ParseFailure) -> None:
        """Add one failure to the buffer. Thread-safe."""
        with self._lock:
            self._buffer.append(failure)

    def flush(self, conn=None) -> None:
        """Process all buffered failures: dedup → group → analyze → create issues.

        Called synchronously at end of run. Any exception inside is caught and
        logged so the run result is never affected.
        """
        with self._lock:
            failures = list(self._buffer)
            self._buffer.clear()

        if not failures:
            return

        try:
            self._flush_inner(failures, conn=conn)
        except Exception as _exc:
            sentry_sdk.capture_exception(_exc)
            logger.exception("ParseErrorReporter.flush failed; run result not affected")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _flush_inner(self, failures: list[ParseFailure], conn=None) -> None:
        from src.db import parse_errors as db_parse_errors
        from src.services.github_client import get_github_client
        from src.services.orchestrator import get_ai_builder

        github = get_github_client()
        if github is None:
            logger.debug("ParseErrorReporter: GITHUB_TOKEN not set; skipping issue creation")
            return

        # Step 1: group by fingerprint, dedup against DB (level 1)
        groups: dict[str, list[ParseFailure]] = {}
        for f in failures:
            fp = compute_fingerprint(f.function_name, f.error_type, f.wiki_url)
            if db_parse_errors.find_by_fingerprint(fp, conn=conn):
                continue  # already reported
            if fp not in groups:
                groups[fp] = []
            groups[fp].append(f)

        if not groups:
            return

        sentry_sdk.add_breadcrumb(
            message=f"ParseErrorReporter: dedup check — {len(failures)} failure(s), {len(groups)} new fingerprint(s)",
            level="info",
        )

        # Step 2: dedup against GitHub labels (level 2 — safety net after DB reset)
        new_groups: dict[str, list[ParseFailure]] = {}
        for fp, group_failures in groups.items():
            label = _fingerprint_label(fp)
            existing = github.find_open_issue_by_label(label)
            if existing:
                rep = _pick_representative(group_failures)
                db_parse_errors.insert_report(
                    fingerprint=fp,
                    function_name=rep.function_name,
                    error_type=rep.error_type,
                    wiki_url=rep.wiki_url,
                    office_name=rep.office_name,
                    github_issue_url=existing["html_url"],
                    github_issue_number=existing["number"],
                    conn=conn,
                )
            else:
                new_groups[fp] = group_failures

        if not new_groups:
            return

        # Step 3: analyze + create issues in batches of _BATCH_SIZE
        try:
            ai_builder = get_ai_builder()
        except RuntimeError:
            logger.warning("ParseErrorReporter: OPENAI_API_KEY not set; skipping OpenAI analysis")
            return

        sentry_sdk.add_breadcrumb(
            message=f"ParseErrorReporter: sending {len(new_groups)} group(s) to OpenAI for analysis",
            level="info",
        )
        group_items = list(new_groups.items())
        for batch_start in range(0, len(group_items), _BATCH_SIZE):
            batch = group_items[batch_start : batch_start + _BATCH_SIZE]
            self._process_batch(batch, ai_builder, github, db_parse_errors, conn)

    def _process_batch(
        self,
        batch: list[tuple[str, list[ParseFailure]]],
        ai_builder,
        github,
        db_parse_errors,
        conn,
    ) -> None:
        groups_data = []
        for fp, group_failures in batch:
            rep = _pick_representative(group_failures)
            groups_data.append(
                {
                    "group_id": fp,
                    "function_name": rep.function_name,
                    "error_type": rep.error_type,
                    "wiki_url": rep.wiki_url,
                    "office_name": rep.office_name,
                    "html_snippet": rep.html_snippet[:2000],
                    "date_str": rep.date_str,
                    "traceback": rep.traceback_str[-1000:],
                    "occurrence_count": len(group_failures),
                }
            )

        try:
            analyses = ai_builder.analyze_parse_failures(groups_data)
        except Exception as _exc:
            sentry_sdk.capture_exception(_exc)
            logger.exception("ParseErrorReporter: OpenAI analysis failed for batch")
            return

        fp_to_failures = dict(batch)
        for analysis in analyses:
            fp = analysis.group_id
            group_failures = fp_to_failures.get(fp)
            if group_failures is None:
                continue
            rep = _pick_representative(group_failures)
            label = _fingerprint_label(fp)
            title = f"[Parser Bug] {analysis.title}"
            body = _format_issue_body(analysis, rep, len(group_failures))

            sentry_sdk.add_breadcrumb(
                message=f"ParseErrorReporter: creating GitHub issue for fingerprint {fp[:8]}",
                level="info",
            )
            try:
                issue = github.create_issue(
                    title=title,
                    body=body,
                    labels=["parser-bug", label],
                )
                db_parse_errors.insert_report(
                    fingerprint=fp,
                    function_name=rep.function_name,
                    error_type=rep.error_type,
                    wiki_url=rep.wiki_url,
                    office_name=rep.office_name,
                    github_issue_url=issue["html_url"],
                    github_issue_number=issue["number"],
                    conn=conn,
                )
                logger.info(
                    "ParseErrorReporter: created issue #%d for %s",
                    issue["number"],
                    analysis.title,
                )
            except Exception as _exc:
                sentry_sdk.capture_exception(_exc)
                logger.exception(
                    "ParseErrorReporter: failed to create GitHub issue for fingerprint %s", fp
                )


# ---------------------------------------------------------------------------
# Module-level helper: used by table_parser.py via _emit_parse_failure
# ---------------------------------------------------------------------------


def compute_fingerprint(function_name: str, error_type: str, wiki_url: str | None) -> str:
    """Stable fingerprint for a (function, error_type, url) triple.

    Same inputs always produce the same string. Used as a GitHub label so that
    duplicate issues can be detected even after a DB reset.
    """
    raw = f"{function_name}|{error_type}|{wiki_url or ''}"
    return "pf-" + hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _fingerprint_label(fp: str) -> str:
    return f"parse-error:{fp}"


def _pick_representative(failures: list[ParseFailure]) -> ParseFailure:
    """Select the failure with the shortest HTML snippet as the representative sample."""
    return min(failures, key=lambda f: len(f.html_snippet))


def _format_issue_body(analysis, rep: ParseFailure, occurrence_count: int) -> str:
    """Format the GitHub issue body as Markdown."""
    url_line = f"- **Wikipedia URL:** {rep.wiki_url}" if rep.wiki_url else ""
    office_line = f"- **Office:** {rep.office_name}" if rep.office_name else ""
    date_line = f"- **Input string:** `{rep.date_str!r}`" if rep.date_str else ""

    context_lines = "\n".join(line for line in [url_line, office_line, date_line] if line)

    return f"""## Root Cause

{analysis.root_cause}

## Context

- **Function:** `{rep.function_name}`
- **Error type:** `{rep.error_type}`
- **Occurrences this run:** {occurrence_count}
{context_lines}

## HTML Snippet

```html
{rep.html_snippet[:2000]}
```

## Traceback

```
{rep.traceback_str[-1000:]}
```

## Suggested Fix

{analysis.suggested_fix}

## Suggested Tests

{analysis.suggested_tests}

## Reproduction Steps

{analysis.reproduction_steps}

---
*Auto-generated by ParseErrorReporter. Label: `{_fingerprint_label(compute_fingerprint(rep.function_name, rep.error_type, rep.wiki_url))}`*
"""
