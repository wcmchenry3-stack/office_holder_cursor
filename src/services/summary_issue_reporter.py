# -*- coding: utf-8 -*-
"""Maintain a single GitHub issue that lists all outstanding data quality items.

Instead of one GH issue per event, this reporter aggregates:
  - structural_change_events (unresolved fill-rate drops)
  - suspect_record_flags (result='needs_review')

into one issue that is created on first run and updated body-in-place on
subsequent runs.  The issue is identified by the label ``summary-report``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_SUMMARY_LABEL = "summary-report"
_SUMMARY_TITLE = "[Action Required] Open data quality issues"


def _fmt_date(value) -> str:
    if value is None:
        return "—"
    s = str(value)
    return s[:10]  # YYYY-MM-DD from ISO string or datetime


def _build_body(structural: list[dict], suspects: list[dict]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = len(structural) + len(suspects)

    lines = [
        "## Open Data Quality Issues",
        "",
        f"_Last updated: {now} — **{total}** item(s) require attention._",
        "_Resolve structural changes by updating `office_table_config`."
        " Resolve suspect records by re-scraping the office after confirming the data._",
        "",
    ]

    lines += [
        f"### Structural Changes ({len(structural)})",
        "",
    ]
    if structural:
        lines += [
            "| # | Office | Drop | Page | Detected |",
            "|---|---|---|---|---|",
        ]
        for row in structural:
            drop = f"{row['drop_pp'] * 100:.0f}pp" if row["drop_pp"] else "—"
            page = f"[link]({row['page_url']})" if row.get("page_url") else "—"
            lines.append(
                f"| {row['id']} | {row['office_name'] or '—'} | {drop} | {page} | {_fmt_date(row['created_at'])} |"
            )
    else:
        lines.append("_No unresolved structural changes._")

    lines += [
        "",
        f"### Suspect Records ({len(suspects)})",
        "",
    ]
    if suspects:
        lines += [
            "| # | Office ID | Name | Verdict | Flagged |",
            "|---|---|---|---|---|",
        ]
        for row in suspects:
            lines.append(
                f"| {row['id']} | {row['office_id'] or '—'} | {row['full_name'] or '—'}"
                f" | {row['result']} | {_fmt_date(row['created_at'])} |"
            )
    else:
        lines.append("_No suspect records awaiting review._")

    return "\n".join(lines)


def refresh(conn=None) -> str | None:
    """Create or update the single summary GH issue. Returns the issue URL or None.

    Gracefully degrades when GITHUB_TOKEN is not set.
    """
    from src.services.github_client import get_github_client

    gh = get_github_client()
    if gh is None:
        logger.debug("SummaryIssueReporter: GITHUB_TOKEN not set; skipping")
        return None

    from src.db import structural_change_events as db_sce
    from src.db import suspect_record_flags as db_flags
    from src.db.connection import get_connection

    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        structural = db_sce.list_unresolved(conn=conn)
        suspects = [
            r
            for r in db_flags.list_recent(limit=500, conn=conn)
            if r.get("result") == "needs_review"
        ]

        body = _build_body(structural, suspects)

        existing = gh.find_open_issue_by_title(_SUMMARY_TITLE, _SUMMARY_LABEL)
        if existing:
            number = existing["number"]
            gh.update_issue(number, body)
            url = existing["html_url"]
            logger.info(
                "Updated summary issue #%d (%d items)", number, len(structural) + len(suspects)
            )
        else:
            resp = gh.create_issue(title=_SUMMARY_TITLE, body=body, labels=[_SUMMARY_LABEL])
            url = resp["html_url"]
            logger.info("Created summary issue %s (%d items)", url, len(structural) + len(suspects))

        return url
    except Exception:
        logger.exception("SummaryIssueReporter.refresh failed")
        return None
    finally:
        if own_conn:
            conn.close()
