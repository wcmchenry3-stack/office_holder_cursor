# -*- coding: utf-8 -*-
"""Report data quality issues by creating GitHub issues.

Mirrors the ParseErrorReporter -> GitHubClient pattern with two-level dedup:
  1. DB fingerprint check (fast) — query data_quality_reports.fingerprint
  2. GitHub label check (safety net) — find_open_issue_by_label()

Uses existing GitHubClient.create_issue() — no changes to github_client.py.
Updates data_quality_reports with github_issue_url after creation.
"""

from __future__ import annotations

import logging

from src.db import data_quality_reports as db_dqr
from src.services.data_quality_checker import QualityCheckResult

logger = logging.getLogger(__name__)

_DQ_LABEL = "data-quality"


def _fingerprint_label(fingerprint: str) -> str:
    """Build the GitHub label used for dedup: ``dq:{fingerprint}``."""
    return f"dq:{fingerprint}"


def _build_issue_title(result: QualityCheckResult) -> str:
    """Build a GitHub issue title from a quality check result."""
    name = result.record_type
    return f"[Data Quality] {result.check_type}: {name} #{result.record_id}"


def _build_issue_body(result: QualityCheckResult, record_data: dict | None = None) -> str:
    """Build a Markdown issue body from a quality check result."""
    lines = [
        "## Data Quality Concern",
        "",
        f"**Record type:** {result.record_type}",
        f"**Record ID:** {result.record_id}",
        f"**Check type:** {result.check_type}",
        f"**Flagged by:** {result.flagged_by}",
        "",
        "## Details",
        "",
    ]
    for concern in result.concerns:
        lines.append(f"- {concern}")

    if record_data:
        lines.append("")
        lines.append("## Record Context")
        lines.append("")
        for key in ("full_name", "wiki_url", "office_name", "term_start_year", "term_end_year"):
            val = record_data.get(key)
            if val is not None:
                lines.append(f"- **{key}:** {val}")

    return "\n".join(lines)


class QualityIssueReporter:
    """Create GitHub issues for flagged data quality records.

    Gracefully degrades when GITHUB_TOKEN is not set (logs only).
    """

    def report(
        self,
        results: list[QualityCheckResult],
        conn=None,
        record_data_map: dict[tuple[str, int], dict] | None = None,
    ) -> list[str]:
        """Create GitHub issues for flagged records. Returns issue URLs.

        Args:
            results: QualityCheckResult objects from DataQualityChecker.
            conn: DB connection (optional, creates own if None).
            record_data_map: Optional mapping of (record_type, record_id) to
                the original record_data dict, for richer issue bodies.

        Returns:
            List of created GitHub issue URLs.
        """
        if not results:
            return []

        from src.services.github_client import get_github_client

        github = get_github_client()
        if github is None:
            logger.info(
                "QualityIssueReporter: GITHUB_TOKEN not set; "
                "logging %d issue(s) without GitHub creation.",
                len(results),
            )
            for r in results:
                logger.info(
                    "Data quality issue: %s #%d [%s] flagged by %s — %s",
                    r.record_type,
                    r.record_id,
                    r.check_type,
                    r.flagged_by,
                    "; ".join(r.concerns),
                )
            return []

        from src.db.connection import get_connection

        own_conn = conn is None
        if own_conn:
            conn = get_connection()

        try:
            return self._create_issues(results, github, conn, record_data_map or {})
        except Exception:
            logger.exception("QualityIssueReporter.report failed")
            return []
        finally:
            if own_conn:
                conn.close()

    def _create_issues(
        self,
        results: list[QualityCheckResult],
        github,
        conn,
        record_data_map: dict[tuple[str, int], dict],
    ) -> list[str]:
        """Two-level dedup then create issues."""
        urls: list[str] = []

        for result in results:
            fingerprint = db_dqr.make_fingerprint(
                result.record_type, result.record_id, result.check_type
            )
            label = _fingerprint_label(fingerprint)

            # Level 1: DB fingerprint check
            existing = db_dqr.find_by_fingerprint(fingerprint, conn=conn)
            if existing and existing.get("github_issue_url"):
                logger.debug("Dedup (DB): skipping %s — already has issue", fingerprint)
                continue

            # Level 2: GitHub label check (safety net after DB reset)
            existing_issue = github.find_open_issue_by_label(label)
            if existing_issue:
                issue_url = existing_issue["html_url"]
                issue_number = existing_issue["number"]
                logger.debug("Dedup (GitHub): found existing issue %s for %s", issue_url, fingerprint)
                db_dqr.update_github_issue(
                    fingerprint, issue_url, issue_number, conn=conn
                )
                continue

            # Create new issue
            record_data = record_data_map.get((result.record_type, result.record_id))
            title = _build_issue_title(result)
            body = _build_issue_body(result, record_data)
            labels = [_DQ_LABEL, label]

            try:
                resp = github.create_issue(title=title, body=body, labels=labels)
            except RuntimeError:
                logger.exception("Failed to create GitHub issue for %s", fingerprint)
                continue

            issue_url = resp["html_url"]
            issue_number = resp["number"]

            # Back-link: update the DB report with the issue URL
            db_dqr.update_github_issue(
                fingerprint, issue_url, issue_number, conn=conn
            )

            logger.info("Created GitHub issue %s for %s", issue_url, fingerprint)
            urls.append(issue_url)

        return urls
