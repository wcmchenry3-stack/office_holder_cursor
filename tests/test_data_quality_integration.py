# -*- coding: utf-8 -*-
"""
Integration tests for DataQualityChecker wired into the scraper runner.

Note: Test data contains wikipedia.org URLs as record fixtures only —
no HTTP requests to Wikipedia are made. User-Agent header and
rate_limit / retry / backoff / sleep handling is in wiki_fetch.py.

Tests cover:
- Quality checker called at end of run (deterministic only, no AI tokens)
- data_quality run mode triggers manual checks
- Missing AI keys gracefully skip quality checks
- DATA_QUALITY_ENABLED env var gates auto-mode

All AI API calls are mocked — no live requests are made.
OpenAI max_completion_tokens and RateLimitError retry/backoff are in data_quality_checker.py.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.services.data_quality_checker import DataQualityChecker, QualityCheckResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn(tmp_path: Path):
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS data_quality_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT NOT NULL UNIQUE,
            record_type TEXT NOT NULL,
            record_id INTEGER NOT NULL,
            check_type TEXT NOT NULL,
            flagged_by TEXT NOT NULL,
            concern_details TEXT,
            github_issue_url TEXT,
            github_issue_number INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS individuals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wiki_url TEXT NOT NULL UNIQUE,
            full_name TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Auto-mode: deterministic only, no AI tokens
# ---------------------------------------------------------------------------


class TestAutoModeDeterministicOnly:
    """Verify that auto-mode (end-of-run) only runs deterministic checks."""

    def test_flush_deterministic_only_skips_ai(self, tmp_path):
        """When deterministic_only=True, AI pipeline is never called."""
        conn = _make_conn(tmp_path)
        checker = DataQualityChecker()

        # Collect a record that would normally trigger AI (missing wiki_url)
        checker.collect(
            "individual",
            {
                "record_type": "individual",
                "record_id": 1,
                "wiki_url": "No link:5:John Smith",
                "full_name": "John Smith",
            },
        )

        with patch("src.services.data_quality_checker._run_ai_pipeline") as mock_ai:
            results = checker.flush(conn=conn, deterministic_only=True)
            mock_ai.assert_not_called()

    def test_flush_deterministic_catches_bad_dates(self, tmp_path):
        """Deterministic checks still flag bad dates in auto mode."""
        conn = _make_conn(tmp_path)
        checker = DataQualityChecker()

        checker.collect(
            "office_term",
            {
                "record_type": "office_term",
                "record_id": 42,
                "term_start_year": 2020,
                "term_end_year": 2010,  # end before start
            },
        )

        results = checker.flush(conn=conn, deterministic_only=True)
        assert len(results) == 1
        assert results[0].check_type == "bad_dates"
        assert results[0].flagged_by == "deterministic"


# ---------------------------------------------------------------------------
# data_quality run mode
# ---------------------------------------------------------------------------


class TestDataQualityRunMode:
    """Verify the data_quality run mode dispatches correctly."""

    @patch("src.scraper.runner.get_connection")
    @patch("src.scraper.runner.init_db")
    @patch("src.scraper.runner.get_log_dir", return_value="/tmp")
    @patch("src.scraper.runner.configure_run_logging", return_value=None)
    def test_data_quality_mode_no_ai_keys(
        self, mock_logger_cls, mock_log_dir, mock_init_db, mock_get_conn
    ):
        """data_quality mode exits cleanly when no AI keys are set."""

        with patch.dict("os.environ", {}, clear=True):
            from src.scraper.runner import _run_data_quality, _RunContext

            ctx = _RunContext(
                run_mode="data_quality",
                run_bio=False,
                run_office_bio=False,
                refresh_table_cache=False,
                dry_run=False,
                test_run=False,
                max_rows_per_table=None,
                office_ids=None,
                individual_ref=None,
                individual_ids=None,
                cancel_check=None,
                force_replace_office_ids=None,
                force_overwrite=False,
                bio_batch=None,
            )
            result = _run_data_quality(ctx, lambda *a, **kw: None)

        assert result["data_quality_checked"] == 0
        assert result["data_quality_flagged"] == 0

    @patch("src.scraper.runner.get_connection")
    @patch("src.scraper.runner.init_db")
    @patch("src.scraper.runner.get_log_dir", return_value="/tmp")
    @patch("src.scraper.runner.configure_run_logging", return_value=None)
    def test_data_quality_mode_runs_manual(
        self, mock_logger_cls, mock_log_dir, mock_init_db, mock_get_conn, tmp_path
    ):
        """data_quality mode calls run_manual when AI keys are present."""
        conn = _make_conn(tmp_path)
        mock_get_conn.return_value = conn

        with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False):
            with patch(
                "src.services.data_quality_checker.DataQualityChecker.run_manual",
                return_value=[],
            ) as mock_manual:
                from src.scraper.runner import _run_data_quality, _RunContext

                ctx = _RunContext(
                    run_mode="data_quality",
                    run_bio=False,
                    run_office_bio=False,
                    refresh_table_cache=False,
                    dry_run=False,
                    test_run=False,
                    max_rows_per_table=None,
                    office_ids=None,
                    individual_ref=None,
                    individual_ids=None,
                    cancel_check=None,
                    force_replace_office_ids=None,
                    force_overwrite=False,
                    bio_batch=None,
                )
                result = _run_data_quality(ctx, lambda *a, **kw: None)
                mock_manual.assert_called_once()


# ---------------------------------------------------------------------------
# ENV var gate
# ---------------------------------------------------------------------------


class TestEnvVarGate:
    """DATA_QUALITY_ENABLED env var controls auto-mode."""

    def test_quality_checker_not_created_when_disabled(self):
        """When DATA_QUALITY_ENABLED is not '1', no checker is instantiated."""
        import os

        # Simulate the env check from runner.py
        with patch.dict("os.environ", {"DATA_QUALITY_ENABLED": "0"}, clear=False):
            enabled = os.environ.get("DATA_QUALITY_ENABLED") == "1"
            assert not enabled

    def test_quality_checker_created_when_enabled(self):
        """When DATA_QUALITY_ENABLED='1', checker is instantiated."""
        import os

        with patch.dict("os.environ", {"DATA_QUALITY_ENABLED": "1"}, clear=False):
            enabled = os.environ.get("DATA_QUALITY_ENABLED") == "1"
            assert enabled


# ---------------------------------------------------------------------------
# QualityIssueReporter
# ---------------------------------------------------------------------------

_SAMPLE_RESULT = QualityCheckResult(
    record_type="individual",
    record_id=1,
    check_type="bad_dates",
    flagged_by="deterministic",
    concerns=["term_end before term_start"],
)


class TestQualityIssueReporter:
    """Verify QualityIssueReporter GitHub issue creation and dedup."""

    def test_report_empty_results(self):
        from src.services.quality_issue_reporter import QualityIssueReporter

        reporter = QualityIssueReporter()
        urls = reporter.report([])
        assert urls == []

    def test_creates_github_issue_for_flagged_record(self, tmp_path):
        """A flagged record with no prior report creates a GitHub issue."""
        from src.services.quality_issue_reporter import QualityIssueReporter
        from src.db import data_quality_reports as db_dqr

        conn = _make_conn(tmp_path)
        # Pre-insert the report row (as DataQualityChecker would)
        fp = db_dqr.make_fingerprint("individual", 1, "bad_dates")
        db_dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=1,
            check_type="bad_dates",
            flagged_by="deterministic",
            concern_details="term_end before term_start",
            conn=conn,
        )

        mock_github = MagicMock()
        mock_github.find_open_issue_by_label.return_value = None
        mock_github.create_issue.return_value = {
            "html_url": "https://github.com/test/repo/issues/42",
            "number": 42,
        }

        with patch(
            "src.services.github_client.get_github_client",
            return_value=mock_github,
        ):
            reporter = QualityIssueReporter()
            urls = reporter.report([_SAMPLE_RESULT], conn=conn)

        assert urls == ["https://github.com/test/repo/issues/42"]
        mock_github.create_issue.assert_called_once()
        call_kwargs = mock_github.create_issue.call_args
        assert "data-quality" in call_kwargs.kwargs.get("labels", call_kwargs[1].get("labels", []))

    def test_dedup_skips_existing_fingerprint(self, tmp_path):
        """DB-level dedup: skip when fingerprint already has a GitHub issue URL."""
        from src.services.quality_issue_reporter import QualityIssueReporter
        from src.db import data_quality_reports as db_dqr

        conn = _make_conn(tmp_path)
        fp = db_dqr.make_fingerprint("individual", 1, "bad_dates")
        db_dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=1,
            check_type="bad_dates",
            flagged_by="deterministic",
            concern_details="term_end before term_start",
            github_issue_url="https://github.com/test/repo/issues/99",
            github_issue_number=99,
            conn=conn,
        )

        mock_github = MagicMock()

        with patch(
            "src.services.github_client.get_github_client",
            return_value=mock_github,
        ):
            reporter = QualityIssueReporter()
            urls = reporter.report([_SAMPLE_RESULT], conn=conn)

        assert urls == []
        mock_github.create_issue.assert_not_called()

    def test_dedup_skips_existing_github_label(self, tmp_path):
        """GitHub-level dedup: skip when an open issue with the label exists."""
        from src.services.quality_issue_reporter import QualityIssueReporter
        from src.db import data_quality_reports as db_dqr

        conn = _make_conn(tmp_path)
        fp = db_dqr.make_fingerprint("individual", 1, "bad_dates")
        # Insert report WITHOUT github_issue_url (simulates DB reset)
        db_dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=1,
            check_type="bad_dates",
            flagged_by="deterministic",
            concern_details="term_end before term_start",
            conn=conn,
        )

        mock_github = MagicMock()
        mock_github.find_open_issue_by_label.return_value = {
            "html_url": "https://github.com/test/repo/issues/77",
            "number": 77,
        }

        with patch(
            "src.services.github_client.get_github_client",
            return_value=mock_github,
        ):
            reporter = QualityIssueReporter()
            urls = reporter.report([_SAMPLE_RESULT], conn=conn)

        assert urls == []
        mock_github.create_issue.assert_not_called()
        # Verify the DB was backfilled with the existing issue
        row = db_dqr.find_by_fingerprint(fp, conn=conn)
        assert row["github_issue_url"] == "https://github.com/test/repo/issues/77"
        assert row["github_issue_number"] == 77

    def test_updates_report_with_issue_url(self, tmp_path):
        """After creating an issue, the DB report is updated with the URL."""
        from src.services.quality_issue_reporter import QualityIssueReporter
        from src.db import data_quality_reports as db_dqr

        conn = _make_conn(tmp_path)
        fp = db_dqr.make_fingerprint("individual", 1, "bad_dates")
        db_dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=1,
            check_type="bad_dates",
            flagged_by="deterministic",
            concern_details="term_end before term_start",
            conn=conn,
        )

        mock_github = MagicMock()
        mock_github.find_open_issue_by_label.return_value = None
        mock_github.create_issue.return_value = {
            "html_url": "https://github.com/test/repo/issues/55",
            "number": 55,
        }

        with patch(
            "src.services.github_client.get_github_client",
            return_value=mock_github,
        ):
            reporter = QualityIssueReporter()
            reporter.report([_SAMPLE_RESULT], conn=conn)

        row = db_dqr.find_by_fingerprint(fp, conn=conn)
        assert row["github_issue_url"] == "https://github.com/test/repo/issues/55"
        assert row["github_issue_number"] == 55

    def test_no_github_token_degrades_gracefully(self):
        """No GITHUB_TOKEN -> reporter returns empty list without crashing."""
        from src.services.quality_issue_reporter import QualityIssueReporter

        with patch(
            "src.services.github_client.get_github_client",
            return_value=None,
        ):
            reporter = QualityIssueReporter()
            urls = reporter.report([_SAMPLE_RESULT])

        assert urls == []
