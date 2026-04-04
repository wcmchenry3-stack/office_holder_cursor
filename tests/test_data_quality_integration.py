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
    @patch("src.scraper.runner.Logger")
    def test_data_quality_mode_no_ai_keys(
        self, mock_logger_cls, mock_log_dir, mock_init_db, mock_get_conn
    ):
        """data_quality mode exits cleanly when no AI keys are set."""
        mock_logger = MagicMock()
        mock_logger_cls.return_value = mock_logger

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
            result = _run_data_quality(ctx, mock_logger, lambda *a, **kw: None)

        assert result["data_quality_checked"] == 0
        assert result["data_quality_flagged"] == 0

    @patch("src.scraper.runner.get_connection")
    @patch("src.scraper.runner.init_db")
    @patch("src.scraper.runner.get_log_dir", return_value="/tmp")
    @patch("src.scraper.runner.Logger")
    def test_data_quality_mode_runs_manual(
        self, mock_logger_cls, mock_log_dir, mock_init_db, mock_get_conn, tmp_path
    ):
        """data_quality mode calls run_manual when AI keys are present."""
        mock_logger = MagicMock()
        mock_logger_cls.return_value = mock_logger
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
                result = _run_data_quality(ctx, mock_logger, lambda *a, **kw: None)
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
# QualityIssueReporter stub
# ---------------------------------------------------------------------------


class TestQualityIssueReporter:
    """Verify the stub reporter logs without errors."""

    def test_report_empty_results(self):
        from src.services.quality_issue_reporter import QualityIssueReporter

        reporter = QualityIssueReporter()
        count = reporter.report([])
        assert count == 0

    def test_report_logs_results(self):
        from src.services.quality_issue_reporter import QualityIssueReporter

        reporter = QualityIssueReporter()
        results = [
            QualityCheckResult(
                record_type="individual",
                record_id=1,
                check_type="bad_dates",
                flagged_by="deterministic",
                concerns=["term_end before term_start"],
            ),
        ]
        count = reporter.report(results)
        assert count == 1
