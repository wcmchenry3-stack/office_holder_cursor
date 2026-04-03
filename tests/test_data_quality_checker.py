# -*- coding: utf-8 -*-
"""
Unit tests for DataQualityChecker pipeline.

Note: Test data contains wikipedia.org URLs as record fixtures only —
no HTTP requests to Wikipedia are made. User-Agent header is set in wiki_fetch.py.

Tests cover:
- Pipeline short-circuits on OpenAI failure (Gemini/Claude not called)
- Missing AI clients gracefully skipped
- Deterministic checks catch bad dates without API calls
- Collect is thread-safe
- Flush deduplicates against data_quality_reports table
- Batch size limit respected
- Manual run queries eligible records

All AI API calls are mocked — no live requests are made.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.services.data_quality_checker import (
    DataQualityChecker,
    _check_suspicious_dates,
    _check_missing_wiki_url,
    _check_party_resolution,
    _run_deterministic_checks,
    MAX_BATCH_SIZE,
)

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
# Deterministic checks
# ---------------------------------------------------------------------------


class TestDeterministicChecks:
    def test_bad_dates_end_before_start(self):
        result = _check_suspicious_dates(
            {
                "record_type": "office_term",
                "record_id": 1,
                "term_start_year": 2000,
                "term_end_year": 1990,
            }
        )
        assert result is not None
        assert result.check_type == "bad_dates"
        assert result.flagged_by == "deterministic"
        assert any("before" in c for c in result.concerns)

    def test_bad_dates_excessive_span(self):
        result = _check_suspicious_dates(
            {
                "record_type": "office_term",
                "record_id": 1,
                "term_start_year": 1900,
                "term_end_year": 2000,
            }
        )
        assert result is not None
        assert any("80 years" in c for c in result.concerns)

    def test_bad_dates_future(self):
        result = _check_suspicious_dates(
            {
                "record_type": "office_term",
                "record_id": 1,
                "term_start_year": 2000,
                "term_end_year": 2050,
            }
        )
        assert result is not None
        assert any("future" in c for c in result.concerns)

    def test_valid_dates_pass(self):
        result = _check_suspicious_dates(
            {
                "record_type": "office_term",
                "record_id": 1,
                "term_start_year": 2000,
                "term_end_year": 2008,
            }
        )
        assert result is None

    def test_missing_wiki_url(self):
        assert _check_missing_wiki_url({"wiki_url": "No link:test"}) == "missing_wiki_url"
        assert _check_missing_wiki_url({"wiki_url": ""}) == "missing_wiki_url"
        assert _check_missing_wiki_url({}) == "missing_wiki_url"

    def test_valid_wiki_url(self):
        assert _check_missing_wiki_url({"wiki_url": "https://en.wikipedia.org/wiki/Test"}) is None

    def test_party_resolution_failure(self):
        result = _check_party_resolution(
            {
                "record_type": "office_term",
                "record_id": 1,
                "party_text": "Democratic",
                "party_id": None,
            }
        )
        assert result is not None
        assert result.check_type == "party_resolution_failure"

    def test_party_resolved_passes(self):
        result = _check_party_resolution(
            {
                "record_type": "office_term",
                "record_id": 1,
                "party_text": "Democratic",
                "party_id": 5,
            }
        )
        assert result is None

    def test_deterministic_no_api_calls(self):
        """Deterministic checks should not import or call any AI client."""
        result = _run_deterministic_checks(
            {
                "record_type": "office_term",
                "record_id": 1,
                "term_start_year": 2000,
                "term_end_year": 1990,
            }
        )
        assert result is not None
        assert result.flagged_by == "deterministic"


# ---------------------------------------------------------------------------
# Pipeline short-circuit
# ---------------------------------------------------------------------------


class TestPipelineShortCircuit:
    @patch("src.services.data_quality_checker._check_with_claude")
    @patch("src.services.data_quality_checker._check_with_gemini")
    @patch("src.services.data_quality_checker._check_with_openai")
    def test_short_circuits_on_openai_failure(
        self, mock_openai, mock_gemini, mock_claude, tmp_path
    ):
        mock_openai.return_value = {
            "is_valid": False,
            "concerns": ["Data looks wrong"],
            "confidence": "high",
        }

        checker = DataQualityChecker()
        checker.collect(
            "individual",
            {
                "record_id": 1,
                "wiki_url": "No link:test",
                "full_name": "John Doe",
            },
        )
        conn = _make_conn(tmp_path)
        results = checker.flush(conn=conn)

        assert len(results) == 1
        assert results[0].flagged_by == "openai"
        mock_gemini.assert_not_called()
        mock_claude.assert_not_called()

    @patch("src.services.data_quality_checker._check_with_claude")
    @patch("src.services.data_quality_checker._check_with_gemini")
    @patch("src.services.data_quality_checker._check_with_openai")
    def test_skips_missing_ai_client(self, mock_openai, mock_gemini, mock_claude, tmp_path):
        """When all AI clients return None (not configured), no failure is raised."""
        mock_openai.return_value = None
        mock_gemini.return_value = None
        mock_claude.return_value = None

        checker = DataQualityChecker()
        checker.collect(
            "individual",
            {
                "record_id": 1,
                "wiki_url": "No link:test",
                "full_name": "John Doe",
            },
        )
        conn = _make_conn(tmp_path)
        results = checker.flush(conn=conn)

        assert len(results) == 0


# ---------------------------------------------------------------------------
# Collect thread safety
# ---------------------------------------------------------------------------


class TestCollectThreadSafety:
    def test_concurrent_collect(self):
        checker = DataQualityChecker()
        errors = []

        def collect_many(start):
            try:
                for i in range(100):
                    checker.collect(
                        "individual",
                        {"record_id": start + i, "wiki_url": f"url_{start + i}"},
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=collect_many, args=(i * 100,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(checker._buffer) == 400


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


class TestFlushDedup:
    @patch("src.services.data_quality_checker._check_with_claude")
    @patch("src.services.data_quality_checker._check_with_gemini")
    @patch("src.services.data_quality_checker._check_with_openai")
    def test_deduplicates_against_db(self, mock_openai, mock_gemini, mock_claude, tmp_path):
        conn = _make_conn(tmp_path)

        # Pre-insert a report for record_id=1
        from src.db import data_quality_reports as db_dqr

        fp = db_dqr.make_fingerprint("individual", 1, "missing_wiki_url")
        db_dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=1,
            check_type="missing_wiki_url",
            flagged_by="openai",
            conn=conn,
        )

        mock_openai.return_value = {
            "is_valid": False,
            "concerns": ["Problem found"],
            "confidence": "high",
        }

        checker = DataQualityChecker()
        # Collect same record that's already in DB
        checker.collect(
            "individual",
            {"record_id": 1, "wiki_url": "No link:test", "full_name": "John Doe"},
        )
        # And a new one
        checker.collect(
            "individual",
            {"record_id": 2, "wiki_url": "No link:test2", "full_name": "Jane Doe"},
        )
        results = checker.flush(conn=conn)

        # Only record_id=2 should be processed (record_id=1 already in DB)
        assert len(results) == 1
        assert results[0].record_id == 2


# ---------------------------------------------------------------------------
# Batch limit
# ---------------------------------------------------------------------------


class TestBatchLimit:
    def test_batch_limit_respected(self, tmp_path):
        checker = DataQualityChecker()

        # Collect more than MAX_BATCH_SIZE records with deterministic failures
        for i in range(MAX_BATCH_SIZE + 20):
            checker.collect(
                "office_term",
                {
                    "record_id": i,
                    "record_type": "office_term",
                    "term_start_year": 2000,
                    "term_end_year": 1990,
                },
            )

        conn = _make_conn(tmp_path)
        results = checker.flush(conn=conn)

        assert len(results) <= MAX_BATCH_SIZE


# ---------------------------------------------------------------------------
# Manual run
# ---------------------------------------------------------------------------


class TestManualRun:
    @patch("src.services.data_quality_checker._check_with_claude")
    @patch("src.services.data_quality_checker._check_with_gemini")
    @patch("src.services.data_quality_checker._check_with_openai")
    def test_queries_eligible_records(self, mock_openai, mock_gemini, mock_claude, tmp_path):
        conn = _make_conn(tmp_path)

        # Insert individuals — one with missing URL, one valid
        conn.execute(
            "INSERT INTO individuals (id, wiki_url, full_name) VALUES (?, ?, ?)",
            (1, "No link:test", "John Doe"),
        )
        conn.execute(
            "INSERT INTO individuals (id, wiki_url, full_name) VALUES (?, ?, ?)",
            (2, "https://en.wikipedia.org/wiki/Jane_Doe", "Jane Doe"),
        )
        conn.commit()

        mock_openai.return_value = {
            "is_valid": False,
            "concerns": ["Missing wiki URL"],
            "confidence": "high",
        }

        checker = DataQualityChecker()
        results = checker.run_manual(conn=conn)

        # Only individual with "No link:" should be checked
        assert len(results) == 1
        assert results[0].record_id == 1
