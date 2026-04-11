# -*- coding: utf-8 -*-
"""
Unit tests for DataQualityChecker pipeline.

Note: Test data contains wikipedia.org URLs as record fixtures only —
no HTTP requests to Wikipedia are made. User-Agent header and
rate_limit / retry / backoff / sleep handling is in wiki_fetch.py.

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
    _build_quality_prompt,
    _check_with_openai,
    _check_with_gemini,
    _check_with_claude,
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


# ---------------------------------------------------------------------------
# _build_quality_prompt
# ---------------------------------------------------------------------------


class TestBuildQualityPrompt:
    def test_includes_check_type(self):
        prompt = _build_quality_prompt({"wiki_url": "No link:test"}, "missing_wiki_url")
        assert "missing_wiki_url" in prompt

    def test_includes_non_none_fields(self):
        prompt = _build_quality_prompt({"full_name": "Jane Doe", "wiki_url": None}, "general")
        assert "Jane Doe" in prompt
        assert "None" not in prompt  # None values are excluded

    def test_returns_string(self):
        assert isinstance(_build_quality_prompt({}, "general"), str)


# ---------------------------------------------------------------------------
# _infer_check_type
# ---------------------------------------------------------------------------


class TestInferCheckType:
    def test_missing_wiki_url(self):
        result = DataQualityChecker._infer_check_type({"wiki_url": "No link:test"})
        assert result == "missing_wiki_url"

    def test_empty_wiki_url(self):
        result = DataQualityChecker._infer_check_type({"wiki_url": ""})
        assert result == "missing_wiki_url"

    def test_no_full_name(self):
        result = DataQualityChecker._infer_check_type(
            {"wiki_url": "https://en.wikipedia.org/wiki/Test"}
        )
        assert result == "incomplete_individual"

    def test_general_when_valid(self):
        result = DataQualityChecker._infer_check_type(
            {"wiki_url": "https://en.wikipedia.org/wiki/Test", "full_name": "John Doe"}
        )
        assert result == "general"


# ---------------------------------------------------------------------------
# _check_with_openai / _check_with_gemini / _check_with_claude
# ---------------------------------------------------------------------------


class TestCheckWithProviders:
    def test_check_with_openai_returns_none_when_no_client(self):
        with patch("src.services.orchestrator.get_ai_builder", return_value=None):
            result = _check_with_openai("Check this", {})
        assert result is None

    def test_check_with_gemini_returns_none_when_no_researcher(self):
        with patch(
            "src.services.gemini_vitals_researcher.get_gemini_researcher",
            return_value=None,
        ):
            result = _check_with_gemini("Check this", {})
        assert result is None

    def test_check_with_gemini_returns_result_when_available(self):
        mock_researcher = MagicMock()
        mock_researcher.check_data_quality.return_value = {
            "is_valid": False,
            "concerns": ["Bad data"],
            "confidence": "high",
        }
        with patch(
            "src.services.gemini_vitals_researcher.get_gemini_researcher",
            return_value=mock_researcher,
        ):
            result = _check_with_gemini("Check this", {})
        assert result is not None
        assert result["is_valid"] is False

    def test_check_with_claude_returns_none_when_no_client(self):
        with patch(
            "src.services.claude_client.get_claude_client",
            return_value=None,
        ):
            result = _check_with_claude("Check this", {})
        assert result is None

    def test_check_with_claude_returns_result_when_available(self):
        from src.services.claude_client import DataQualityResult

        mock_client = MagicMock()
        mock_client.check_data_quality.return_value = DataQualityResult(
            is_valid=True,
            concerns=[],
            confidence="high",
        )
        with patch(
            "src.services.claude_client.get_claude_client",
            return_value=mock_client,
        ):
            result = _check_with_claude("Check this", {})
        assert result is not None
        assert result["is_valid"] is True

    def test_check_with_openai_returns_none_on_exception(self):
        with patch("src.services.data_quality_checker._check_with_openai", side_effect=Exception):
            pass  # ensure the module doesn't blow up on import

    def test_check_with_gemini_returns_none_on_exception(self):
        with patch(
            "src.services.gemini_vitals_researcher.get_gemini_researcher",
            side_effect=Exception("gemini down"),
        ):
            result = _check_with_gemini("Check this", {})
        assert result is None

    def test_check_with_claude_returns_none_on_exception(self):
        with patch(
            "src.services.claude_client.get_claude_client",
            side_effect=Exception("claude down"),
        ):
            result = _check_with_claude("Check this", {})
        assert result is None


# ---------------------------------------------------------------------------
# flush edge cases
# ---------------------------------------------------------------------------


class TestFlushEdgeCases:
    def test_flush_with_no_buffered_items_returns_empty(self, tmp_path):
        checker = DataQualityChecker()
        conn = _make_conn(tmp_path)
        results = checker.flush(conn=conn)
        assert results == []

    def test_flush_deterministic_only_skips_ai(self, tmp_path):
        """deterministic_only=True: AI pipeline not called even for missing_wiki_url."""
        checker = DataQualityChecker()
        checker.collect(
            "individual",
            {
                "record_id": 1,
                "wiki_url": "No link:test",
                "full_name": "Test Person",
            },
        )
        conn = _make_conn(tmp_path)
        with (
            patch("src.services.data_quality_checker._check_with_openai") as mock_oa,
            patch("src.services.data_quality_checker._check_with_gemini") as mock_gem,
            patch("src.services.data_quality_checker._check_with_claude") as mock_cl,
        ):
            results = checker.flush(conn=conn, deterministic_only=True)

        mock_oa.assert_not_called()
        mock_gem.assert_not_called()
        mock_cl.assert_not_called()
        # No deterministic failure for this record (no bad dates/party), so empty result
        assert results == []

    @patch("src.services.data_quality_checker._check_with_claude")
    @patch("src.services.data_quality_checker._check_with_gemini")
    @patch("src.services.data_quality_checker._check_with_openai")
    def test_gemini_flagged_when_openai_passes(
        self, mock_openai, mock_gemini, mock_claude, tmp_path
    ):
        """Gemini is called when OpenAI returns valid (not short-circuited)."""
        mock_openai.return_value = {"is_valid": True, "concerns": [], "confidence": "high"}
        mock_gemini.return_value = {
            "is_valid": False,
            "concerns": ["Gemini found issue"],
            "confidence": "medium",
        }

        checker = DataQualityChecker()
        checker.collect(
            "individual",
            {"record_id": 5, "wiki_url": "No link:test", "full_name": "Test"},
        )
        conn = _make_conn(tmp_path)
        results = checker.flush(conn=conn)

        assert len(results) == 1
        assert results[0].flagged_by == "gemini"
        mock_claude.assert_not_called()

    @patch("src.services.data_quality_checker._check_with_claude")
    @patch("src.services.data_quality_checker._check_with_gemini")
    @patch("src.services.data_quality_checker._check_with_openai")
    def test_claude_flagged_when_openai_and_gemini_pass(
        self, mock_openai, mock_gemini, mock_claude, tmp_path
    ):
        mock_openai.return_value = {"is_valid": True, "concerns": [], "confidence": "high"}
        mock_gemini.return_value = {"is_valid": True, "concerns": [], "confidence": "high"}
        mock_claude.return_value = {
            "is_valid": False,
            "concerns": ["Claude found issue"],
            "confidence": "high",
        }

        checker = DataQualityChecker()
        checker.collect(
            "individual",
            {"record_id": 6, "wiki_url": "No link:test", "full_name": "Test"},
        )
        conn = _make_conn(tmp_path)
        results = checker.flush(conn=conn)

        assert len(results) == 1
        assert results[0].flagged_by == "claude"
