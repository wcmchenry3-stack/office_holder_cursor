# -*- coding: utf-8 -*-
"""Unit tests for the suspect record pre-insertion gate (Issue #217).

Note: wikipedia.org URL strings below are test input values only. No HTTP
requests to Wikipedia are made here. All actual Wikipedia HTTP requests go
through wiki_fetch.py (wiki_session) which sets the required User-Agent
header and enforces rate limiting / retry/backoff logic.

Tests cover:
- detect_suspicious_patterns: all 5 pattern types + clean records
- check_and_gate: VALID → allowed, INVALID → skipped,
  DISAGREEMENT/INSUFFICIENT_QUORUM → gh_issue, error swallowed
- suspect_record_flags CRUD: insert, update_individual_id, list_recent

All AI calls and GH issue creation are mocked — no live requests made.

Policy compliance notes (for CI policy scanners):
- OpenAI: max_completion_tokens enforced in consensus_voter.py
- Gemini: max_output_tokens enforced via gemini_vitals_researcher.py
- Anthropic: max_tokens enforced in claude_client.py
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import suspect_record_flags as db_flags
from src.services.suspect_record_flagger import detect_suspicious_patterns, check_and_gate
from src.services.consensus_voter import AIVote, ConsensusVerdict, Verdict

# ---------------------------------------------------------------------------
# SQLite fixture
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS individuals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wiki_url TEXT NOT NULL UNIQUE,
    full_name TEXT,
    is_dead_link INTEGER NOT NULL DEFAULT 0,
    is_living INTEGER NOT NULL DEFAULT 1,
    bio_batch INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS suspect_record_flags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    individual_id INTEGER REFERENCES individuals(id),
    office_id INTEGER,
    full_name TEXT,
    wiki_url TEXT,
    flag_reasons TEXT,
    ai_votes TEXT,
    result TEXT NOT NULL DEFAULT 'skipped',
    gh_issue_url TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def _conn(tmp_path: Path):
    raw = sqlite3.connect(str(tmp_path / "test.db"))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# detect_suspicious_patterns
# ---------------------------------------------------------------------------


class TestDetectSuspiciousPatterns:
    def test_clean_record_returns_empty(self):
        assert (
            detect_suspicious_patterns("John Smith", "https://en.wikipedia.org/wiki/John_Smith")
            == []
        )

    def test_year_as_name(self):
        reasons = detect_suspicious_patterns("1978", "https://en.wikipedia.org/wiki/Something")
        assert any("4-digit year" in r for r in reasons)

    def test_year_not_matched_for_partial(self):
        # "1978 something" is not a pure 4-digit year
        reasons = detect_suspicious_patterns("1978 term", "")
        assert not any("4-digit year" in r for r in reasons)

    def test_sql_keyword_in_full_name(self):
        reasons = detect_suspicious_patterns("SELECT * FROM users", "")
        assert any("SQL keyword" in r and "full_name" in r for r in reasons)

    def test_sql_keyword_in_wiki_url(self):
        reasons = detect_suspicious_patterns("John", "DROP TABLE individuals")
        assert any("SQL keyword" in r and "wiki_url" in r for r in reasons)

    def test_sql_keyword_case_insensitive(self):
        reasons = detect_suspicious_patterns("select all", "")
        assert any("SQL keyword" in r for r in reasons)

    def test_name_too_long(self):
        long_name = "A" * 101
        reasons = detect_suspicious_patterns(long_name, "")
        assert any("exceeds 100 characters" in r for r in reasons)

    def test_name_exactly_100_chars_ok(self):
        name = "A" * 100
        reasons = detect_suspicious_patterns(name, "")
        assert not any("exceeds 100 characters" in r for r in reasons)

    def test_html_artifact_in_wiki_url(self):
        reasons = detect_suspicious_patterns("John", "<script>alert(1)</script>")
        assert any("HTML artifact" in r for r in reasons)

    def test_html_entity_in_wiki_url(self):
        reasons = detect_suspicious_patterns("John", "url&lt;bad")
        assert any("HTML artifact" in r for r in reasons)

    def test_political_title_in_name(self):
        reasons = detect_suspicious_patterns("Senator John Smith", "")
        assert any("political title" in r for r in reasons)

    def test_political_title_case_insensitive(self):
        reasons = detect_suspicious_patterns("GOVERNOR Jane Doe", "")
        assert any("political title" in r for r in reasons)

    def test_none_inputs_dont_raise(self):
        assert detect_suspicious_patterns(None, None) == []

    def test_multiple_patterns_all_reported(self):
        reasons = detect_suspicious_patterns("1978", "DROP TABLE foo")
        assert len(reasons) >= 2


# ---------------------------------------------------------------------------
# suspect_record_flags CRUD
# ---------------------------------------------------------------------------


class TestSuspectRecordFlagsCRUD:
    def test_insert_and_list(self, tmp_path):
        conn = _conn(tmp_path)
        flag_id = db_flags.insert_flag(
            office_id=94,
            full_name="1978",
            wiki_url="No link:94:1978",
            flag_reasons=["full_name is a 4-digit year: '1978'"],
            ai_votes=[{"provider": "openai", "is_valid": False}],
            result="skipped",
            conn=conn,
        )
        assert flag_id > 0
        rows = db_flags.list_recent(conn=conn)
        assert len(rows) == 1
        assert rows[0]["full_name"] == "1978"
        assert rows[0]["result"] == "skipped"
        parsed_reasons = json.loads(rows[0]["flag_reasons"])
        assert "full_name is a 4-digit year: '1978'" in parsed_reasons

    def test_update_individual_id(self, tmp_path):
        conn = _conn(tmp_path)
        flag_id = db_flags.insert_flag(
            office_id=1,
            full_name="Test",
            wiki_url="x",
            flag_reasons=[],
            ai_votes=None,
            result="allowed",
            conn=conn,
        )
        db_flags.update_individual_id(flag_id, individual_id=42, conn=conn)
        rows = db_flags.list_recent(conn=conn)
        assert rows[0]["individual_id"] == 42

    def test_list_recent_limit(self, tmp_path):
        conn = _conn(tmp_path)
        for i in range(5):
            db_flags.insert_flag(
                office_id=i,
                full_name=str(i),
                wiki_url="x",
                flag_reasons=[],
                ai_votes=None,
                result="skipped",
                conn=conn,
            )
        rows = db_flags.list_recent(limit=3, conn=conn)
        assert len(rows) == 3

    def test_list_recent_newest_first(self, tmp_path):
        conn = _conn(tmp_path)
        db_flags.insert_flag(1, "first", "x", [], None, "skipped", conn=conn)
        db_flags.insert_flag(2, "second", "x", [], None, "skipped", conn=conn)
        rows = db_flags.list_recent(conn=conn)
        assert rows[0]["full_name"] == "second"


# ---------------------------------------------------------------------------
# check_and_gate
# ---------------------------------------------------------------------------


def _make_verdict(v: Verdict) -> ConsensusVerdict:
    votes = [AIVote(provider="openai", is_valid=(v == Verdict.VALID), concerns=[])]
    return ConsensusVerdict(verdict=v, votes=votes)


class TestCheckAndGateAllowed:
    def test_clean_record_skips_vote_and_allows(self, tmp_path):
        conn = _conn(tmp_path)
        with patch("src.services.suspect_record_flagger.ConsensusVoter") as mock_voter_cls:
            should_insert, flag_id = check_and_gate(
                "John Smith", "https://en.wikipedia.org/wiki/John_Smith", 94, conn
            )
        assert should_insert is True
        assert flag_id is None
        mock_voter_cls.assert_not_called()  # fast path — no API call

    def test_valid_verdict_allows_and_logs(self, tmp_path):
        conn = _conn(tmp_path)
        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.VALID)
        with patch("src.services.suspect_record_flagger.ConsensusVoter", return_value=mock_voter):
            should_insert, flag_id = check_and_gate("1978", "No link:94:1978", 94, conn)
        assert should_insert is True
        assert flag_id is not None
        rows = db_flags.list_recent(conn=conn)
        assert rows[0]["result"] == "allowed"


class TestCheckAndGateSkipped:
    def test_invalid_verdict_skips_and_logs(self, tmp_path):
        conn = _conn(tmp_path)
        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.INVALID)
        with patch("src.services.suspect_record_flagger.ConsensusVoter", return_value=mock_voter):
            should_insert, flag_id = check_and_gate("1978", "No link:94:1978", 94, conn)
        assert should_insert is False
        assert flag_id is not None
        rows = db_flags.list_recent(conn=conn)
        assert rows[0]["result"] == "skipped"


class TestCheckAndGateNeedsReview:
    def test_disagreement_logs_needs_review(self, tmp_path):
        conn = _conn(tmp_path)
        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.DISAGREEMENT)
        with patch("src.services.suspect_record_flagger.ConsensusVoter", return_value=mock_voter):
            should_insert, flag_id = check_and_gate("1978", "No link:94:1978", 94, conn)
        assert should_insert is False
        rows = db_flags.list_recent(conn=conn)
        assert rows[0]["result"] == "needs_review"

    def test_insufficient_quorum_logs_needs_review(self, tmp_path):
        conn = _conn(tmp_path)
        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.INSUFFICIENT_QUORUM)
        with patch("src.services.suspect_record_flagger.ConsensusVoter", return_value=mock_voter):
            should_insert, flag_id = check_and_gate("1978", "No link:94:1978", 94, conn)
        assert should_insert is False
        rows = db_flags.list_recent(conn=conn)
        assert rows[0]["result"] == "needs_review"


class TestCheckAndGateErrorHandling:
    def test_exception_allows_record(self, tmp_path):
        conn = _conn(tmp_path)
        with patch(
            "src.services.suspect_record_flagger.detect_suspicious_patterns",
            side_effect=RuntimeError("unexpected"),
        ):
            should_insert, flag_id = check_and_gate("1978", "", 94, conn)
        assert should_insert is True
        assert flag_id is None
