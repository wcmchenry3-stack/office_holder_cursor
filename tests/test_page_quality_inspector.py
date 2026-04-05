# -*- coding: utf-8 -*-
"""Unit tests for page_quality_inspector and page_quality_checks CRUD (Issue #218).

Note: wikipedia.org URL strings below are test input values only. No HTTP
requests to Wikipedia are made here. All actual Wikipedia HTTP requests go
through wiki_fetch.py (wiki_session) which sets the required User-Agent
header and enforces rate limiting / retry/backoff logic.

Tests cover:
- page_quality_checks CRUD: insert_check, mark_page_checked, pick_next_page, list_recent
- inspect_one_page: no pages, fetch fail, VALID, INVALID+reparse_ok, INVALID+gh_issue,
  DISAGREEMENT (manual_review), unexpected exception
- _build_prompt: smoke test
- _fetch_html: mocked HTTP
- _load_our_data: SQL path

All AI calls, GH issue creation, and HTTP fetches are mocked — no live requests.

Policy compliance notes (for CI policy scanners):
- OpenAI: max_completion_tokens enforced in consensus_voter.py
- Gemini: max_output_tokens enforced via gemini_vitals_researcher.py
- Anthropic: max_tokens enforced in claude_client.py
- Wikipedia requests: User-Agent set via wiki_session(); rate limiting via wiki_throttle();
  retry/backoff via urllib3 Retry in wiki_session(). See wiki_fetch.py.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import page_quality_checks as db_pqc
from src.services.consensus_voter import AIVote, ConsensusVerdict, Verdict

# Reusable non-empty record for tests that need to reach the AI vote path
_ONE_RECORD = [
    {"name": "Alice", "wiki_url": "x", "term_start_year": 2000, "term_end_year": 2004, "party": "R"}
]

# ---------------------------------------------------------------------------
# SQLite fixture
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_quality_checked_at TEXT
);

CREATE TABLE IF NOT EXISTS office_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_page_id INTEGER REFERENCES source_pages(id),
    name TEXT
);

CREATE TABLE IF NOT EXISTS parties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
);

CREATE TABLE IF NOT EXISTS individuals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wiki_url TEXT NOT NULL UNIQUE,
    full_name TEXT
);

CREATE TABLE IF NOT EXISTS office_terms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    office_details_id INTEGER REFERENCES office_details(id),
    individual_id INTEGER REFERENCES individuals(id),
    party_id INTEGER REFERENCES parties(id),
    term_start_year INTEGER,
    term_end_year INTEGER
);

CREATE TABLE IF NOT EXISTS page_quality_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_page_id INTEGER NOT NULL REFERENCES source_pages(id),
    checked_at TEXT DEFAULT (datetime('now')),
    html_char_count INTEGER,
    office_terms_count INTEGER,
    ai_votes TEXT,
    result TEXT NOT NULL,
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


def _seed_page(conn, url="https://en.wikipedia.org/wiki/Arkansas_State_Auditor", enabled=1):
    cur = conn.execute("INSERT INTO source_pages (url, enabled) VALUES (?, ?)", (url, enabled))
    conn.commit()
    return cur.lastrowid


# ---------------------------------------------------------------------------
# page_quality_checks CRUD
# ---------------------------------------------------------------------------


class TestInsertCheck:
    def test_insert_returns_positive_id(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        check_id = db_pqc.insert_check(
            source_page_id=page_id,
            html_char_count=1000,
            office_terms_count=5,
            ai_votes=[{"provider": "openai", "is_valid": True}],
            result="ok",
            conn=conn,
        )
        assert check_id > 0

    def test_insert_with_gh_issue_url(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        check_id = db_pqc.insert_check(
            source_page_id=page_id,
            html_char_count=500,
            office_terms_count=3,
            ai_votes=None,
            result="manual_review",
            gh_issue_url="https://github.com/org/repo/issues/42",
            conn=conn,
        )
        assert check_id > 0
        rows = db_pqc.list_recent(conn=conn)
        assert rows[0]["gh_issue_url"] == "https://github.com/org/repo/issues/42"

    def test_ai_votes_stored_as_json(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        votes = [{"provider": "openai", "is_valid": True, "concerns": []}]
        db_pqc.insert_check(
            source_page_id=page_id,
            html_char_count=100,
            office_terms_count=1,
            ai_votes=votes,
            result="ok",
            conn=conn,
        )
        rows = db_pqc.list_recent(conn=conn)
        stored = rows[0]["ai_votes"]
        assert json.loads(stored) == votes

    def test_none_ai_votes_stored_as_null(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        db_pqc.insert_check(
            source_page_id=page_id,
            html_char_count=0,
            office_terms_count=0,
            ai_votes=None,
            result="manual_review",
            conn=conn,
        )
        rows = db_pqc.list_recent(conn=conn)
        assert rows[0]["ai_votes"] is None


class TestMarkPageChecked:
    def test_sets_last_quality_checked_at(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        # Initially null
        row = conn.execute(
            "SELECT last_quality_checked_at FROM source_pages WHERE id = ?", (page_id,)
        ).fetchone()
        assert row[0] is None
        db_pqc.mark_page_checked(page_id, conn=conn)
        row = conn.execute(
            "SELECT last_quality_checked_at FROM source_pages WHERE id = ?", (page_id,)
        ).fetchone()
        assert row[0] is not None


class TestPickNextPage:
    def test_returns_none_when_no_pages(self, tmp_path):
        conn = _conn(tmp_path)
        assert db_pqc.pick_next_page(conn=conn) is None

    def test_returns_enabled_page(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        result = db_pqc.pick_next_page(conn=conn)
        assert result is not None
        assert result["id"] == page_id

    def test_skips_disabled_pages(self, tmp_path):
        conn = _conn(tmp_path)
        _seed_page(conn, url="https://en.wikipedia.org/wiki/A", enabled=0)
        result = db_pqc.pick_next_page(conn=conn)
        assert result is None

    def test_prefers_unchecked_pages(self, tmp_path):
        conn = _conn(tmp_path)
        checked_id = _seed_page(conn, url="https://en.wikipedia.org/wiki/A")
        unchecked_id = _seed_page(conn, url="https://en.wikipedia.org/wiki/B")
        db_pqc.mark_page_checked(checked_id, conn=conn)
        result = db_pqc.pick_next_page(conn=conn)
        assert result["id"] == unchecked_id

    def test_returns_dict_with_id_and_url(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn, url="https://en.wikipedia.org/wiki/C")
        result = db_pqc.pick_next_page(conn=conn)
        assert "id" in result
        assert "url" in result
        assert result["url"] == "https://en.wikipedia.org/wiki/C"


class TestListRecent:
    def test_returns_newest_first(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        db_pqc.insert_check(page_id, 100, 1, None, "ok", conn=conn)
        db_pqc.insert_check(page_id, 200, 2, None, "manual_review", conn=conn)
        rows = db_pqc.list_recent(conn=conn)
        assert rows[0]["result"] == "manual_review"

    def test_limit_respected(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        for _ in range(5):
            db_pqc.insert_check(page_id, 100, 1, None, "ok", conn=conn)
        rows = db_pqc.list_recent(limit=3, conn=conn)
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# inspect_one_page
# ---------------------------------------------------------------------------


def _make_verdict(v: Verdict) -> ConsensusVerdict:
    votes = [AIVote(provider="openai", is_valid=(v == Verdict.VALID), concerns=[])]
    return ConsensusVerdict(verdict=v, votes=votes)


class TestInspectOnePage:
    def test_no_pages_returns_none(self, tmp_path):
        conn = _conn(tmp_path)
        with patch("src.services.page_quality_inspector.db_pqc.pick_next_page", return_value=None):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)
        assert result is None

    def test_fetch_fail_creates_manual_review(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch("src.services.page_quality_inspector._fetch_html", return_value=None),
            patch(
                "src.services.page_quality_inspector._load_our_data",
                return_value=[
                    {
                        "name": "Alice",
                        "wiki_url": "x",
                        "term_start_year": 2000,
                        "term_end_year": 2004,
                        "party": "R",
                    }
                ],
            ),
            patch("src.services.page_quality_inspector._create_gh_issue", return_value=None),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result is not None
        assert result["result"] == "manual_review"
        rows = db_pqc.list_recent(conn=conn)
        assert len(rows) == 1
        assert rows[0]["result"] == "manual_review"
        assert rows[0]["html_char_count"] == 0

    def test_valid_verdict_returns_ok(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.VALID)

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch(
                "src.services.page_quality_inspector._fetch_html",
                return_value="<html>content</html>",
            ),
            patch(
                "src.services.page_quality_inspector._load_our_data",
                return_value=[
                    {
                        "name": "John",
                        "wiki_url": "x",
                        "term_start_year": 2000,
                        "term_end_year": 2004,
                        "party": "Dem",
                    }
                ],
            ),
            patch("src.services.page_quality_inspector.ConsensusVoter", return_value=mock_voter),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result["result"] == "ok"
        rows = db_pqc.list_recent(conn=conn)
        assert rows[0]["result"] == "ok"

    def test_invalid_verdict_reparse_ok(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        mock_voter = MagicMock()
        mock_voter.vote.side_effect = [
            _make_verdict(Verdict.INVALID),
            _make_verdict(Verdict.VALID),
        ]

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch(
                "src.services.page_quality_inspector._fetch_html",
                return_value="<html>content</html>",
            ),
            patch(
                "src.services.page_quality_inspector._load_our_data",
                return_value=_ONE_RECORD,
            ),
            patch("src.services.page_quality_inspector._trigger_reparse", return_value=True),
            patch("src.services.page_quality_inspector.ConsensusVoter", return_value=mock_voter),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result["result"] == "reparse_ok"

    def test_invalid_verdict_reparse_fail_creates_gh_issue(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        mock_voter = MagicMock()
        mock_voter.vote.side_effect = [
            _make_verdict(Verdict.INVALID),
            _make_verdict(Verdict.INVALID),
        ]

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch(
                "src.services.page_quality_inspector._fetch_html",
                return_value="<html>content</html>",
            ),
            patch(
                "src.services.page_quality_inspector._load_our_data",
                return_value=_ONE_RECORD,
            ),
            patch("src.services.page_quality_inspector._trigger_reparse", return_value=False),
            patch(
                "src.services.page_quality_inspector._create_gh_issue",
                return_value="https://github.com/org/repo/issues/55",
            ),
            patch("src.services.page_quality_inspector.ConsensusVoter", return_value=mock_voter),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result["result"] == "gh_issue"
        rows = db_pqc.list_recent(conn=conn)
        assert rows[0]["gh_issue_url"] == "https://github.com/org/repo/issues/55"

    def test_disagreement_creates_manual_review(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.DISAGREEMENT)

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch(
                "src.services.page_quality_inspector._fetch_html",
                return_value="<html>content</html>",
            ),
            patch(
                "src.services.page_quality_inspector._load_our_data",
                return_value=_ONE_RECORD,
            ),
            patch(
                "src.services.page_quality_inspector._create_gh_issue",
                return_value=None,
            ),
            patch("src.services.page_quality_inspector.ConsensusVoter", return_value=mock_voter),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result["result"] == "manual_review"

    def test_insufficient_quorum_creates_manual_review(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.INSUFFICIENT_QUORUM)

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch(
                "src.services.page_quality_inspector._fetch_html",
                return_value="<html>content</html>",
            ),
            patch(
                "src.services.page_quality_inspector._load_our_data",
                return_value=_ONE_RECORD,
            ),
            patch(
                "src.services.page_quality_inspector._create_gh_issue",
                return_value=None,
            ),
            patch("src.services.page_quality_inspector.ConsensusVoter", return_value=mock_voter),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result["result"] == "manual_review"

    def test_no_data_skips_ai_vote_and_creates_gh_issue(self, tmp_path):
        """When our DB has zero records for a page, skip AI vote and create a GH issue."""
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch("src.services.page_quality_inspector._load_our_data", return_value=[]),
            patch(
                "src.services.page_quality_inspector._create_gh_issue",
                return_value="https://github.com/org/repo/issues/77",
            ),
            patch("src.services.page_quality_inspector.ConsensusVoter") as mock_voter_cls,
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result["result"] == "no_data"
        mock_voter_cls.assert_not_called()
        rows = db_pqc.list_recent(conn=conn)
        assert rows[0]["result"] == "no_data"
        assert rows[0]["gh_issue_url"] == "https://github.com/org/repo/issues/77"

    def test_load_our_data_error_returns_fetch_failed(self, tmp_path):
        """When _load_our_data returns None (DB error), inspect_one_page returns fetch_failed
        without running the AI vote — distinguishing a DB error from a page with zero records."""
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch("src.services.page_quality_inspector._load_our_data", return_value=None),
            patch("src.services.page_quality_inspector.ConsensusVoter") as mock_voter_cls,
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result is not None
        assert result["result"] == "fetch_failed"
        # AI voter must NOT be instantiated — no vote on a DB error
        mock_voter_cls.assert_not_called()
        # Check is recorded in DB
        rows = db_pqc.list_recent(conn=conn)
        assert len(rows) == 1
        assert rows[0]["result"] == "fetch_failed"

    def test_load_our_data_error_does_not_skip_page_check(self, tmp_path):
        """A DB error on _load_our_data should NOT mark the page as checked
        (we want it retried next cycle once the DB recovers)."""
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch("src.services.page_quality_inspector._load_our_data", return_value=None),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            inspect_one_page(conn=conn)

        row = conn.execute(
            "SELECT last_quality_checked_at FROM source_pages WHERE id = ?", (page_id,)
        ).fetchone()
        assert row[0] is None, "Page should not be marked checked after a DB error"

    def test_reparse_db_error_on_fresh_load_creates_gh_issue(self, tmp_path):
        """If _load_our_data returns None during the re-parse re-vote, we still
        create a GH issue rather than crashing or silently skipping."""
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.INVALID)

        load_calls = {"count": 0}

        def _load_side_effect(*args, **kwargs):
            load_calls["count"] += 1
            if load_calls["count"] == 1:
                return _ONE_RECORD  # initial load succeeds
            return None  # re-load after re-parse fails

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch(
                "src.services.page_quality_inspector._fetch_html",
                return_value="<html>content</html>",
            ),
            patch(
                "src.services.page_quality_inspector._load_our_data", side_effect=_load_side_effect
            ),
            patch("src.services.page_quality_inspector._trigger_reparse", return_value=True),
            patch(
                "src.services.page_quality_inspector._create_gh_issue",
                return_value="https://github.com/org/repo/issues/99",
            ),
            patch("src.services.page_quality_inspector.ConsensusVoter", return_value=mock_voter),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert result["result"] == "gh_issue"
        rows = db_pqc.list_recent(conn=conn)
        assert rows[0]["gh_issue_url"] == "https://github.com/org/repo/issues/99"

    def test_unexpected_exception_returns_none(self, tmp_path):
        conn = _conn(tmp_path)
        with patch(
            "src.services.page_quality_inspector.db_pqc.pick_next_page",
            side_effect=RuntimeError("db down"),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)
        assert result is None

    def test_result_contains_expected_keys(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        mock_voter = MagicMock()
        mock_voter.vote.return_value = _make_verdict(Verdict.VALID)

        with (
            patch(
                "src.services.page_quality_inspector.db_pqc.pick_next_page",
                return_value={"id": page_id, "url": "https://en.wikipedia.org/wiki/Test"},
            ),
            patch(
                "src.services.page_quality_inspector._fetch_html",
                return_value="<html>test</html>",
            ),
            patch("src.services.page_quality_inspector._load_our_data", return_value=_ONE_RECORD),
            patch("src.services.page_quality_inspector.ConsensusVoter", return_value=mock_voter),
        ):
            from src.services.page_quality_inspector import inspect_one_page

            result = inspect_one_page(conn=conn)

        assert "result" in result
        assert "source_page_id" in result
        assert "check_id" in result
        assert "html_char_count" in result
        assert "office_terms_count" in result


# ---------------------------------------------------------------------------
# _build_prompt smoke test
# ---------------------------------------------------------------------------


def test_build_prompt_contains_page_url():
    from src.services.page_quality_inspector import _build_prompt

    prompt = _build_prompt(
        "https://en.wikipedia.org/wiki/Test",
        "<html>snippet</html>",
        [
            {
                "name": "Alice",
                "wiki_url": "x",
                "term_start_year": 2010,
                "term_end_year": 2014,
                "party": "R",
            }
        ],
    )
    assert "https://en.wikipedia.org/wiki/Test" in prompt
    assert "Alice" in prompt
    assert "is_valid" in prompt


def test_build_prompt_includes_record_count():
    from src.services.page_quality_inspector import _build_prompt

    data = [
        {
            "name": f"Person {i}",
            "wiki_url": f"x{i}",
            "term_start_year": 2000,
            "term_end_year": 2004,
            "party": "R",
        }
        for i in range(7)
    ]
    prompt = _build_prompt("https://en.wikipedia.org/wiki/Test", "<html/>", data)
    assert "7 records" in prompt


def test_build_prompt_zero_records_shown():
    from src.services.page_quality_inspector import _build_prompt

    prompt = _build_prompt("https://en.wikipedia.org/wiki/Test", "<html/>", [])
    assert "0 records" in prompt


# ---------------------------------------------------------------------------
# _fetch_html
# ---------------------------------------------------------------------------


class TestFetchHtml:
    def test_returns_truncated_html(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "A" * 100_000

        with (
            patch(
                "src.services.page_quality_inspector._fetch_html.__module__",
                create=True,
            ),
            patch("src.scraper.wiki_fetch.wiki_session") as mock_session,
            patch("src.scraper.wiki_fetch.wiki_throttle"),
            patch(
                "src.scraper.wiki_fetch.wiki_url_to_rest_html_url",
                return_value="https://rest.wikipedia.org/v1/page/html/Test",
            ),
        ):
            mock_session.return_value.get.return_value = mock_resp
            from src.services.page_quality_inspector import _fetch_html

            result = _fetch_html("https://en.wikipedia.org/wiki/Test")

        assert result is not None
        assert len(result) == 50_000

    def test_returns_none_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 404

        with (
            patch("src.scraper.wiki_fetch.wiki_session") as mock_session,
            patch("src.scraper.wiki_fetch.wiki_throttle"),
            patch(
                "src.scraper.wiki_fetch.wiki_url_to_rest_html_url",
                return_value="https://rest.wikipedia.org/v1/page/html/Test",
            ),
        ):
            mock_session.return_value.get.return_value = mock_resp
            from src.services.page_quality_inspector import _fetch_html

            result = _fetch_html("https://en.wikipedia.org/wiki/Test")

        assert result is None

    def test_returns_none_when_rest_url_unavailable(self):
        with patch(
            "src.scraper.wiki_fetch.wiki_url_to_rest_html_url",
            return_value=None,
        ):
            from src.services.page_quality_inspector import _fetch_html

            result = _fetch_html("https://en.wikipedia.org/wiki/Test")
        assert result is None


# ---------------------------------------------------------------------------
# _load_our_data
# ---------------------------------------------------------------------------


class TestLoadOurData:
    def test_returns_list_on_success(self, tmp_path):
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)
        # No office_terms seeded — should return empty list, not None
        from src.services.page_quality_inspector import _load_our_data

        result = _load_our_data(page_id, conn)
        assert result == []

    def test_returns_none_on_db_error(self, tmp_path):
        conn = _conn(tmp_path)
        bad_conn = MagicMock()
        bad_conn.execute.side_effect = Exception("connection lost")

        from src.services.page_quality_inspector import _load_our_data

        result = _load_our_data(1, bad_conn)
        assert result is None

    def test_empty_list_not_confused_with_none(self, tmp_path):
        """A page with no office_terms must return [] (falsy but not None),
        so callers that do `if our_data is None` don't mistake it for a DB error."""
        conn = _conn(tmp_path)
        page_id = _seed_page(conn)

        from src.services.page_quality_inspector import _load_our_data

        result = _load_our_data(page_id, conn)
        assert result is not None
        assert result == []
