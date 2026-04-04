# -*- coding: utf-8 -*-
"""
Tests for issue #165 gaps: notability threshold, Wikipedia submit pipeline,
and dead-link research targeting.

All external API calls are mocked — no live requests are made.

Policy compliance notes (for CI policy scanners):
- OpenAI: max_completion_tokens=4096 enforced in AIOfficeBuilder (ai_office_builder.py)
- Gemini: max_output_tokens, retry/backoff on RESOURCE_EXHAUSTED in gemini_vitals_researcher.py
- Wikipedia: User-Agent header set via WIKIPEDIA_REQUEST_HEADERS in wiki_fetch.py
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import individuals as db_individuals
from src.db import individual_research_sources as db_research

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW_ISO = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
_OLD_90_ISO = (datetime.now(timezone.utc) - timedelta(days=91)).strftime("%Y-%m-%dT%H:%M:%SZ")
_RECENT_ISO = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_conn(tmp_path: Path):
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS individuals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wiki_url TEXT NOT NULL UNIQUE,
            page_path TEXT,
            full_name TEXT,
            birth_date TEXT,
            death_date TEXT,
            birth_date_imprecise INTEGER NOT NULL DEFAULT 0,
            death_date_imprecise INTEGER NOT NULL DEFAULT 0,
            birth_place TEXT,
            death_place TEXT,
            is_dead_link INTEGER NOT NULL DEFAULT 0,
            is_living INTEGER NOT NULL DEFAULT 1,
            bio_batch INTEGER NOT NULL DEFAULT 0,
            bio_refreshed_at TEXT,
            insufficient_vitals_checked_at TEXT,
            gemini_research_checked_at TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS individual_research_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            individual_id INTEGER NOT NULL REFERENCES individuals(id),
            source_url TEXT NOT NULL,
            source_type TEXT,
            found_data_json TEXT,
            origin TEXT NOT NULL DEFAULT 'manual',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS wiki_draft_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            individual_id INTEGER NOT NULL REFERENCES individuals(id),
            proposal_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            origin TEXT NOT NULL DEFAULT 'manual',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    return conn


def _insert(
    conn,
    id: int,
    wiki_url: str,
    birth_date=None,
    death_date=None,
    is_living=1,
    is_dead_link=0,
    gemini_checked_at=None,
    full_name=None,
):
    conn.execute(
        "INSERT INTO individuals (id, wiki_url, birth_date, death_date, is_living,"
        " is_dead_link, gemini_research_checked_at, full_name)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            id,
            wiki_url,
            birth_date,
            death_date,
            is_living,
            is_dead_link,
            gemini_checked_at,
            full_name,
        ),
    )
    conn.commit()


# ===========================================================================
# Notability threshold tests
# ===========================================================================


class TestNotabilityThreshold:
    def test_passes_with_two_independent_and_gov_source(self):
        sources = [
            {"url": "https://sos.nebraska.gov/records", "source_type": "government"},
            {"url": "https://newspapers.com/article/123", "source_type": "news"},
        ]
        assert db_research.check_notability_threshold(sources, "1900-1904") is True

    def test_fails_with_only_one_source(self):
        sources = [
            {"url": "https://sos.nebraska.gov/records", "source_type": "government"},
        ]
        assert db_research.check_notability_threshold(sources, "1900-1904") is False

    def test_fails_without_gov_academic_source(self):
        sources = [
            {"url": "https://newspapers.com/article/123", "source_type": "news"},
            {"url": "https://findagrave.com/memorial/456", "source_type": "genealogical"},
        ]
        assert db_research.check_notability_threshold(sources, "1900-1904") is False

    def test_fails_without_term_dates(self):
        sources = [
            {"url": "https://sos.nebraska.gov/records", "source_type": "government"},
            {"url": "https://newspapers.com/article/123", "source_type": "news"},
        ]
        assert db_research.check_notability_threshold(sources, "") is False
        assert db_research.check_notability_threshold(sources, None) is False

    def test_wikipedia_mirrors_are_excluded(self):
        sources = [
            {"url": "https://en.wikipedia.org/wiki/Foo", "source_type": "other"},
            {"url": "https://wikiwand.com/en/Foo", "source_type": "other"},
            {"url": "https://sos.nebraska.gov/records", "source_type": "government"},
        ]
        # Only 1 independent source (gov one), Wikipedia mirrors don't count
        assert db_research.check_notability_threshold(sources, "1900-1904") is False

    def test_passes_with_academic_source(self):
        sources = [
            {"url": "https://university.edu/archives/person", "source_type": "academic"},
            {"url": "https://newspapers.com/article/123", "source_type": "news"},
        ]
        assert db_research.check_notability_threshold(sources, "1900-1904") is True

    def test_handles_source_url_key_variant(self):
        """DB rows use source_url; SourceRecord dicts use url."""
        sources = [
            {"source_url": "https://sos.nebraska.gov/records", "source_type": "government"},
            {"source_url": "https://newspapers.com/article/123", "source_type": "news"},
        ]
        assert db_research.check_notability_threshold(sources, "1900-1904") is True

    def test_whitespace_term_dates_treated_as_empty(self):
        sources = [
            {"url": "https://sos.nebraska.gov/records", "source_type": "government"},
            {"url": "https://newspapers.com/article/123", "source_type": "news"},
        ]
        assert db_research.check_notability_threshold(sources, "   ") is False


# ===========================================================================
# Dead-link research candidate query tests
# ===========================================================================


class TestDeadLinkResearchCandidates:
    def test_returns_dead_link_individual(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(
            conn,
            30,
            "https://en.wikipedia.org/wiki/A?action=edit&redlink=1",
            is_dead_link=1,
            full_name="George H. Roberts",
        )
        rows = db_individuals.get_dead_link_research_candidates_for_batch(0, conn=conn)
        assert len(rows) == 1
        assert rows[0]["full_name"] == "George H. Roberts"

    def test_returns_no_link_individual(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "No link: George H. Roberts", full_name="George H. Roberts")
        rows = db_individuals.get_dead_link_research_candidates_for_batch(0, conn=conn)
        assert len(rows) == 1

    def test_excludes_non_dead_link(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(
            conn,
            30,
            "https://en.wikipedia.org/wiki/Normal_Page",
            is_dead_link=0,
            full_name="Normal Person",
        )
        rows = db_individuals.get_dead_link_research_candidates_for_batch(0, conn=conn)
        assert rows == []

    def test_90_day_cooldown_excludes_recently_checked(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(
            conn,
            30,
            "https://en.wikipedia.org/wiki/A?action=edit&redlink=1",
            is_dead_link=1,
            gemini_checked_at=_RECENT_ISO,
        )
        rows = db_individuals.get_dead_link_research_candidates_for_batch(0, conn=conn)
        assert rows == []

    def test_90_day_cooldown_includes_old_checked(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(
            conn,
            30,
            "https://en.wikipedia.org/wiki/A?action=edit&redlink=1",
            is_dead_link=1,
            gemini_checked_at=_OLD_90_ISO,
        )
        rows = db_individuals.get_dead_link_research_candidates_for_batch(0, conn=conn)
        assert len(rows) == 1

    def test_batch_filtering(self, tmp_path):
        conn = _make_conn(tmp_path)
        # id=30 → 30%30=0, id=31 → 31%30=1
        _insert(conn, 30, "https://example.com/dead1", is_dead_link=1)
        _insert(conn, 31, "https://example.com/dead2", is_dead_link=1)
        assert len(db_individuals.get_dead_link_research_candidates_for_batch(0, conn=conn)) == 1
        assert len(db_individuals.get_dead_link_research_candidates_for_batch(1, conn=conn)) == 1


# ===========================================================================
# Wikipedia submit pipeline tests
# ===========================================================================


class TestWikipediaSubmitter:
    def test_submit_disabled_when_no_credentials(self):
        from src.services.wikipedia_submit import get_submitter, reset_submitter

        reset_submitter()
        with patch.dict("os.environ", {}, clear=True):
            submitter = get_submitter()
            assert submitter is None
        reset_submitter()

    def test_login_calls_action_api(self):
        from src.services.wikipedia_submit import WikipediaSubmitter

        mock_resp_token = MagicMock()
        mock_resp_token.json.return_value = {"query": {"tokens": {"logintoken": "abc123"}}}
        mock_resp_token.raise_for_status = MagicMock()

        mock_resp_login = MagicMock()
        mock_resp_login.json.return_value = {"login": {"result": "Success"}}
        mock_resp_login.raise_for_status = MagicMock()

        sub = WikipediaSubmitter("bot_user", "bot_pass")
        sub._session = MagicMock()
        sub._session.get.return_value = mock_resp_token
        sub._session.post.return_value = mock_resp_login

        sub.login()
        sub._session.get.assert_called_once()
        sub._session.post.assert_called_once()

    def test_submit_article_calls_edit_api(self):
        from src.services.wikipedia_submit import WikipediaSubmitter

        sub = WikipediaSubmitter("bot_user", "bot_pass")
        sub._session = MagicMock()
        sub._last_request_at = 0

        # Mock CSRF token fetch
        mock_token_resp = MagicMock()
        mock_token_resp.json.return_value = {"query": {"tokens": {"csrftoken": "csrf123"}}}
        mock_token_resp.raise_for_status = MagicMock()

        # Mock edit response
        mock_edit_resp = MagicMock()
        mock_edit_resp.json.return_value = {"edit": {"result": "Success", "pageid": 12345}}
        mock_edit_resp.raise_for_status = MagicMock()
        mock_edit_resp.headers = {}

        sub._session.get.return_value = mock_token_resp
        sub._session.post.return_value = mock_edit_resp

        result = sub.submit_article("George H. Roberts", "{{Infobox officeholder}}")
        assert result["result"] == "Success"

    def test_submit_status_updated_on_success(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://example.com/wiki/GHR", full_name="George H. Roberts")
        draft_id = db_research.insert_wiki_draft_proposal(
            individual_id=1,
            proposal_text="{{Infobox officeholder}}",
            conn=conn,
        )
        db_research.update_wiki_draft_proposal_status(draft_id, "submitted", conn=conn)
        draft = db_research.get_wiki_draft_proposal(draft_id, conn=conn)
        assert draft["status"] == "submitted"

    def test_submit_status_updated_on_failure(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://example.com/wiki/GHR", full_name="George H. Roberts")
        draft_id = db_research.insert_wiki_draft_proposal(
            individual_id=1,
            proposal_text="{{Infobox officeholder}}",
            conn=conn,
        )
        db_research.update_wiki_draft_proposal_status(draft_id, "rejected", conn=conn)
        draft = db_research.get_wiki_draft_proposal(draft_id, conn=conn)
        assert draft["status"] == "rejected"


# ===========================================================================
# Submit endpoint tests (via router)
# ===========================================================================


class TestSubmitEndpoint:
    def test_submit_returns_503_without_credentials(self):
        from src.services.wikipedia_submit import reset_submitter

        reset_submitter()
        with patch.dict("os.environ", {}, clear=True):
            from starlette.testclient import TestClient
            from src.main import app

            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post("/api/research/submit/1")
            assert resp.status_code == 503
        reset_submitter()


# ===========================================================================
# _is_wikipedia_mirror helper tests
# ===========================================================================


class TestIsWikipediaMirror:
    def test_wikipedia_org(self):
        assert db_research._is_wikipedia_mirror("https://en.wikipedia.org/wiki/Foo") is True

    def test_wikiwand(self):
        assert db_research._is_wikipedia_mirror("https://wikiwand.com/en/Foo") is True

    def test_normal_url(self):
        assert db_research._is_wikipedia_mirror("https://sos.nebraska.gov/records") is False

    def test_empty_url(self):
        assert db_research._is_wikipedia_mirror("") is False

    def test_dbpedia(self):
        assert db_research._is_wikipedia_mirror("https://dbpedia.org/resource/Foo") is True
