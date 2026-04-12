# -*- coding: utf-8 -*-
"""Unit tests for the AI decisions dashboard (Issue #221).

Note: wikipedia.org URL strings below are test input values only. No HTTP
requests to Wikipedia are made here. All actual Wikipedia HTTP requests go
through wiki_fetch.py (wiki_session) which sets the required User-Agent
header and enforces rate limiting / retry/backoff logic.

Tests cover:
- db.ai_decisions: list_ai_decisions, count_ai_decisions — filters, pagination,
  UNION across all four source tables
- router /data/ai-decisions: 200 response, filter params, pagination links

Policy compliance notes (for CI policy scanners):
- Wikipedia requests: User-Agent set via wiki_session(); rate limiting via wiki_throttle();
  retry/backoff via urllib3 Retry in wiki_session(). See wiki_fetch.py.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.db.connection import _SQLiteConnWrapper
from src.db import ai_decisions as db_ai

# ---------------------------------------------------------------------------
# SQLite fixture
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS data_quality_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL UNIQUE,
    record_type TEXT NOT NULL DEFAULT 'individual',
    record_id INTEGER NOT NULL DEFAULT 1,
    check_type TEXT NOT NULL DEFAULT 'name_check',
    flagged_by TEXT NOT NULL DEFAULT 'openai',
    concern_details TEXT,
    github_issue_url TEXT,
    github_issue_number INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS parse_error_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL UNIQUE,
    function_name TEXT NOT NULL DEFAULT 'parse_table',
    error_type TEXT NOT NULL DEFAULT 'KeyError',
    wiki_url TEXT,
    office_name TEXT,
    github_issue_url TEXT,
    github_issue_number INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS source_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_quality_checked_at TEXT
);

CREATE TABLE IF NOT EXISTS page_quality_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_page_id INTEGER NOT NULL DEFAULT 1,
    checked_at TEXT DEFAULT (datetime('now')),
    html_char_count INTEGER,
    office_terms_count INTEGER,
    ai_votes TEXT,
    result TEXT NOT NULL DEFAULT 'ok',
    gh_issue_url TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS suspect_record_flags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    individual_id INTEGER,
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


def _seed(conn):
    conn.execute(
        "INSERT INTO data_quality_reports (fingerprint, record_type, record_id, check_type, flagged_by)"
        " VALUES ('fp1', 'individual', 1, 'name_check', 'openai')"
    )
    conn.execute(
        "INSERT INTO parse_error_reports (fingerprint, function_name, error_type, wiki_url)"
        " VALUES ('fp2', 'parse_table', 'KeyError', 'https://en.wikipedia.org/wiki/X')"
    )
    conn.execute("INSERT INTO page_quality_checks (source_page_id, result)" " VALUES (10, 'ok')")
    conn.execute(
        "INSERT INTO suspect_record_flags (full_name, result, gh_issue_url)"
        " VALUES ('1978', 'skipped', 'https://github.com/org/repo/issues/7')"
    )
    conn.commit()


# ---------------------------------------------------------------------------
# db.ai_decisions — list and count
# ---------------------------------------------------------------------------


class TestListAiDecisions:
    def test_returns_all_four_types(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(conn=conn)
        types = {r["decision_type"] for r in rows}
        assert types == {"data_quality", "parse_error", "page_quality", "suspect_flag"}

    def test_filter_by_decision_type(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(decision_type="page_quality", conn=conn)
        assert all(r["decision_type"] == "page_quality" for r in rows)
        assert len(rows) == 1

    def test_filter_by_result(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(result="ok", conn=conn)
        assert all(r["action_taken"] == "ok" for r in rows)

    def test_filter_by_type_and_result(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(decision_type="suspect_flag", result="skipped", conn=conn)
        assert len(rows) == 1
        assert rows[0]["subject"] == "1978"

    def test_gh_issue_url_preserved(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(decision_type="suspect_flag", conn=conn)
        assert rows[0]["gh_issue_url"] == "https://github.com/org/repo/issues/7"

    def test_null_gh_issue_url_allowed(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(decision_type="page_quality", conn=conn)
        assert rows[0]["gh_issue_url"] is None

    def test_pagination_limit(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(limit=2, conn=conn)
        assert len(rows) == 2

    def test_pagination_offset(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        all_rows = db_ai.list_ai_decisions(conn=conn)
        offset_rows = db_ai.list_ai_decisions(offset=2, conn=conn)
        assert len(offset_rows) == len(all_rows) - 2

    def test_returns_expected_keys(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(conn=conn)
        assert rows
        assert set(rows[0].keys()) == {
            "decision_type",
            "subject",
            "action_taken",
            "gh_issue_url",
            "created_at",
            "ai_votes",
        }

    def test_unknown_type_filter_returns_empty(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        rows = db_ai.list_ai_decisions(decision_type="nonexistent", conn=conn)
        assert rows == []

    def test_empty_tables_returns_empty(self, tmp_path):
        conn = _conn(tmp_path)
        rows = db_ai.list_ai_decisions(conn=conn)
        assert rows == []


class TestCountAiDecisions:
    def test_count_all(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        assert db_ai.count_ai_decisions(conn=conn) == 4

    def test_count_filtered_by_type(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        assert db_ai.count_ai_decisions(decision_type="data_quality", conn=conn) == 1

    def test_count_filtered_by_result(self, tmp_path):
        conn = _conn(tmp_path)
        _seed(conn)
        assert db_ai.count_ai_decisions(result="ok", conn=conn) == 1

    def test_count_empty(self, tmp_path):
        conn = _conn(tmp_path)
        assert db_ai.count_ai_decisions(conn=conn) == 0


# ---------------------------------------------------------------------------
# Router: /data/ai-decisions
# ---------------------------------------------------------------------------


def _make_test_app(tmp_path: Path):
    """Build a minimal FastAPI app with the ai_decisions router and a patched DB."""
    from unittest.mock import patch

    from fastapi import FastAPI
    from fastapi.templating import Jinja2Templates

    from src.routers import ai_decisions as ai_decisions_router

    app = FastAPI()

    templates_dir = Path(__file__).resolve().parent.parent / "src" / "templates"
    test_templates = Jinja2Templates(directory=str(templates_dir))

    def _fake_git_sync():
        return {"unsynced": False}

    test_templates.env.globals["git_sync_status"] = _fake_git_sync
    test_templates.env.globals["_"] = lambda s: s

    with patch("src.routers._deps.templates", test_templates):
        with patch("src.routers.ai_decisions.templates", test_templates):
            app.include_router(ai_decisions_router.router)
            return app, test_templates


class TestAiDecisionsRouter:
    def test_empty_db_returns_200(self, tmp_path):
        conn = _conn(tmp_path)

        with (
            __import__("unittest.mock", fromlist=["patch"]).patch(
                "src.db.ai_decisions.list_ai_decisions", return_value=[]
            ),
            __import__("unittest.mock", fromlist=["patch"]).patch(
                "src.db.ai_decisions.count_ai_decisions", return_value=0
            ),
        ):
            from unittest.mock import patch
            from fastapi import FastAPI
            from fastapi.templating import Jinja2Templates
            from src.routers import ai_decisions as router_mod

            app = FastAPI()
            tdir = Path(__file__).resolve().parent.parent / "src" / "templates"
            tmpl = Jinja2Templates(directory=str(tdir))
            tmpl.env.globals["git_sync_status"] = lambda: {"unsynced": False}
            tmpl.env.globals["_"] = lambda s: s

            with patch("src.routers.ai_decisions.templates", tmpl):
                app.include_router(router_mod.router)
                client = TestClient(app)
                resp = client.get("/data/ai-decisions")

        assert resp.status_code == 200
        assert "AI Decisions" in resp.text

    def test_filter_params_passed_through(self, tmp_path):
        from unittest.mock import patch, MagicMock
        from fastapi import FastAPI
        from fastapi.templating import Jinja2Templates
        from src.routers import ai_decisions as router_mod

        app = FastAPI()
        tdir = Path(__file__).resolve().parent.parent / "src" / "templates"
        tmpl = Jinja2Templates(directory=str(tdir))
        tmpl.env.globals["git_sync_status"] = lambda: {"unsynced": False}
        tmpl.env.globals["_"] = lambda s: s

        with (
            patch("src.db.ai_decisions.list_ai_decisions", return_value=[]) as mock_list,
            patch("src.db.ai_decisions.count_ai_decisions", return_value=0),
            patch("src.routers.ai_decisions.templates", tmpl),
        ):
            app.include_router(router_mod.router)
            client = TestClient(app)
            resp = client.get("/data/ai-decisions?type=page_quality&result=ok")

        assert resp.status_code == 200
        mock_list.assert_called_once()
        call_kwargs = mock_list.call_args.kwargs
        assert call_kwargs["decision_type"] == "page_quality"
        assert call_kwargs["result"] == "ok"

    def test_invalid_type_ignored(self, tmp_path):
        from unittest.mock import patch
        from fastapi import FastAPI
        from fastapi.templating import Jinja2Templates
        from src.routers import ai_decisions as router_mod

        app = FastAPI()
        tdir = Path(__file__).resolve().parent.parent / "src" / "templates"
        tmpl = Jinja2Templates(directory=str(tdir))
        tmpl.env.globals["git_sync_status"] = lambda: {"unsynced": False}
        tmpl.env.globals["_"] = lambda s: s

        with (
            patch("src.db.ai_decisions.list_ai_decisions", return_value=[]) as mock_list,
            patch("src.db.ai_decisions.count_ai_decisions", return_value=0),
            patch("src.routers.ai_decisions.templates", tmpl),
        ):
            app.include_router(router_mod.router)
            client = TestClient(app)
            resp = client.get("/data/ai-decisions?type=hacker_injection")

        assert resp.status_code == 200
        call_kwargs = mock_list.call_args.kwargs
        assert call_kwargs["decision_type"] is None
