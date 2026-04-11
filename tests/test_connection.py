# -*- coding: utf-8 -*-
"""Unit tests for src/db/connection.py.

Tests cover:
- Path helpers: get_db_path, get_log_dir, get_cache_dir
- _SQLiteConnWrapper._adapt: SQL translation (placeholder, NOW(), type casts)
- _PrefetchedCursor: fetchone / fetchall behaviour
- _PGSavepointContext: SQLite no-op path; success and rollback paths (mocked)

No live PostgreSQL connection is needed — all tests use SQLite or mocks.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from src.db.connection import (
    _SQLiteConnWrapper,
    _PrefetchedCursor,
    _PGSavepointContext,
    get_db_path,
    get_log_dir,
    get_cache_dir,
    is_postgres,
)

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


class TestPathHelpers:
    def test_get_db_path_default(self, monkeypatch):
        monkeypatch.delenv("OFFICE_HOLDER_DB_PATH", raising=False)
        p = get_db_path()
        assert isinstance(p, Path)
        assert p.name == "office_holder.db"

    def test_get_db_path_override(self, monkeypatch, tmp_path):
        monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(tmp_path / "custom.db"))
        p = get_db_path()
        assert p.name == "custom.db"

    def test_get_log_dir_default(self, monkeypatch):
        monkeypatch.delenv("LOG_DIR", raising=False)
        p = get_log_dir()
        assert p.name == "logs"

    def test_get_log_dir_override(self, monkeypatch, tmp_path):
        monkeypatch.setenv("LOG_DIR", str(tmp_path / "mylogs"))
        p = get_log_dir()
        assert p.name == "mylogs"

    def test_get_cache_dir_default(self, monkeypatch):
        monkeypatch.delenv("WIKI_CACHE_DIR", raising=False)
        p = get_cache_dir()
        assert p.name == "wiki_cache"

    def test_get_cache_dir_override(self, monkeypatch, tmp_path):
        monkeypatch.setenv("WIKI_CACHE_DIR", str(tmp_path / "mycache"))
        p = get_cache_dir()
        assert p.name == "mycache"

    def test_is_postgres_false_without_env(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        assert is_postgres() is False

    def test_is_postgres_true_with_env(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@host/db")
        assert is_postgres() is True


# ---------------------------------------------------------------------------
# _SQLiteConnWrapper._adapt
# ---------------------------------------------------------------------------


class TestSQLiteAdapt:
    def test_replaces_percent_s_with_question_mark(self):
        sql = "SELECT * FROM t WHERE id = %s AND name = %s"
        adapted = _SQLiteConnWrapper._adapt(sql)
        assert adapted == "SELECT * FROM t WHERE id = ? AND name = ?"

    def test_replaces_now(self):
        sql = "INSERT INTO t (created_at) VALUES (NOW())"
        adapted = _SQLiteConnWrapper._adapt(sql)
        assert "CURRENT_TIMESTAMP" in adapted
        assert "NOW()" not in adapted

    def test_strips_text_cast(self):
        sql = "SELECT id::TEXT FROM t"
        adapted = _SQLiteConnWrapper._adapt(sql)
        assert "::TEXT" not in adapted

    def test_strips_integer_cast(self):
        sql = "SELECT val::integer FROM t"
        adapted = _SQLiteConnWrapper._adapt(sql)
        assert "::integer" not in adapted

    def test_strips_date_cast(self):
        sql = "SELECT d::date FROM t"
        adapted = _SQLiteConnWrapper._adapt(sql)
        assert "::date" not in adapted

    def test_replaces_double_percent(self):
        sql = "SELECT id %% 7 FROM t"
        adapted = _SQLiteConnWrapper._adapt(sql)
        assert adapted == "SELECT id % 7 FROM t"

    def test_no_change_when_no_postgres_syntax(self):
        sql = "SELECT * FROM t WHERE id = ?"
        assert _SQLiteConnWrapper._adapt(sql) == sql


# ---------------------------------------------------------------------------
# _SQLiteConnWrapper.execute
# ---------------------------------------------------------------------------


class TestSQLiteConnWrapperExecute:
    def _make_wrapper(self, tmp_path):
        raw = sqlite3.connect(str(tmp_path / "test.db"))
        raw.row_factory = sqlite3.Row
        return _SQLiteConnWrapper(raw)

    def test_execute_simple_select(self, tmp_path):
        conn = self._make_wrapper(tmp_path)
        conn.executescript("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t (val) VALUES (?)", ("hello",))
        conn.commit()
        row = conn.execute("SELECT val FROM t WHERE id = ?", (1,)).fetchone()
        assert row["val"] == "hello"

    def test_execute_translates_percent_s(self, tmp_path):
        conn = self._make_wrapper(tmp_path)
        conn.executescript("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t (val) VALUES (%s)", ("world",))
        conn.commit()
        row = conn.execute("SELECT val FROM t").fetchone()
        assert row["val"] == "world"

    def test_execute_returning_prefetches(self, tmp_path):
        conn = self._make_wrapper(tmp_path)
        conn.executescript("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        cur = conn.execute("INSERT INTO t (val) VALUES (%s) RETURNING id", ("x",))
        conn.commit()
        row = cur.fetchone()
        # Row has 'id' key (via pre-fetched _PrefetchedCursor)
        assert row[0] is not None or row["id"] is not None


# ---------------------------------------------------------------------------
# _PrefetchedCursor
# ---------------------------------------------------------------------------


class TestPrefetchedCursor:
    def _make(self, rows):
        return _PrefetchedCursor(rows, len(rows), None)

    def test_fetchone_returns_first_row(self):
        cur = self._make([(1, "a"), (2, "b")])
        assert cur.fetchone() == (1, "a")

    def test_fetchone_advances_cursor(self):
        cur = self._make([(1,), (2,)])
        cur.fetchone()
        assert cur.fetchone() == (2,)

    def test_fetchone_returns_none_when_exhausted(self):
        cur = self._make([(1,)])
        cur.fetchone()
        assert cur.fetchone() is None

    def test_fetchall_returns_remaining(self):
        cur = self._make([(1,), (2,), (3,)])
        cur.fetchone()  # advance past first
        result = cur.fetchall()
        assert result == [(2,), (3,)]

    def test_fetchall_empty_after_full_consume(self):
        cur = self._make([(1,)])
        cur.fetchall()
        assert cur.fetchall() == []

    def test_rowcount_set(self):
        cur = self._make([(1,), (2,)])
        assert cur.rowcount == 2


# ---------------------------------------------------------------------------
# _PGSavepointContext — SQLite path (no-op)
# ---------------------------------------------------------------------------


class TestPGSavepointContext:
    def test_sqlite_noop_success(self, monkeypatch, tmp_path):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        raw = sqlite3.connect(str(tmp_path / "t.db"))
        conn = _SQLiteConnWrapper(raw)
        # Should execute without calling SAVEPOINT on SQLite
        with _PGSavepointContext(conn, "sp1"):
            pass  # no exception

    def test_sqlite_noop_exception_not_suppressed(self, monkeypatch, tmp_path):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        raw = sqlite3.connect(str(tmp_path / "t.db"))
        conn = _SQLiteConnWrapper(raw)
        with pytest.raises(ValueError, match="test error"):
            with _PGSavepointContext(conn, "sp1"):
                raise ValueError("test error")

    def test_postgres_path_releases_on_success(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://fake/db")
        mock_conn = MagicMock()
        ctx = _PGSavepointContext(mock_conn, "my_sp")
        ctx.__enter__()
        mock_conn.execute.assert_called_with("SAVEPOINT my_sp")
        ctx.__exit__(None, None, None)
        mock_conn.execute.assert_called_with("RELEASE SAVEPOINT my_sp")

    def test_postgres_path_rollbacks_on_error(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://fake/db")
        mock_conn = MagicMock()
        ctx = _PGSavepointContext(mock_conn, "my_sp")
        ctx.__enter__()
        ctx.__exit__(ValueError, ValueError("oops"), None)
        calls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("ROLLBACK" in c for c in calls)
        assert any("RELEASE" in c for c in calls)

    def test_returns_false_never_suppresses(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        raw = sqlite3.connect(":memory:")
        conn = _SQLiteConnWrapper(raw)
        ctx = _PGSavepointContext(conn, "sp")
        ctx.__enter__()
        result = ctx.__exit__(None, None, None)
        assert result is False
