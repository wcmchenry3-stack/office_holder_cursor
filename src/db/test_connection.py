"""Unit tests for the dual-backend connection layer.

Tests _SQLiteConnWrapper._adapt(), _PrefetchedCursor, RETURNING detection,
and get_connection() routing logic. All tests use in-memory SQLite — no
PostgreSQL connection required.

Run: pytest src/db/test_connection.py -v
"""

from __future__ import annotations

import os
import sqlite3

import pytest

from src.db.connection import (
    _PrefetchedCursor,
    _SQLiteConnWrapper,
    get_connection,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _memory_conn() -> _SQLiteConnWrapper:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return _SQLiteConnWrapper(conn)


# ---------------------------------------------------------------------------
# _SQLiteConnWrapper._adapt() — SQL translation
# ---------------------------------------------------------------------------


def test_adapt_replaces_percent_s_placeholder():
    assert _SQLiteConnWrapper._adapt("SELECT * FROM t WHERE id = %s") == (
        "SELECT * FROM t WHERE id = ?"
    )


def test_adapt_replaces_multiple_percent_s():
    result = _SQLiteConnWrapper._adapt("INSERT INTO t (a, b) VALUES (%s, %s)")
    assert result == "INSERT INTO t (a, b) VALUES (?, ?)"


def test_adapt_replaces_double_percent_with_single():
    """%%  (psycopg2 escaped literal %) → % (SQLite modulo operator)."""
    result = _SQLiteConnWrapper._adapt("UPDATE t SET batch = id %% 7")
    assert result == "UPDATE t SET batch = id % 7"


def test_adapt_replaces_now_with_current_timestamp():
    result = _SQLiteConnWrapper._adapt("INSERT INTO t (ts) VALUES (NOW())")
    assert result == "INSERT INTO t (ts) VALUES (CURRENT_TIMESTAMP)"


def test_adapt_strips_double_colon_text_cast():
    assert _SQLiteConnWrapper._adapt("SELECT col::TEXT FROM t") == "SELECT col FROM t"
    assert _SQLiteConnWrapper._adapt("SELECT col::text FROM t") == "SELECT col FROM t"


def test_adapt_strips_double_colon_integer_cast():
    assert _SQLiteConnWrapper._adapt("SELECT val::integer FROM t") == "SELECT val FROM t"
    assert _SQLiteConnWrapper._adapt("SELECT val::INTEGER FROM t") == "SELECT val FROM t"


def test_adapt_strips_double_colon_date_cast():
    assert _SQLiteConnWrapper._adapt("SELECT d::date FROM t") == "SELECT d FROM t"


def test_adapt_combined_translations():
    sql = "UPDATE t SET ts = NOW(), batch = id %% 7 WHERE id = %s"
    result = _SQLiteConnWrapper._adapt(sql)
    assert result == "UPDATE t SET ts = CURRENT_TIMESTAMP, batch = id % 7 WHERE id = ?"


def test_adapt_passthrough_when_no_substitutions():
    sql = "SELECT id, name FROM countries ORDER BY name"
    assert _SQLiteConnWrapper._adapt(sql) == sql


# ---------------------------------------------------------------------------
# _PrefetchedCursor
# ---------------------------------------------------------------------------


def _make_prefetched(rows: list) -> _PrefetchedCursor:
    return _PrefetchedCursor(rows, rowcount=len(rows), description=None)


def test_prefetched_cursor_fetchone_returns_rows_in_order():
    cur = _make_prefetched([{"id": 1}, {"id": 2}])
    assert cur.fetchone()["id"] == 1
    assert cur.fetchone()["id"] == 2


def test_prefetched_cursor_fetchone_returns_none_after_exhaustion():
    cur = _make_prefetched([{"id": 1}])
    cur.fetchone()
    assert cur.fetchone() is None


def test_prefetched_cursor_fetchall_returns_all_rows():
    cur = _make_prefetched([{"id": 1}, {"id": 2}, {"id": 3}])
    rows = cur.fetchall()
    assert len(rows) == 3
    assert rows[0]["id"] == 1


def test_prefetched_cursor_fetchall_after_fetchone_returns_remaining():
    cur = _make_prefetched([{"id": 1}, {"id": 2}, {"id": 3}])
    cur.fetchone()  # consume first
    remaining = cur.fetchall()
    assert len(remaining) == 2
    assert remaining[0]["id"] == 2


def test_prefetched_cursor_rowcount_preserved():
    cur = _make_prefetched([{"id": 1}, {"id": 2}])
    assert cur.rowcount == 2


def test_prefetched_cursor_description_preserved():
    cur = _PrefetchedCursor([], rowcount=0, description="mock-description")
    assert cur.description == "mock-description"


# ---------------------------------------------------------------------------
# _SQLiteConnWrapper.execute() — RETURNING detection
# ---------------------------------------------------------------------------


def test_execute_with_returning_returns_prefetched_cursor():
    conn = _memory_conn()
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    result = conn.execute("INSERT INTO items (name) VALUES (%s) RETURNING id", ("test",))
    assert isinstance(result, _PrefetchedCursor)


def test_execute_without_returning_returns_raw_cursor():
    conn = _memory_conn()
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    result = conn.execute("INSERT INTO items (name) VALUES (%s)", ("test",))
    # Should NOT be a _PrefetchedCursor
    assert not isinstance(result, _PrefetchedCursor)


def test_execute_returning_detection_is_case_insensitive():
    conn = _memory_conn()
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    # lowercase 'returning'
    result = conn.execute("INSERT INTO items (name) VALUES (%s) returning id", ("test",))
    assert isinstance(result, _PrefetchedCursor)


def test_execute_returning_fetchone_works_after_commit():
    """Core guarantee: RETURNING result survives commit() because it's pre-fetched."""
    conn = _memory_conn()
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    cur = conn.execute("INSERT INTO items (name) VALUES (%s) RETURNING id", ("alpha",))
    conn.commit()  # must not lose the pre-fetched row
    row = cur.fetchone()
    assert row is not None
    assert row["id"] == 1


# ---------------------------------------------------------------------------
# get_connection() routing
# ---------------------------------------------------------------------------


def test_get_connection_returns_sqlite_wrapper_without_database_url(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(tmp_path / "test.db"))
    conn = get_connection()
    try:
        assert isinstance(conn, _SQLiteConnWrapper)
    finally:
        conn.close()


def test_get_connection_with_explicit_path_returns_sqlite_wrapper_even_if_database_url_set(
    tmp_path, monkeypatch
):
    """Explicit path arg always returns SQLite, even when DATABASE_URL is set."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://fake/db")
    db_path = tmp_path / "explicit.db"
    conn = get_connection(path=db_path)
    try:
        assert isinstance(conn, _SQLiteConnWrapper)
    finally:
        conn.close()
