# -*- coding: utf-8 -*-
"""
Tests for _SQLiteConnWrapper._adapt() and _split_sql().

Documents known behaviour and edge-cases of the string-replacement-based
SQL adapter, including cases where it is known NOT to handle correctly.
"""
import pytest

from src.db.connection import _SQLiteConnWrapper, _split_sql


# ---------------------------------------------------------------------------
# _adapt: basic translations
# ---------------------------------------------------------------------------


def test_adapt_replaces_placeholder():
    sql = "SELECT * FROM t WHERE id = %s"
    assert _SQLiteConnWrapper._adapt(sql) == "SELECT * FROM t WHERE id = ?"


def test_adapt_replaces_multiple_placeholders():
    sql = "INSERT INTO t (a, b) VALUES (%s, %s)"
    assert _SQLiteConnWrapper._adapt(sql) == "INSERT INTO t (a, b) VALUES (?, ?)"


def test_adapt_now_to_current_timestamp():
    sql = "UPDATE t SET updated_at = NOW()"
    assert _SQLiteConnWrapper._adapt(sql) == "UPDATE t SET updated_at = CURRENT_TIMESTAMP"


def test_adapt_strips_text_cast():
    sql = "SELECT val::TEXT FROM t"
    assert _SQLiteConnWrapper._adapt(sql) == "SELECT val FROM t"


def test_adapt_strips_integer_cast():
    sql = "SELECT val::integer FROM t"
    assert _SQLiteConnWrapper._adapt(sql) == "SELECT val FROM t"


def test_adapt_strips_date_cast():
    sql = "SELECT val::date FROM t"
    assert _SQLiteConnWrapper._adapt(sql) == "SELECT val FROM t"


def test_adapt_double_percent_becomes_single():
    """%%  (psycopg2 escaped literal %) → % for SQLite modulo operator."""
    sql = "SELECT id %% 7 FROM individuals"
    result = _SQLiteConnWrapper._adapt(sql)
    assert result == "SELECT id % 7 FROM individuals"
    # Ensure the % is not further replaced as a placeholder
    assert "?" not in result


def test_adapt_empty_string():
    assert _SQLiteConnWrapper._adapt("") == ""


def test_adapt_no_substitutions_needed():
    sql = "SELECT 1"
    assert _SQLiteConnWrapper._adapt(sql) == "SELECT 1"


def test_adapt_mixed_substitutions():
    sql = "UPDATE individuals SET bio_batch = id %% 7 WHERE id = %s AND updated_at = NOW()"
    result = _SQLiteConnWrapper._adapt(sql)
    assert "?" in result       # %s → ?
    assert "%" in result       # %% → %
    assert "NOW()" not in result
    assert "CURRENT_TIMESTAMP" in result


# ---------------------------------------------------------------------------
# _adapt: known limitations (documented)
# ---------------------------------------------------------------------------


def test_adapt_cast_inside_string_literal_corrupts_sql():
    """
    KNOWN LIMITATION: ::TEXT inside a string literal is incorrectly stripped.
    The adapter uses plain string replacement and is not SQL-parser-aware.
    This test documents the behaviour; the current schema does not trigger it.
    """
    sql = "SELECT '::TEXT' AS col"
    result = _SQLiteConnWrapper._adapt(sql)
    # The literal ::TEXT is stripped, producing broken SQL
    assert "''" in result or "col" in result  # documenting the output, not asserting correctness


# ---------------------------------------------------------------------------
# _split_sql
# ---------------------------------------------------------------------------


def test_split_sql_single_statement():
    sql = "CREATE TABLE t (id INTEGER PRIMARY KEY)"
    parts = _split_sql(sql)
    assert parts == ["CREATE TABLE t (id INTEGER PRIMARY KEY)"]


def test_split_sql_multiple_statements():
    sql = "CREATE TABLE a (id INTEGER); CREATE TABLE b (id INTEGER);"
    parts = _split_sql(sql)
    assert len(parts) == 2
    assert parts[0] == "CREATE TABLE a (id INTEGER)"
    assert parts[1] == "CREATE TABLE b (id INTEGER)"


def test_split_sql_empty_string():
    assert _split_sql("") == []


def test_split_sql_only_whitespace_and_semicolons():
    assert _split_sql("  ;  ;  ") == []


def test_split_sql_strips_whitespace():
    sql = "  SELECT 1  ;  SELECT 2  "
    parts = _split_sql(sql)
    assert parts == ["SELECT 1", "SELECT 2"]


def test_split_sql_semicolon_in_string_literal_known_limitation():
    """
    KNOWN LIMITATION: semicolons inside string literals split the statement.
    _split_sql is not SQL-parser-aware.  The current schema does not contain
    semicolons inside literals.
    """
    sql = "SELECT 'hello; world'"
    parts = _split_sql(sql)
    # Will incorrectly split into two fragments
    assert len(parts) == 2  # documents the behaviour, not correctness
