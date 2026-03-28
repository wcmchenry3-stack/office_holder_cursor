"""Schema sync test: ensures SCHEMA_SQL (SQLite/tests) stays in sync with SCHEMA_PG_SQL (PostgreSQL/production).

Run: pytest src/db/test_schema_sync.py -v

Fails CI if a column or table is present in SCHEMA_PG_SQL but missing from SCHEMA_SQL.
When adding a column/table, update BOTH schema constants and add a _run_pg_migrations()
entry in connection.py for the live database.
"""

from __future__ import annotations

import re
import sqlite3

import pytest

from src.db.schema import SCHEMA_PG_SQL
from src.db.connection import init_db

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_pg_tables(pg_sql: str) -> dict[str, list[str]]:
    """Extract {table_name: [column_names]} from SCHEMA_PG_SQL using regex.

    Only captures column definitions (lines that start with a bare identifier
    followed by a type keyword). Constraint lines (PRIMARY KEY, UNIQUE, etc.)
    and index-only statements are skipped.
    """
    tables: dict[str, list[str]] = {}
    # Match each CREATE TABLE block
    for block in re.finditer(
        r"CREATE TABLE IF NOT EXISTS (\w+)\s*\(([^;]+?)\);",
        pg_sql,
        re.IGNORECASE | re.DOTALL,
    ):
        table_name = block.group(1).lower()
        body = block.group(2)
        columns: list[str] = []
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if not line:
                continue
            # Skip constraint lines
            if re.match(
                r"(PRIMARY KEY|UNIQUE|CHECK|FOREIGN KEY|CONSTRAINT)\b", line, re.IGNORECASE
            ):
                continue
            # First token is the column name
            col_match = re.match(r"(\w+)\s+\w+", line)
            if col_match:
                col = col_match.group(1).lower()
                columns.append(col)
        tables[table_name] = columns
    return tables


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1].lower() for row in cur.fetchall()}


def _sqlite_tables(conn: sqlite3.Connection) -> set[str]:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0].lower() for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sqlite_db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("schema_sync") / "sync_test.db"
    init_db(path=db_path)
    conn = sqlite3.connect(str(db_path))
    yield conn
    conn.close()


def test_all_pg_tables_exist_in_sqlite(sqlite_db):
    pg_tables = _parse_pg_tables(SCHEMA_PG_SQL)
    sqlite_tbls = _sqlite_tables(sqlite_db)
    missing = sorted(t for t in pg_tables if t not in sqlite_tbls)
    assert not missing, (
        f"Tables in SCHEMA_PG_SQL missing from SCHEMA_SQL: {missing}\n"
        "Update SCHEMA_SQL in src/db/schema.py to add the missing table(s)."
    )


@pytest.mark.parametrize("table,pg_cols", sorted(_parse_pg_tables(SCHEMA_PG_SQL).items()))
def test_pg_columns_present_in_sqlite(sqlite_db, table, pg_cols):
    sqlite_tbls = _sqlite_tables(sqlite_db)
    if table not in sqlite_tbls:
        pytest.skip(
            f"Table '{table}' not in SQLite DB (caught by test_all_pg_tables_exist_in_sqlite)"
        )

    sqlite_cols = _sqlite_columns(sqlite_db, table)
    missing = sorted(c for c in pg_cols if c not in sqlite_cols)
    assert not missing, (
        f"Columns in SCHEMA_PG_SQL[{table}] missing from SCHEMA_SQL: {missing}\n"
        "Update the corresponding CREATE TABLE in SCHEMA_SQL in src/db/schema.py."
    )
