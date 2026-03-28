"""One-time migration script: copy all data from a local SQLite file to Render PostgreSQL.

Usage:
    # 1. Download the production SQLite DB from the Render disk (via Render shell or scp).
    # 2. Set DATABASE_URL to the Render external connection string.
    # 3. Run:
    DATABASE_URL="postgresql://..." python scripts/migrate_sqlite_to_postgres.py /path/to/office_holder.db

Options:
    --dry-run    Print row counts per table without inserting anything.
    --table NAME Migrate only the named table (can repeat). Default: all tables.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Table migration order — respects FK dependencies
# ---------------------------------------------------------------------------
TABLE_ORDER = [
    "countries",
    "levels",
    "branches",
    "states",
    "cities",
    "parties",
    "office_category",
    "office_category_countries",
    "office_category_levels",
    "office_category_branches",
    "infobox_role_key_filter",
    "infobox_role_key_filter_countries",
    "infobox_role_key_filter_levels",
    "infobox_role_key_filter_branches",
    "source_pages",
    "offices",
    "alt_links",
    "office_details",
    "office_table_config",
    "parser_test_scripts",
    # individuals and office_terms are intentionally excluded — re-scraped fresh.
]

# Timestamp columns that SQLite stores as "YYYY-MM-DD HH:MM:SS" strings.
# psycopg2 needs Python datetime objects for TIMESTAMPTZ columns.
TIMESTAMP_COLUMNS: dict[str, set[str]] = {
    "individuals": {"created_at", "updated_at", "bio_refreshed_at"},
    "offices": {"created_at", "updated_at"},
    "office_details": {"created_at", "updated_at", "scraped_at"},
    "office_table_config": {"created_at", "updated_at"},
    "office_terms": {"created_at", "updated_at"},
    "source_pages": {"created_at", "updated_at"},
    "alt_links": {"created_at", "updated_at"},
    "parties": {"created_at", "updated_at"},
    "parser_test_scripts": {"created_at", "updated_at"},
    "countries": {"created_at", "updated_at"},
    "states": {"created_at", "updated_at"},
    "cities": {"created_at", "updated_at"},
    "levels": {"created_at", "updated_at"},
    "branches": {"created_at", "updated_at"},
    "office_category": {"created_at", "updated_at"},
    "infobox_role_key_filter": {"created_at", "updated_at"},
}

BATCH_SIZE = 500


def _parse_ts(val: str | None) -> datetime | None:
    """Parse a SQLite timestamp string to a Python datetime, or return None."""
    if not val:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None  # unparseable — let psycopg2 try as-is


def _get_sqlite_columns(sqlite_conn: sqlite3.Connection, table: str) -> list[str]:
    cur = sqlite_conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _migrate_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    table: str,
    dry_run: bool,
) -> int:
    """Migrate one table. Returns number of rows processed."""
    columns = _get_sqlite_columns(sqlite_conn, table)
    if not columns:
        print(f"  [{table}] SKIP — table not found in SQLite source")
        return 0

    ts_cols = TIMESTAMP_COLUMNS.get(table, set())

    cur = sqlite_conn.execute(f"SELECT {', '.join(columns)} FROM {table}")
    rows = cur.fetchall()
    total = len(rows)

    if dry_run:
        print(f"  [{table}] {total} rows (dry-run, skipping insert)")
        return total

    if total == 0:
        print(f"  [{table}] 0 rows — nothing to do")
        return 0

    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})" " ON CONFLICT DO NOTHING"

    col_indices = {col: i for i, col in enumerate(columns)}

    def _coerce_row(raw_row) -> tuple:
        coerced = list(raw_row)
        for col in ts_cols:
            if col in col_indices:
                idx = col_indices[col]
                coerced[idx] = _parse_ts(coerced[idx])
        return tuple(coerced)

    inserted = 0
    pg_cur = pg_conn.cursor()
    for batch_start in range(0, total, BATCH_SIZE):
        batch = [_coerce_row(r) for r in rows[batch_start : batch_start + BATCH_SIZE]]
        pg_cur.executemany(sql, batch)
        inserted += len(batch)

    pg_conn.commit()
    print(f"  [{table}] {inserted}/{total} rows migrated")

    # Reset the PostgreSQL sequence so that new INSERTs don't collide with migrated IDs.
    if "id" in columns:
        pg_cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE(MAX(id), 1)) FROM {table}"
        )
        pg_conn.commit()

    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate SQLite office_holder.db to Render PostgreSQL."
    )
    parser.add_argument("sqlite_path", help="Path to the source SQLite .db file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row counts without inserting anything",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        metavar="NAME",
        help="Migrate only this table (can repeat). Default: all tables.",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        print(f"ERROR: SQLite file not found: {sqlite_path}", file=sys.stderr)
        sys.exit(1)

    import os

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    tables_to_migrate = args.tables if args.tables else TABLE_ORDER
    # Keep dependency order even when --table filters are applied
    tables_to_migrate = [t for t in TABLE_ORDER if t in tables_to_migrate]

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(database_url, cursor_factory=psycopg2.extras.DictCursor)

    print(f"Source: {sqlite_path}")
    print(f"Target: {database_url[:40]}...")
    if args.dry_run:
        print("DRY RUN — no data will be written\n")
    else:
        print()

    total_rows = 0
    try:
        for table in tables_to_migrate:
            total_rows += _migrate_table(sqlite_conn, pg_conn, table, dry_run=args.dry_run)
    finally:
        sqlite_conn.close()
        pg_conn.close()

    print(f"\nDone. {total_rows} total rows processed across {len(tables_to_migrate)} tables.")


if __name__ == "__main__":
    main()
