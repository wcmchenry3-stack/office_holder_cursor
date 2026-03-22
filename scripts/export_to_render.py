#!/usr/bin/env python3
"""
Export office configuration data from a source SQLite DB to a SQL file
suitable for importing into Render's production database.

Skips individuals and office_terms (populated by scraper runs).
Handles known column differences between the old and new schemas.

Usage:
    python scripts/export_to_render.py [source_db_path]

    source_db_path defaults to:
        C:\\Users\\wcmch\\cursor\\office_holder\\data\\office_holder.db

Output:
    migration_output.sql  (at project root — gitignored)

Apply to Render via the Render shell:
    sqlite3 /data/office_holder.db < migration_output.sql
"""

import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_SOURCE = r"C:\Users\wcmch\cursor\office_holder\data\office_holder.db"
OUTPUT = Path(__file__).resolve().parent.parent / "migration_output.sql"

# ---------------------------------------------------------------------------
# Tables to migrate, in FK-safe order.
# Each entry: (table_name, [source_cols], insert_mode)
# source_cols are selected from the source DB and inserted into the same
# column names in the target DB.  Column mapping notes:
#   - offices: skip alt_link, superseded_by_office_details_id (not in target)
#   - source_pages: skip table_reuse_across_offices (old name); allow_reuse_tables is kept
#   - office_details: skip last_scraped_at (not in target)
#   - office_table_config: skip infobox_role_key (not in target)
# ---------------------------------------------------------------------------

TABLES = [
    # Reference tables — already seeded in target, INSERT OR IGNORE is safe
    ("countries", ["id", "name"], "INSERT OR IGNORE"),
    ("states", ["id", "country_id", "name"], "INSERT OR IGNORE"),
    ("cities", ["id", "state_id", "name"], "INSERT OR IGNORE"),
    ("levels", ["id", "name"], "INSERT OR IGNORE"),
    ("branches", ["id", "name"], "INSERT OR IGNORE"),
    # Config tables — INSERT OR REPLACE so the script is safe to re-run and
    # handles any rows that were auto-seeded in the target DB at startup.
    (
        "parties",
        [
            "id",
            "country_id",
            "party_name",
            "party_link",
            "created_at",
        ],
        "INSERT OR REPLACE",
    ),
    ("office_category", ["id", "name"], "INSERT OR REPLACE"),
    ("office_category_countries", ["category_id", "country_id"], "INSERT OR REPLACE"),
    ("office_category_levels", ["category_id", "level_id"], "INSERT OR REPLACE"),
    ("office_category_branches", ["category_id", "branch_id"], "INSERT OR REPLACE"),
    ("infobox_role_key_filter", ["id", "name", "role_key"], "INSERT OR REPLACE"),
    ("infobox_role_key_filter_countries", ["filter_id", "country_id"], "INSERT OR REPLACE"),
    ("infobox_role_key_filter_levels", ["filter_id", "level_id"], "INSERT OR REPLACE"),
    ("infobox_role_key_filter_branches", ["filter_id", "branch_id"], "INSERT OR REPLACE"),
    # offices: skip alt_link (superseded by alt_links table) and
    #          superseded_by_office_details_id (internal migration column)
    (
        "offices",
        [
            "id",
            "country_id",
            "state_id",
            "level_id",
            "branch_id",
            "department",
            "name",
            "enabled",
            "notes",
            "url",
            "table_no",
            "table_rows",
            "link_column",
            "party_column",
            "term_start_column",
            "term_end_column",
            "district_column",
            "filter_column",
            "filter_criteria",
            "dynamic_parse",
            "read_right_to_left",
            "find_date_in_infobox",
            "parse_rowspan",
            "consolidate_rowspan_terms",
            "rep_link",
            "party_link",
            "alt_link_include_main",
            "use_full_page_for_table",
            "years_only",
            "term_dates_merged",
            "party_ignore",
            "district_ignore",
            "district_at_large",
            "ignore_non_links",
            "remove_duplicates",
            "infobox_role_key",
            "created_at",
            # infobox_role_key_filter_id not in source DB (migration ran later) — omitted, defaults to NULL
        ],
        "INSERT OR REPLACE",
    ),
    ("alt_links", ["id", "office_id", "office_details_id", "link_path"], "INSERT OR REPLACE"),
    # source_pages: source has both table_reuse_across_offices (old) and
    # allow_reuse_tables (new) — we select allow_reuse_tables only
    (
        "source_pages",
        [
            "id",
            "country_id",
            "state_id",
            "city_id",
            "level_id",
            "branch_id",
            "url",
            "notes",
            "enabled",
            "allow_reuse_tables",
            "disable_auto_table_update",
            "last_scraped_at",
            "created_at",
            "updated_at",
        ],
        "INSERT OR REPLACE",
    ),
    # office_details: skip last_scraped_at (not in target schema)
    (
        "office_details",
        [
            "id",
            "source_page_id",
            "name",
            "variant_name",
            "department",
            "notes",
            "alt_link_include_main",
            "enabled",
            "created_at",
            "updated_at",
            "office_category_id",
        ],
        "INSERT OR REPLACE",
    ),
    # office_table_config: skip infobox_role_key (not in target schema)
    (
        "office_table_config",
        [
            "id",
            "office_details_id",
            "table_no",
            "table_rows",
            "link_column",
            "party_column",
            "term_start_column",
            "term_end_column",
            "district_column",
            "filter_column",
            "filter_criteria",
            "dynamic_parse",
            "read_right_to_left",
            "find_date_in_infobox",
            "parse_rowspan",
            "rep_link",
            "party_link",
            "enabled",
            "use_full_page_for_table",
            "years_only",
            "term_dates_merged",
            "party_ignore",
            "district_ignore",
            "district_at_large",
            "ignore_non_links",
            "remove_duplicates",
            "consolidate_rowspan_terms",
            "infobox_role_key_filter_id",
            "notes",
            "name",
            "created_at",
            "updated_at",
        ],
        "INSERT OR REPLACE",
    ),
    # parser_test_scripts: INSERT OR REPLACE to overwrite any auto-seeded rows
    (
        "parser_test_scripts",
        [
            "id",
            "name",
            "test_type",
            "enabled",
            "html_file",
            "source_url",
            "config_json",
            "expected_json",
            "created_at",
            "updated_at",
        ],
        "INSERT OR REPLACE",
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def sql_literal(value) -> str:
    """Convert a Python value to a safe SQL literal string."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def build_inserts(conn: sqlite3.Connection, table: str, cols: list[str], mode: str) -> list[str]:
    col_csv = ", ".join(cols)
    rows = conn.execute(f"SELECT {col_csv} FROM {table}").fetchall()
    stmts = []
    for row in rows:
        values = ", ".join(sql_literal(v) for v in row)
        stmts.append(f"{mode} INTO {table} ({col_csv}) VALUES ({values});")
    return stmts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    source_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SOURCE

    if not Path(source_path).exists():
        print(f"ERROR: Source DB not found: {source_path}")
        sys.exit(1)

    print(f"Source: {source_path}")
    print(f"Output: {OUTPUT}")

    src = sqlite3.connect(source_path)
    src.row_factory = sqlite3.Row

    lines = [
        "-- Office Holder config data migration",
        "-- Generated by scripts/export_to_render.py",
        "-- Apply with: sqlite3 /data/office_holder.db < migration_output.sql",
        "",
        "BEGIN TRANSACTION;",
        "",
    ]

    total_rows = 0
    for table, cols, mode in TABLES:
        stmts = build_inserts(src, table, cols, mode)
        lines.append(f"-- {table} ({len(stmts)} rows)")
        lines.extend(stmts)
        lines.append("")
        print(f"  {table}: {len(stmts)} rows")
        total_rows += len(stmts)

    lines.append("COMMIT;")
    lines.append("")

    src.close()

    OUTPUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nDone. {total_rows} total rows written to {OUTPUT.name}")
    print("\nNext steps:")
    print("  1. Upload migration_output.sql to Render shell")
    print("  2. Run: sqlite3 /data/office_holder.db < migration_output.sql")
    print("  3. Verify row counts in Datasette at /db/")


if __name__ == "__main__":
    main()
