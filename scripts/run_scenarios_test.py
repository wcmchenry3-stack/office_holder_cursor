#!/usr/bin/env python3
"""
Test script for run scenarios (delta, populate-terms, preview).
Uses a test database and pre-filled table cache so no network or production DB is touched.
Exit 0 on success, non-zero on failure.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import sys
from pathlib import Path

# Set test DB path and Python path before any import that uses get_connection/init_db
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DB_PATH = PROJECT_ROOT / "data" / "test_run.db"
os.environ["OFFICE_HOLDER_DB_PATH"] = str(TEST_DB_PATH)

# Safety: ensure we never point at production DB
from src.db.connection import DB_PATH

if os.environ.get("OFFICE_HOLDER_DB_PATH") == str(DB_PATH):
    print("ERROR: OFFICE_HOLDER_DB_PATH must not point at production DB.", file=sys.stderr)
    sys.exit(2)

# Now safe to import app modules
from src.db.connection import init_db, get_connection
from src.db import offices as db_offices
from src.db import office_terms as db_office_terms
from src.scraper.runner import run_with_db


def _cache_key(url: str, table_no: int, use_full_page: bool = False) -> str:
    """Match table_cache._cache_key so we write the same key the runner will read."""
    normalized = (url.strip() + "|" + str(table_no) + "|" + ("1" if use_full_page else "0")).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()[:32]


def _extract_table_no_and_count(html: str, table_no: int) -> tuple[str, int]:
    """Extract the N-th <table> from full page HTML (1-based). Returns (table_html, num_tables)."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    num_tables = len(tables)
    if not (1 <= table_no <= num_tables):
        raise ValueError(f"Table {table_no} not found (page has {num_tables} tables)")
    return str(tables[table_no - 1]), num_tables


def _write_fixture_to_cache(
    url: str,
    table_no: int,
    table_html: str,
    use_full_page: bool = False,
    num_tables: int = 1,
) -> None:
    """Write fixture table HTML to wiki_cache so get_table_html_cached hits cache."""
    key = _cache_key(url, table_no, use_full_page)
    cache_dir = PROJECT_ROOT / "data" / "wiki_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{key}.json.gz"
    with gzip.open(cache_path, "wt", encoding="utf-8") as f:
        json.dump(
            {"table_no": table_no, "num_tables": num_tables, "html": table_html},
            f,
            ensure_ascii=False,
        )


def main() -> int:
    # 1. Init test DB (schema + seed)
    init_db()
    conn = get_connection()
    try:
        # 2. Get country_id (United States from seed)
        row = conn.execute("SELECT id FROM countries WHERE name = ? LIMIT 1", ("United States of America",)).fetchone()
        if not row:
            print("ERROR: Seed data missing (no United States country).", file=sys.stderr)
            return 1
        country_id = row[0]

        # 3. Load fixture and config from manifest
        manifest_path = PROJECT_ROOT / "test_scripts" / "manifest" / "parser_tests.json"
        if not manifest_path.exists():
            print("ERROR: Manifest not found.", file=sys.stderr)
            return 1
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not manifest:
            print("ERROR: Manifest is empty.", file=sys.stderr)
            return 1
        entry = manifest[0]
        source_url = (entry.get("source_url") or "").strip()
        html_file = entry.get("html_file") or ""
        config = dict(entry.get("config_json") or {})
        if not source_url or not html_file:
            print("ERROR: First manifest entry missing source_url or html_file.", file=sys.stderr)
            return 1

        html_path = PROJECT_ROOT / html_file.replace("/", os.sep)
        if not html_path.exists():
            print(f"ERROR: Fixture HTML not found: {html_path}", file=sys.stderr)
            return 1
        full_html = html_path.read_text(encoding="utf-8")
        table_no = int(config.get("table_no", 1))
        table_html, num_tables = _extract_table_no_and_count(full_html, table_no)

        # 4. Create one test office (source_page + office_details + office_table_config)
        office_data = {
            "country_id": country_id,
            "url": source_url,
            "name": "Test Secretary of Commerce (run_scenarios_test)",
            "enabled": 1,
            **config,
        }
        office_details_id = db_offices.create_office(office_data, conn=conn)
        conn.commit()

        # Get office_table_config id (runner uses this as office_id in list_runnable_units)
        tc_row = conn.execute(
            "SELECT id FROM office_table_config WHERE office_details_id = ? ORDER BY id LIMIT 1",
            (office_details_id,),
        ).fetchone()
        if not tc_row:
            print("ERROR: No office_table_config created.", file=sys.stderr)
            return 1
        tc_id = tc_row[0]
    finally:
        conn.close()

    # 5. Pre-fill table cache so runner does not hit the network
    _write_fixture_to_cache(
        source_url,
        table_no,
        table_html,
        use_full_page=bool(config.get("use_full_page_for_table", False)),
        num_tables=num_tables,
    )

    # 6. Scenario 1 – Delta run (write terms)
    result = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=[tc_id],
        run_office_bio=False,
    )
    if result.get("office_count", 0) == 0:
        print("ERROR: Delta run processed no offices.", file=sys.stderr)
        return 1
    terms = db_office_terms.get_existing_terms_for_office(tc_id)
    expected_min = 5  # manifest expected_json has many rows; require at least 5
    if len(terms) < expected_min:
        print(f"ERROR: Expected at least {expected_min} terms, got {len(terms)}.", file=sys.stderr)
        return 1
    # Spot-check first term
    first = terms[0]
    if not (first.get("wiki_url") and (first.get("term_start") or first.get("term_end") or first.get("term_start_year") or first.get("term_end_year"))):
        print("ERROR: First term missing wiki_url or term dates.", file=sys.stderr)
        return 1

    # 7. Scenario 2 – Populate-terms (same code path as UI; we already wrote above, just re-run and assert)
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=[tc_id],
        run_office_bio=False,
    )
    terms2 = db_office_terms.get_existing_terms_for_office(tc_id)
    if len(terms2) < expected_min:
        print(f"ERROR: After second run expected at least {expected_min} terms, got {len(terms2)}.", file=sys.stderr)
        return 1

    # 8. Scenario 3 – Preview / dry run (no DB write)
    result3 = run_with_db(
        run_mode="delta",
        dry_run=True,
        test_run=False,
        office_ids=[tc_id],
        run_office_bio=False,
    )
    preview = result3.get("preview_rows") or []
    if not preview:
        print("ERROR: Dry run returned no preview_rows.", file=sys.stderr)
        return 1
    if not any(p.get("Wiki Link") and (p.get("Term Start") or p.get("Term End")) for p in preview[:5]):
        print("ERROR: Preview rows missing expected shape (Wiki Link, Term Start/End).", file=sys.stderr)
        return 1

    print("OK: All run scenarios passed (delta write, populate-terms, dry-run preview).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
