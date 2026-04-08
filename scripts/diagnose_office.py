"""Diagnose why a specific office is being skipped as unchanged in delta runs.

Usage:
    python scripts/diagnose_office.py --office-id 1283
    python scripts/diagnose_office.py --office-id 1283 --fetch   # live HTTP fetch

Shows:
- Office config (URL, table_no, hash stored vs. current)
- Existing terms in DB
- Parsed rows from Wikipedia (live or cached)
- Diff result (new/changed/unchanged/vanished)
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.connection import get_connection, init_db
from src.db import offices as db_offices
from src.db import office_terms as db_office_terms
from src.db import parties as db_parties
from src.scraper.table_cache import get_table_html_cached
from src.scraper.runner import (
    _diff_office_table,
    _term_data_changed,
    _parse_office_html,
)
from src.scraper import parse_core


def main():
    parser = argparse.ArgumentParser(description="Diagnose office delta-run behaviour")
    parser.add_argument(
        "--office-id",
        type=int,
        required=True,
        help="office_table_config_id (= office_details_id for most offices)",
    )
    parser.add_argument(
        "--fetch", action="store_true", help="Force fresh HTTP fetch instead of cached HTML"
    )
    args = parser.parse_args()

    office_id = args.office_id

    # Load office config
    conn = get_connection()
    units = db_offices.list_runnable_units(conn=conn)
    office_row = next(
        (
            u
            for u in units
            if u.get("office_table_config_id") == office_id or u.get("id") == office_id
        ),
        None,
    )

    if office_row is None:
        print(f"ERROR: office_table_config_id={office_id} not found in runnable units")
        sys.exit(1)

    url = (office_row.get("url") or "").strip()
    table_no = int(office_row.get("table_no") or 1)
    stored_hash = office_row.get("last_html_hash")
    office_name = office_row.get("name") or f"Office {office_id}"
    years_only = bool(office_row.get("years_only"))
    use_infobox = bool(office_row.get("find_date_in_infobox"))

    print("=" * 70)
    print(f"Office: {office_name} (id={office_id})")
    print(f"URL:    {url}")
    print(f"table_no={table_no}, years_only={years_only}, use_infobox={use_infobox}")
    print(f"stored last_html_hash: {'<none>' if not stored_hash else stored_hash[:16] + '...'}")
    print()

    # Load existing terms
    existing_terms = db_office_terms.get_existing_terms_for_office(office_id)
    print(f"Existing terms in DB: {len(existing_terms)}")
    for t in existing_terms[-5:]:
        print(
            f"  id={t['id']:6d}  {t.get('full_name') or '(no name)':30s}  "
            f"start={t.get('term_start') or t.get('term_start_year')}  "
            f"end={t.get('term_end') or t.get('term_end_year') or 'NULL'}  "
            f"url={t.get('wiki_url', '')[:60]}"
        )
    if len(existing_terms) > 5:
        print(f"  ... ({len(existing_terms) - 5} more not shown)")
    print()

    # Fetch HTML
    print(f"Fetching HTML (refresh={args.fetch}) ...")
    cache_result = get_table_html_cached(
        url, table_no, refresh=args.fetch, use_full_page=False, run_cache=None
    )
    if "error" in cache_result:
        print(f"ERROR fetching HTML: {cache_result['error']}")
        sys.exit(1)

    html_content = cache_result.get("html") or ""
    if cache_result.get("cache_file"):
        print(f"Cache file: {cache_result['cache_file']}")

    current_hash = (
        hashlib.sha256(html_content.encode("utf-8")).hexdigest() if html_content else None
    )
    print(f"Current HTML hash:  {'<none>' if not current_hash else current_hash[:16] + '...'}")
    print(f"Stored HTML hash:   {'<none>' if not stored_hash else stored_hash[:16] + '...'}")
    if current_hash and stored_hash:
        if current_hash == stored_hash:
            print("*** HASH MATCH — delta run would skip early (no re-parse) ***")
        else:
            print("Hash MISMATCH — delta run proceeds to diff")
    print()

    if not html_content:
        print("ERROR: no HTML content")
        sys.exit(1)

    # Parse the table (no infobox for pre-parse diff)
    office_row_no_infobox = {**office_row, "find_date_in_infobox": False}

    data_cleanup = parse_core.DataCleanup()
    biography = parse_core.Biography(data_cleanup)
    offices_parser = parse_core.Offices(biography, data_cleanup)
    party_list = db_parties.get_party_list_for_scraper()

    print("Parsing table (no infobox) ...")
    table_data_pre = _parse_office_html(
        office_row_no_infobox,
        html_content,
        url,
        party_list,
        offices_parser,
        cached_table_html=html_content,
        progress_callback=None,
        max_rows=None,
        run_cache=None,
    )
    print(f"Parsed rows: {len(table_data_pre)}")
    for row in table_data_pre[-10:]:
        print(
            f"  {row.get('Name') or row.get('Wiki Link', '')[:40]:40s}  "
            f"start={row.get('Term Start') or row.get('Term Start Year')}  "
            f"end={row.get('Term End') or row.get('Term End Year') or 'None'}  "
            f"link={row.get('Wiki Link', '')[:50]}"
        )
    print()

    if not existing_terms:
        print("No existing terms — would be first-time insert (all rows are new).")
        return

    # Run diff
    diff = _diff_office_table(existing_terms, table_data_pre, office_id, years_only, use_infobox)
    print("Diff result:")
    print(f"  new_rows:          {len(diff['new_rows'])}")
    print(f"  changed_rows:      {len(diff['changed_rows'])}")
    print(f"  unchanged_rows:    {len(diff['unchanged_rows'])}")
    print(
        f"  vanished_real_ids: {len(diff['vanished_real_ids'])} -> {diff['vanished_real_ids'][:5]}"
    )
    print(f"  placeholder_ids:   {len(diff['placeholder_ids'])}")
    print()

    if diff["new_rows"]:
        print("NEW rows (would be inserted):")
        for row in diff["new_rows"]:
            print(
                f"  {row.get('Name') or row.get('Wiki Link', '')[:40]}  "
                f"start={row.get('Term Start')}  end={row.get('Term End')}"
            )
        print()

    if diff["changed_rows"]:
        print("CHANGED rows (would be updated):")
        for row in diff["changed_rows"]:
            eid = row.get("_existing_term_id")
            existing = next((t for t in existing_terms if t["id"] == eid), {})
            print(f"  {row.get('Name') or row.get('Wiki Link', '')[:40]}")
            print(f"    DB:     start={existing.get('term_start')}  end={existing.get('term_end')}")
            print(f"    Parsed: start={row.get('Term Start')}  end={row.get('Term End')}")
        print()

    if not diff["new_rows"] and not diff["changed_rows"] and not diff["placeholder_ids"]:
        print("*** DELTA RUN CONCLUSION: data unchanged — office skipped (no write) ***")
        if diff["vanished_real_ids"]:
            print(
                f"    NOTE: {len(diff['vanished_real_ids'])} vanished holder(s) kept (not deleted by design)"
            )

    conn.close()


if __name__ == "__main__":
    main()
