#!/usr/bin/env python3
"""Debug table matching for Michigan Attorney General (or any office/page).

This script compares existing office_terms holder URLs to each table on the page and
shows which table best matches by URL overlap (same logic family used by runner).

Usage examples:
  python scripts/debug_mi_atty_gen_table_match.py
  python scripts/debug_mi_atty_gen_table_match.py --office-table-config-id 216
  python scripts/debug_mi_atty_gen_table_match.py --url https://en.wikipedia.org/wiki/Michigan_Attorney_General --current-table-no 3
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.db.connection import init_db
from src.db import offices as db_offices
from src.db import office_terms as db_office_terms
from src.db import parties as db_parties
from src.scraper.table_cache import get_table_html_cached
from src.scraper import parse_core
from src.scraper.logger import Logger
from src.scraper.runner import _parse_office_html, _canonical_holder_url

DEFAULT_URL = "https://en.wikipedia.org/wiki/Michigan_Attorney_General"


def _existing_urls_for_table_config(tc_id: int) -> set[str]:
    rows = db_office_terms.get_existing_terms_for_office(tc_id)
    out: set[str] = set()
    for r in rows:
        u = _canonical_holder_url((r.get("wiki_url") or "").strip())
        if u:
            out.add(u)
    return out


def _parsed_urls_for_table(office_row: dict, table_no: int) -> set[str]:
    url = (office_row.get("url") or "").strip()
    use_full_page = bool(office_row.get("use_full_page_for_table"))
    cached = get_table_html_cached(url, table_no, refresh=False, use_full_page=use_full_page)
    html = cached.get("html") or ""
    if not html:
        return set()

    log = Logger("debug_match", "Office")
    cleanup = parse_core.DataCleanup(log)
    bio = parse_core.Biography(log, cleanup)
    parser = parse_core.Offices(log, bio, cleanup)
    party_list = db_parties.get_party_list_for_scraper()

    office_row_no_infobox = {**office_row, "table_no": table_no, "find_date_in_infobox": False}
    rows = _parse_office_html(
        office_row_no_infobox,
        html,
        url,
        party_list,
        parser,
        cached_table_html=html,
        progress_callback=None,
    )
    out: set[str] = set()
    for r in rows:
        u = _canonical_holder_url((r.get("Wiki Link") or "").strip())
        if u and u != "No link":
            out.add(u)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--office-table-config-id", type=int, default=0, help="office_table_config.id to debug"
    )
    ap.add_argument("--url", default=DEFAULT_URL, help="Wikipedia page URL")
    ap.add_argument(
        "--current-table-no",
        type=int,
        default=0,
        help="current table_no (if not using office-table-config-id)",
    )
    args = ap.parse_args()

    init_db()

    if args.office_table_config_id:
        office_row = db_offices.get_office_by_table_config_id(args.office_table_config_id)
        if not office_row:
            print(f"Table config not found: {args.office_table_config_id}")
            return 2
        tc_id = int(args.office_table_config_id)
    else:
        # best-effort row for page URL
        units = [
            u
            for u in db_offices.list_runnable_units()
            if (u.get("url") or "").strip() == args.url.strip()
        ]
        if not units:
            print("No runnable unit found for URL; pass --office-table-config-id")
            return 2
        office_row = units[0]
        tc_id = int(office_row.get("office_table_config_id") or office_row.get("id") or 0)

    page_url = (office_row.get("url") or "").strip()
    current_tno = int(args.current_table_no or office_row.get("table_no") or 1)

    existing_urls = _existing_urls_for_table_config(tc_id)
    if not existing_urls:
        print("No existing office_terms URLs found; run populate first.")
        return 2

    first = get_table_html_cached(
        page_url, 1, refresh=False, use_full_page=bool(office_row.get("use_full_page_for_table"))
    )
    n_tables = int(first.get("num_tables") or 0)
    if n_tables <= 0:
        print(f"Could not load tables for {page_url}")
        return 2

    print(f"URL: {page_url}")
    print(f"table_config_id: {tc_id}")
    print(f"current table_no: {current_tno}")
    print(f"existing unique holder URLs: {len(existing_urls)}")
    print(f"tables on page: {n_tables}")
    print("-" * 72)

    best = None
    for tno in range(1, n_tables + 1):
        parsed_urls = _parsed_urls_for_table(office_row, tno)
        overlap = len(existing_urls & parsed_urls)
        missing = len(existing_urls - parsed_urls)
        extra = len(parsed_urls - existing_urls)
        score = (missing, -overlap, extra)
        if best is None or score < best[0]:
            best = (score, tno, overlap, missing, extra, parsed_urls)
        mark = "*" if tno == current_tno else " "
        print(
            f"{mark} table {tno:>2}: overlap={overlap:>3}  missing={missing:>3}  extra={extra:>3}  parsed={len(parsed_urls):>3}"
        )

    print("-" * 72)
    if best:
        _, best_tno, overlap, missing, extra, best_urls = best
        print(f"Best match table_no: {best_tno}")
        print(f"Best stats: overlap={overlap}, missing={missing}, extra={extra}")
        if best_tno != current_tno:
            print("Suggestion: update table_no to best match.")
        else:
            print("Current table appears to be best by URL overlap.")

        sample_missing = sorted(existing_urls - best_urls)[:20]
        if sample_missing:
            print("\nSample missing URLs (up to 20):")
            for u in sample_missing:
                print("  -", u)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
