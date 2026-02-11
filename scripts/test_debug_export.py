#!/usr/bin/env python3
"""
Test parser against a debug export file.
Usage: python scripts/test_debug_export.py debug/Secretary_of_the_Navy_2026-02-08T12-16-44.txt

Reads CONFIG and RAW HTML from the file, runs the same pipeline as preview,
and prints Term Start / Term End for the first rows to verify date parsing.
"""
import re
import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

def parse_debug_file(filepath: Path) -> tuple[dict, str]:
    """Parse debug export file. Returns (config_dict, raw_html_string)."""
    text = filepath.read_text(encoding="utf-8")
    config = {}
    raw_html = ""

    # CONFIG section: key: value (after === CONFIG === until next section === XXX ===)
    # Use \n=== to avoid matching === at end of "=== CONFIG (form values...) ==="
    config_start = text.find("=== CONFIG ")
    if config_start == -1:
        return config, raw_html
    next_section = text.find("\n===", config_start + 1)
    config_end = next_section if next_section != -1 else len(text)
    config_block = text[config_start:config_end]
    for line in config_block.splitlines():
        if ":" in line and not line.strip().startswith("=="):
            k, v = line.split(":", 1)
            k, v = k.strip(), v.strip()
            if k in ("table_no", "table_rows", "link_column", "party_column",
                     "term_start_column", "term_end_column", "district_column",
                     "country_id", "level_id", "branch_id", "state_id"):
                try:
                    config[k] = int(v) if v and v != "None" else 0
                except ValueError:
                    config[k] = 0
            elif k in ("dynamic_parse", "find_date_in_infobox", "years_only", "read_right_to_left",
                       "parse_rowspan", "party_link", "rep_link"):
                config[k] = v and v.lower() in ("true", "1", "yes")
            else:
                config[k] = v if v != "None" else None

    # RAW HTML section
    raw_start = text.find("=== RAW HTML (selected table) ===")
    if raw_start != -1:
        raw_start = text.find("\n", raw_start) + 1
        raw_html = text[raw_start:].strip()
    return config, raw_html


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_debug_export.py debug/<filename>.txt")
        sys.exit(1)
    filepath = ROOT / sys.argv[1].lstrip("/")
    if not filepath.is_file():
        print(f"File not found: {filepath}")
        sys.exit(1)

    config, raw_html = parse_debug_file(filepath)
    if not raw_html or not raw_html.startswith("<"):
        print("Could not find RAW HTML in file.")
        sys.exit(1)

    # Build office_row as the preview API does (form values = 1-based columns)
    office_row = {
        "url": config.get("url") or "",
        "name": config.get("name") or "",
        "department": config.get("department") or "",
        "notes": config.get("notes") or "",
        "table_no": int(config.get("table_no") or 1),
        "table_rows": int(config.get("table_rows") or 4),
        "link_column": int(config.get("link_column") or 0),
        "party_column": int(config.get("party_column") or 0),
        "term_start_column": int(config.get("term_start_column") or 4),
        "term_end_column": int(config.get("term_end_column") or 5),
        "district_column": int(config.get("district_column") or 0),
        "dynamic_parse": config.get("dynamic_parse", True),
        "read_right_to_left": config.get("read_right_to_left", False),
        "find_date_in_infobox": config.get("find_date_in_infobox", False),
        "years_only": config.get("years_only", False),
        "parse_rowspan": config.get("parse_rowspan", False),
        "rep_link": config.get("rep_link", False),
        "party_link": config.get("party_link", False),
        "alt_link": config.get("alt_link"),
        "country_name": "",
        "level_name": "",
        "branch_name": "",
        "state_name": "",
    }

    from src.db import connection
    from src.db import offices as db_offices
    from src.db import parties as db_parties
    from src.scraper import parse_core
    from src.scraper.runner import get_log_dir
    from src.scraper.logger import Logger

    connection.init_db()
    log_dir = get_log_dir()
    logger = Logger("test_debug_export", "Office", log_dir=log_dir)
    party_list = db_parties.get_party_list_for_scraper()
    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)
    offices_parser = parse_core.Offices(logger, biography, data_cleanup)

    table_config = db_offices.office_row_to_table_config(office_row)
    office_details = db_offices.office_row_to_office_details(office_row)
    url = office_row.get("url") or ""

    # RAW HTML from debug export is the selected table only — use table_no=1
    table_config["table_no"] = 1

    print("Parsed from file (1-based): term_start_column=%s term_end_column=%s link_column=%s" % (
        office_row.get("term_start_column"), office_row.get("term_end_column"), office_row.get("link_column")))
    print("Table config (0-based columns):", {k: v for k, v in table_config.items() if "column" in k})
    print()

    try:
        table_data = offices_parser.process_table(
            raw_html, table_config, office_details, url, party_list
        )
    except Exception as e:
        print(f"Parser error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print(f"Parsed {len(table_data)} rows. First 5:")
    for i, row in enumerate(table_data[:5]):
        ts = row.get("Term Start", "")
        te = row.get("Term End", "")
        tsy = row.get("Term Start Year")
        tey = row.get("Term End Year")
        link = (row.get("Wiki Link") or "")[:50]
        year_part = f"  Start year={tsy!r}  End year={tey!r}" if (tsy is not None or tey is not None) else ""
        print(f"  {i+1}. Term Start={ts!r}  Term End={te!r}{year_part}  Link={link}...")
    invalid = sum(1 for r in table_data if r.get("Term Start") == "Invalid date" or r.get("Term End") == "Invalid date")
    if invalid:
        print(f"\nResult: {invalid} rows with Invalid date (ISSUE NOT RESOLVED)")
    else:
        print(f"\nResult: No Invalid dates in parsed rows (dates OK)")


if __name__ == "__main__":
    main()
