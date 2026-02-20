"""Run local parser test scripts against saved HTML samples."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from src.scraper.logger import Logger
from src.scraper import parse_core
from src.scraper.runner import parse_full_table_for_export
from src.db.connection import ensure_data_dir, get_log_dir


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TEST_SCRIPTS_DIR = PROJECT_ROOT / "test_scripts"


DEFAULT_TABLE_CONFIG = {
    "url": "https://en.wikipedia.org/wiki/Sample",
    "name": "Sample Office",
    "country_name": "United States",
    "state_name": "",
    "level_name": "Federal",
    "branch_name": "Legislative",
    "table_no": 1,
    "table_rows": 4,
    "link_column": 1,
    "party_column": 0,
    "term_start_column": 4,
    "term_end_column": 5,
    "district_column": 0,
    "dynamic_parse": True,
    "read_right_to_left": False,
    "find_date_in_infobox": False,
    "years_only": False,
    "parse_rowspan": False,
    "consolidate_rowspan_terms": False,
    "rep_link": False,
    "party_link": False,
    "alt_links": [],
    "alt_link_include_main": False,
    "use_full_page_for_table": False,
    "term_dates_merged": False,
    "party_ignore": False,
    "district_ignore": False,
    "district_at_large": False,
}


def _load_html(html_file: str) -> str:
    path = (TEST_SCRIPTS_DIR / (html_file or "")).resolve()
    if TEST_SCRIPTS_DIR not in path.parents and path != TEST_SCRIPTS_DIR:
        raise ValueError("Invalid html path")
    if not path.exists():
        raise ValueError(f"HTML file not found: {html_file}")
    return path.read_text(encoding="utf-8")


def _run_table_test(html_content: str, cfg: dict[str, Any], source_url: str) -> Any:
    office_row = {**DEFAULT_TABLE_CONFIG, **(cfg or {})}
    url = source_url or office_row.get("url") or DEFAULT_TABLE_CONFIG["url"]
    return parse_full_table_for_export(office_row, html_content, url)


def _run_bio_like_test(html_content: str, mode: str) -> dict[str, Any]:
    ensure_data_dir()
    logger = Logger("test_script", "Office", log_dir=get_log_dir())
    cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, cleanup)
    soup = BeautifulSoup(html_content, "html.parser")
    infobox = soup.find("table", {"class": ["infobox vcard", "infobox biography vcard"]})
    if mode == "infobox":
        if not infobox:
            return {}
        return biography.parse_infobox(infobox)
    if infobox:
        return biography.parse_infobox(infobox)
    first_paragraph = soup.find("p")
    return biography.parse_first_paragraph(first_paragraph) if first_paragraph else {}


def run_test_script(test_row: dict[str, Any]) -> dict[str, Any]:
    html_content = _load_html(test_row.get("html_file") or "")
    test_type = (test_row.get("test_type") or "table_config").strip()
    config_json = test_row.get("config_json") or {}
    source_url = (test_row.get("source_url") or "").strip()
    if test_type == "table_config":
        actual = _run_table_test(html_content, config_json, source_url)
    elif test_type == "infobox":
        actual = _run_bio_like_test(html_content, "infobox")
    elif test_type == "bio":
        actual = _run_bio_like_test(html_content, "bio")
    else:
        raise ValueError(f"Unknown test type: {test_type}")
    expected = test_row.get("expected_json")
    passed = expected is not None and json.dumps(actual, sort_keys=True, ensure_ascii=False) == json.dumps(expected, sort_keys=True, ensure_ascii=False)
    return {"passed": passed, "actual": actual, "expected": expected}
