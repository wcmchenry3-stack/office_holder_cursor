"""Run local parser test scripts against saved HTML samples."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import requests
from typing import Any

from bs4 import BeautifulSoup

from src.scraper import parse_core
from src.scraper.runner import parse_full_table_for_export
from src.scraper.wiki_fetch import wiki_url_to_rest_html_url, normalize_wiki_url
from src.db.infobox_role_key_filter import get_infobox_role_key_filter
from src.db.connection import ensure_data_dir, get_log_dir

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TEST_SCRIPTS_DIR = PROJECT_ROOT / "test_scripts"


def _fixture_path(name: str) -> Path:
    rel = (name or "").strip().replace("\\", "/")
    if rel.startswith("test_scripts/"):
        rel = rel[len("test_scripts/") :]
    return (TEST_SCRIPTS_DIR / rel).resolve()


def _load_member_fixture_html(cfg: dict[str, Any]) -> dict[str, str]:
    fixtures = cfg.get("_member_fixtures") if isinstance(cfg, dict) else None
    if not isinstance(fixtures, dict):
        return {}
    out: dict[str, str] = {}
    for raw_url, rel_file in fixtures.items():
        if not isinstance(raw_url, str) or not isinstance(rel_file, str):
            continue
        path = _fixture_path(rel_file)
        if TEST_SCRIPTS_DIR not in path.parents or not path.exists():
            continue
        html = path.read_text(encoding="utf-8")
        norm = normalize_wiki_url(raw_url) or raw_url
        out[norm] = html
        rest = wiki_url_to_rest_html_url(norm)
        if rest:
            out[rest] = html
    return out


@contextmanager
def _requests_get_with_member_fixtures(member_html_by_url: dict[str, str]):
    if not member_html_by_url:
        yield
        return

    original_get = requests.get

    class _Resp:
        def __init__(self, text: str):
            self.status_code = 200
            self.text = text

    def patched_get(url, *args, **kwargs):
        key = normalize_wiki_url(url) or url
        if key in member_html_by_url:
            return _Resp(member_html_by_url[key])
        rest = wiki_url_to_rest_html_url(key)
        if rest and rest in member_html_by_url:
            return _Resp(member_html_by_url[rest])
        if url in member_html_by_url:
            return _Resp(member_html_by_url[url])
        return original_get(url, *args, **kwargs)

    requests.get = patched_get
    try:
        yield
    finally:
        requests.get = original_get


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
    "ignore_non_links": False,
    "infobox_role_key": "",
}


def _load_html(html_file: str) -> str:
    raw_name = (html_file or "").strip()
    path = _fixture_path(raw_name)
    if TEST_SCRIPTS_DIR not in path.parents and path != TEST_SCRIPTS_DIR:
        raise ValueError("Invalid html path")
    # Backward compatibility: older DB rows stored basename-only fixture names.
    # If a direct lookup fails, also try test_scripts/fixtures/<basename>.
    if not path.exists() and raw_name and "/" not in raw_name.replace("\\", "/"):
        fallback = (TEST_SCRIPTS_DIR / "fixtures" / raw_name).resolve()
        if TEST_SCRIPTS_DIR in fallback.parents and fallback.exists():
            path = fallback
    if not path.exists():
        raise ValueError(f"HTML file not found: {html_file}")
    return path.read_text(encoding="utf-8")


def _run_table_test(html_content: str, cfg: dict[str, Any], source_url: str) -> Any:
    office_row = {**DEFAULT_TABLE_CONFIG, **(cfg or {})}
    url = source_url or office_row.get("url") or DEFAULT_TABLE_CONFIG["url"]

    # Match the real scraper path: select configured table_no first, then parse a single-table HTML fragment.
    table_no = int(office_row.get("table_no") or 1)
    soup = BeautifulSoup(html_content or "", "html.parser")
    tables = soup.find_all("table")
    if not (1 <= table_no <= len(tables)):
        return []
    selected_table_html = str(tables[table_no - 1])

    member_html_by_url = _load_member_fixture_html(office_row)
    with _requests_get_with_member_fixtures(member_html_by_url):
        return parse_full_table_for_export(office_row, selected_table_html, url)


def _run_bio_like_test(html_content: str, mode: str) -> dict[str, Any]:
    ensure_data_dir()
    cleanup = parse_core.DataCleanup()
    biography = parse_core.Biography(cleanup)
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


def run_test_script_from_html(
    *,
    test_type: str,
    html_content: str,
    config_json: dict[str, Any] | None = None,
    source_url: str = "",
    expected_json: Any = None,
) -> dict[str, Any]:
    parsed_type = (test_type or "table_config").strip()
    cfg = dict(config_json or {})
    if parsed_type == "table_config":
        raw_filter_id = cfg.get("infobox_role_key_filter_id")
        filter_id = None
        if isinstance(raw_filter_id, str):
            raw_filter_id = raw_filter_id.strip()
        if isinstance(raw_filter_id, int):
            filter_id = raw_filter_id
        elif isinstance(raw_filter_id, str) and raw_filter_id.isdigit():
            filter_id = int(raw_filter_id)
        if filter_id:
            role_filter = get_infobox_role_key_filter(filter_id)
            if role_filter and (role_filter.get("role_key") or "").strip():
                cfg["infobox_role_key"] = (role_filter.get("role_key") or "").strip()
    if parsed_type == "table_config":
        actual = _run_table_test(html_content, cfg, source_url.strip())
    elif parsed_type == "infobox":
        actual = _run_bio_like_test(html_content, "infobox")
    elif parsed_type == "bio":
        actual = _run_bio_like_test(html_content, "bio")
    else:
        raise ValueError(f"Unknown test type: {parsed_type}")
    expected = expected_json
    passed = expected is not None and json.dumps(
        actual, sort_keys=True, ensure_ascii=False
    ) == json.dumps(expected, sort_keys=True, ensure_ascii=False)
    return {"passed": passed, "actual": actual, "expected": expected}


def run_test_script(test_row: dict[str, Any]) -> dict[str, Any]:
    html_content = _load_html(test_row.get("html_file") or "")
    return run_test_script_from_html(
        test_type=(test_row.get("test_type") or "table_config"),
        html_content=html_content,
        config_json=test_row.get("config_json") or {},
        source_url=(test_row.get("source_url") or "").strip(),
        expected_json=test_row.get("expected_json"),
    )
