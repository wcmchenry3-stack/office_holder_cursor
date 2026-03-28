"""
Smoke test for the nightly delta job pipeline.

Picks the first 3 enabled manifest entries, seeds them into a fresh DB, pre-populates
the disk cache with fixture HTML, then calls run_with_db(run_mode="delta") exactly as
the nightly job does.  Zero network calls.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import src.scraper.wiki_fetch as _wiki_fetch_module
from tests.conftest import PROJECT_ROOT, _extract_table, _write_fixture_to_cache
from tests.test_e2e_runner import (
    _build_member_html_map,
    _get_country_id,
    _load_html,
    _load_manifest,
    _make_requests_mock,
    _seed_office,
)

SMOKE_ENTRY_COUNT = 3


@pytest.mark.integration
def test_nightly_delta_parses_offices_and_finds_individuals(db_with_cache, monkeypatch):
    """
    Simulate the nightly delta run against 3 manifest pages.

    Asserts:
    - All 3 offices are processed (office_count == 3).
    - At least one individual term is found across the pages (terms_parsed > 0).
    - No office-level errors occurred.
    """
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.scraper.runner import run_with_db

    manifest = _load_manifest()
    enabled = [e for e in manifest if e.get("enabled")]
    assert len(enabled) >= SMOKE_ENTRY_COUNT, (
        f"Need at least {SMOKE_ENTRY_COUNT} enabled manifest entries for smoke test, "
        f"found {len(enabled)}"
    )
    entries = enabled[:SMOKE_ENTRY_COUNT]

    member_html = _build_member_html_map(entries)
    monkeypatch.setattr(_wiki_fetch_module, "_session", _make_requests_mock(member_html))

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        tc_ids: list[int] = []

        for entry in entries:
            source_url = (entry.get("source_url") or "").strip()
            config = dict(entry.get("config_json") or {})
            table_no = int(config.get("table_no", 1))

            full_html = _load_html(entry.get("html_file") or "")
            table_html, num_tables = _extract_table(full_html, table_no)

            _write_fixture_to_cache(
                cache_dir,
                source_url,
                table_no,
                table_html,
                use_full_page=bool(config.get("use_full_page_for_table", False)),
                num_tables=num_tables,
            )

            _, tc_id = _seed_office(conn, country_id, entry)
            tc_ids.append(tc_id)
    finally:
        conn.close()

    result = run_with_db(
        run_mode="delta",
        run_bio=False,
        run_office_bio=False,
        office_ids=tc_ids,
    )

    assert result["office_count"] == SMOKE_ENTRY_COUNT, (
        f"Expected {SMOKE_ENTRY_COUNT} offices processed, got {result['office_count']}"
    )
    assert result["terms_parsed"] > 0, "No individual terms found — scraper may have failed silently"
    assert result["office_errors"] == [], f"Office errors: {result['office_errors']}"
