"""
Integration test: full scraper pipeline using fixture HTML and a temp DB.

Converted from scripts/run_scenarios_test.py.
Covers delta run → idempotent re-run → dry-run preview, all without network calls.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.conftest import PROJECT_ROOT, _extract_table, _write_fixture_to_cache

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_full_scraper_scenario(db_with_cache, monkeypatch):
    """
    Delta run populates terms from fixture HTML.
    Re-run is idempotent (no duplicate terms).
    Dry-run preview returns expected row shape.
    All without network calls — wiki cache is pre-filled with fixture HTML.
    """
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import offices as db_offices
    from src.db import office_terms as db_office_terms
    from src.scraper.runner import run_with_db

    conn = get_connection(db_path)
    try:
        # 1. Get country_id from seed data
        row = conn.execute(
            "SELECT id FROM countries WHERE name = ? LIMIT 1",
            ("United States of America",),
        ).fetchone()
        assert row, "Seed data missing: no United States country"
        country_id = row[0]

        # 2. Load first manifest entry
        manifest_path = PROJECT_ROOT / "test_scripts" / "manifest" / "parser_tests.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest, "Manifest is empty"
        entry = manifest[0]
        source_url = (entry.get("source_url") or "").strip()
        html_file = entry.get("html_file") or ""
        config = dict(entry.get("config_json") or {})
        assert source_url and html_file, "First manifest entry missing source_url or html_file"

        html_path = PROJECT_ROOT / html_file.replace("/", os.sep)
        assert html_path.exists(), f"Fixture HTML not found: {html_path}"
        full_html = html_path.read_text(encoding="utf-8")
        table_no = int(config.get("table_no", 1))
        table_html, num_tables = _extract_table(full_html, table_no)

        # 3. Seed one test office
        office_data = {
            "country_id": country_id,
            "url": source_url,
            "name": "Test Office (test_scenarios)",
            "enabled": 1,
            **config,
        }
        office_details_id = db_offices.create_office(office_data, conn=conn)
        conn.commit()

        tc_row = conn.execute(
            "SELECT id FROM office_table_config WHERE office_details_id = ? ORDER BY id LIMIT 1",
            (office_details_id,),
        ).fetchone()
        assert tc_row, "No office_table_config row created"
        tc_id = tc_row[0]
    finally:
        conn.close()

    # 4. Pre-fill wiki cache so runner never hits the network
    _write_fixture_to_cache(
        cache_dir,
        source_url,
        table_no,
        table_html,
        use_full_page=bool(config.get("use_full_page_for_table", False)),
        num_tables=num_tables,
    )

    # 5. Scenario 1 — delta run writes terms
    result = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=[tc_id],
        run_office_bio=False,
    )
    assert result.get("office_count", 0) > 0, "Delta run processed no offices"

    conn2 = get_connection(db_path)
    try:
        terms = db_office_terms.get_existing_terms_for_office(tc_id)
    finally:
        conn2.close()

    assert len(terms) >= 5, f"Expected at least 5 terms, got {len(terms)}"
    first = terms[0]
    has_date = any(
        first.get(k) for k in ("term_start", "term_end", "term_start_year", "term_end_year")
    )
    assert first.get("wiki_url") and has_date, "First term missing wiki_url or term dates"

    term_count_after_first_run = len(terms)

    # 6. Scenario 2 — re-run is idempotent
    run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=[tc_id],
        run_office_bio=False,
    )
    conn3 = get_connection(db_path)
    try:
        terms2 = db_office_terms.get_existing_terms_for_office(tc_id)
    finally:
        conn3.close()
    assert (
        len(terms2) == term_count_after_first_run
    ), f"Re-run changed term count: {term_count_after_first_run} → {len(terms2)}"

    # 7. Scenario 3 — dry-run preview returns expected shape
    result3 = run_with_db(
        run_mode="delta",
        dry_run=True,
        test_run=False,
        office_ids=[tc_id],
        run_office_bio=False,
    )
    preview = result3.get("preview_rows") or []
    assert preview, "Dry run returned no preview_rows"
    assert any(
        p.get("Wiki Link") and (p.get("Term Start") or p.get("Term End")) for p in preview[:5]
    ), "Preview rows missing expected shape (Wiki Link + Term Start/End)"
