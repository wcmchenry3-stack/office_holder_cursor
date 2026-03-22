"""
Integration test: full scraper pipeline using fixture HTML and a temp DB.

Converted from scripts/run_scenarios_test.py.
Covers delta run → idempotent re-run → dry-run preview, all without network calls.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers (ported from run_scenarios_test.py)
# ---------------------------------------------------------------------------


def _cache_key(url: str, table_no: int, use_full_page: bool = False) -> str:
    """Match table_cache._cache_key so we write the same key the runner will read."""
    normalized = (url.strip() + "|" + str(table_no) + "|" + ("1" if use_full_page else "0")).encode(
        "utf-8"
    )
    return hashlib.sha256(normalized).hexdigest()[:32]


def _extract_table(html: str, table_no: int) -> tuple[str, int]:
    """Extract the N-th <table> from full page HTML (1-based). Returns (table_html, num_tables)."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    num_tables = len(tables)
    if not (1 <= table_no <= num_tables):
        raise ValueError(f"Table {table_no} not found (page has {num_tables} tables)")
    return str(tables[table_no - 1]), num_tables


def _write_fixture_to_cache(
    cache_dir: Path,
    url: str,
    table_no: int,
    table_html: str,
    use_full_page: bool = False,
    num_tables: int = 1,
) -> None:
    """Write fixture HTML to wiki_cache so get_table_html_cached hits cache."""
    key = _cache_key(url, table_no, use_full_page)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{key}.json.gz"
    with gzip.open(cache_path, "wt", encoding="utf-8") as f:
        json.dump(
            {"table_no": table_no, "num_tables": num_tables, "html": table_html},
            f,
            ensure_ascii=False,
        )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_full_scraper_scenario(tmp_path, monkeypatch):
    """
    Delta run populates terms from fixture HTML.
    Re-run is idempotent (no duplicate terms).
    Dry-run preview returns expected row shape.
    All without network calls — wiki cache is pre-filled with fixture HTML.
    """
    # 1. Point DB and wiki cache at tmp dirs before importing app modules
    db_path = tmp_path / "test_run.db"
    cache_dir = tmp_path / "wiki_cache"
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))

    from src.db.connection import DB_PATH, init_db, get_connection
    from src.db import offices as db_offices
    from src.db import office_terms as db_office_terms
    from src.scraper.runner import run_with_db
    import src.scraper.table_cache as table_cache_mod

    # Safety guard
    assert str(db_path) != str(DB_PATH), "test DB must not point at production DB"

    # Redirect wiki cache to a temp dir so tests never read from or write to data/wiki_cache
    monkeypatch.setattr(table_cache_mod, "_cache_dir", lambda: cache_dir)

    # 2. Init DB
    init_db(path=db_path)
    conn = get_connection(db_path)
    try:
        # 3. Get country_id from seed data
        row = conn.execute(
            "SELECT id FROM countries WHERE name = ? LIMIT 1",
            ("United States of America",),
        ).fetchone()
        assert row, "Seed data missing: no United States country"
        country_id = row[0]

        # 4. Load first manifest entry
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

        # 5. Seed one test office
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

    # 6. Pre-fill wiki cache so runner never hits the network
    _write_fixture_to_cache(
        cache_dir,
        source_url,
        table_no,
        table_html,
        use_full_page=bool(config.get("use_full_page_for_table", False)),
        num_tables=num_tables,
    )

    # 7. Scenario 1 — delta run writes terms
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

    # 8. Scenario 2 — re-run is idempotent
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

    # 9. Scenario 3 — dry-run preview returns expected shape
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
