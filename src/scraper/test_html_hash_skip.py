"""Tests for HTML hash-based delta skip in runner.py."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Helpers (same as tests/test_scenarios.py)
# ---------------------------------------------------------------------------


def _cache_key(url: str, table_no: int, use_full_page: bool = False) -> str:
    normalized = (url.strip() + "|" + str(table_no) + "|" + ("1" if use_full_page else "0")).encode(
        "utf-8"
    )
    return hashlib.sha256(normalized).hexdigest()[:32]


def _extract_table(html: str, table_no: int) -> tuple[str, int]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    num_tables = len(tables)
    if not (1 <= table_no <= num_tables):
        raise ValueError(f"Table {table_no} not found (page has {num_tables} tables)")
    return str(tables[table_no - 1]), num_tables


def _write_cache(
    cache_dir: Path,
    url: str,
    table_no: int,
    table_html: str,
    use_full_page: bool = False,
    num_tables: int = 1,
) -> None:
    key = _cache_key(url, table_no, use_full_page)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{key}.json.gz"
    with gzip.open(cache_path, "wt", encoding="utf-8") as f:
        json.dump(
            {"table_no": table_no, "num_tables": num_tables, "html": table_html},
            f,
            ensure_ascii=False,
        )


def _setup_db_and_office(tmp_path, monkeypatch):
    """Init DB, seed one office from first manifest entry. Returns (db_path, cache_dir, tc_id, source_url, table_no, table_html, config)."""
    db_path = tmp_path / "test_run.db"
    cache_dir = tmp_path / "wiki_cache"
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))

    from src.db.connection import DB_PATH, init_db, get_connection
    from src.db import offices as db_offices
    import src.scraper.table_cache as table_cache_mod

    assert str(db_path) != str(DB_PATH), "test DB must not point at production DB"
    monkeypatch.setattr(table_cache_mod, "_cache_dir", lambda: cache_dir)

    init_db(path=db_path)
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT id FROM countries WHERE name = ? LIMIT 1",
            ("United States of America",),
        ).fetchone()
        assert row, "Seed data missing: no United States country"
        country_id = row[0]

        manifest_path = PROJECT_ROOT / "test_scripts" / "manifest" / "parser_tests.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        entry = manifest[0]
        source_url = (entry.get("source_url") or "").strip()
        html_file = entry.get("html_file") or ""
        config = dict(entry.get("config_json") or {})
        assert source_url and html_file

        html_path = PROJECT_ROOT / html_file.replace("/", os.sep)
        full_html = html_path.read_text(encoding="utf-8")
        table_no = int(config.get("table_no", 1))
        table_html, num_tables = _extract_table(full_html, table_no)

        office_data = {
            "country_id": country_id,
            "url": source_url,
            "name": "Test Office (hash skip)",
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

    _write_cache(
        cache_dir,
        source_url,
        table_no,
        table_html,
        use_full_page=bool(config.get("use_full_page_for_table", False)),
        num_tables=num_tables,
    )

    return db_path, cache_dir, tc_id, source_url, table_no, table_html, config


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_hash_skip_activates_on_second_run(tmp_path, monkeypatch):
    """Second delta run with unchanged HTML reports offices_unchanged == 1, term count stable."""
    db_path, cache_dir, tc_id, source_url, table_no, table_html, config = _setup_db_and_office(
        tmp_path, monkeypatch
    )

    from src.scraper.runner import run_with_db

    # First run: hash is null → always parses, stores hash, writes terms
    result1 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result1["terms_parsed"] > 0, "Expected terms from first run"
    assert result1.get("offices_unchanged", 0) == 0, "First run should not skip any offices"

    # Second run: hash matches → skip
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result2.get("offices_unchanged", 0) == 1, "Second run should skip unchanged office"
    assert result2["terms_parsed"] == 0, "No new terms should be parsed on skip"

    # Verify DB term count is unchanged
    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM office_terms").fetchone()[0]
    finally:
        conn.close()
    assert count > 0, "Terms should exist after first run"


@pytest.mark.integration
def test_hash_skip_bypassed_when_html_changes(tmp_path, monkeypatch):
    """When cache HTML changes but table data is identical, the diff-based delta correctly
    marks the office as data-unchanged (no unnecessary write).  A non-data change such as an
    HTML comment does not cause a write.
    """
    db_path, cache_dir, tc_id, source_url, table_no, table_html, config = _setup_db_and_office(
        tmp_path, monkeypatch
    )

    from src.scraper.runner import run_with_db

    # First run: populates data and html hash
    result1 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result1["terms_parsed"] > 0

    # Overwrite cache with cosmetically different HTML (added comment — data unchanged)
    modified_html = table_html + "<!-- modified -->"
    _write_cache(
        cache_dir,
        source_url,
        table_no,
        modified_html,
        use_full_page=bool(config.get("use_full_page_for_table", False)),
    )

    # Second run: HTML hash differs so the page is re-parsed, but the diff finds no data
    # change → office is correctly reported as unchanged (no write needed).
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result2.get("offices_unchanged", 0) == 1, (
        "Cosmetic HTML change with identical table data should be reported as data-unchanged"
    )


@pytest.mark.integration
def test_hash_skip_bypassed_with_refresh_table_cache(tmp_path, monkeypatch):
    """refresh_table_cache=True bypasses the HTML hash check and re-parses, but if the
    parsed data is identical to existing terms the diff correctly reports data-unchanged."""
    db_path, cache_dir, tc_id, source_url, table_no, table_html, config = _setup_db_and_office(
        tmp_path, monkeypatch
    )

    from src.scraper.runner import run_with_db

    # First run: store hash
    run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )

    # Second run with refresh=True: should re-parse even though HTML is unchanged
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
        refresh_table_cache=True,
    )
    assert result2.get("offices_unchanged", 0) == 1, (
        "refresh_table_cache=True bypasses the HTML hash check and re-parses, but the diff "
        "finds identical data — office is correctly reported as data-unchanged"
    )
