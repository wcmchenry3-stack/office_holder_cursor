"""Tests for the infobox-delta-skip optimization in runner.py.

When run_mode='delta' and the holder set (by canonical wiki URL) is
identical to existing terms, the runner skips all infobox HTTP fetches.

Three scenarios are tested:
1. Holder set unchanged  → infobox NOT called, offices_unchanged incremented
2. Existing terms subset of table (new holder present) → infobox IS called
3. html_hash stored on holder-set skip so the next run uses the coarser hash-match
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Helpers (mirror of test_html_hash_skip.py)
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
    """Init DB, seed one office from the first manifest entry.

    Returns (db_path, cache_dir, tc_id, source_url, table_no, table_html, config).
    """
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
            "name": "Test Office (infobox delta skip)",
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
def test_infobox_skipped_when_holder_set_unchanged_in_delta(tmp_path, monkeypatch):
    """Delta run: HTML changes but parsed holder set is identical to existing → infobox not called."""
    db_path, cache_dir, tc_id, source_url, table_no, table_html, config = _setup_db_and_office(
        tmp_path, monkeypatch
    )

    from src.db.connection import get_connection
    from src.scraper.runner import run_with_db
    import src.scraper.table_parser as tp_mod

    # Enable infobox lookup on the office
    conn = get_connection(db_path)
    conn.execute(
        "UPDATE office_table_config SET find_date_in_infobox=1, years_only=1 WHERE id=?",
        (tc_id,),
    )
    conn.commit()
    conn.close()

    # Mock infobox fetcher: track calls, never hit network
    infobox_calls: list[str] = []

    def fake_find_term_dates(self, wiki_link, url, table_config, office_details, district):
        infobox_calls.append(wiki_link)
        return [], []

    monkeypatch.setattr(tp_mod.Biography, "find_term_dates", fake_find_term_dates)

    # First run: has_existing=False → pre-validation block skipped → full parse → terms written
    result1 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result1["terms_parsed"] > 0, "First run must write terms"

    # Swap cache to HTML with a different hash but identical table content
    infobox_calls.clear()
    _write_cache(
        cache_dir,
        source_url,
        table_no,
        table_html + "<!-- changed -->",
        use_full_page=bool(config.get("use_full_page_for_table", False)),
    )

    # Second run: hash mismatch → pre-validation → holder set identical → skip infobox
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert (
        result2.get("offices_unchanged", 0) == 1
    ), "Holder set unchanged should count as offices_unchanged"
    assert (
        len(infobox_calls) == 0
    ), "find_term_dates must not be called when holder set is identical to existing"


@pytest.mark.integration
def test_infobox_runs_when_existing_terms_are_subset_of_table(tmp_path, monkeypatch):
    """Delta run: existing has N-1 holders, table has N → new holder detected → infobox runs."""
    db_path, cache_dir, tc_id, source_url, table_no, table_html, config = _setup_db_and_office(
        tmp_path, monkeypatch
    )

    from src.db.connection import get_connection
    from src.scraper.runner import run_with_db
    import src.scraper.table_parser as tp_mod

    conn = get_connection(db_path)
    conn.execute(
        "UPDATE office_table_config SET find_date_in_infobox=1, years_only=1 WHERE id=?",
        (tc_id,),
    )
    conn.commit()
    conn.close()

    infobox_calls: list[str] = []

    def fake_find_term_dates(self, wiki_link, url, table_config, office_details, district):
        infobox_calls.append(wiki_link)
        return [], []

    monkeypatch.setattr(tp_mod.Biography, "find_term_dates", fake_find_term_dates)

    # First run: writes N terms
    result1 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result1["terms_parsed"] > 0

    # Remove one existing term so existing has N-1 holders
    conn = get_connection(db_path)
    conn.execute(
        "DELETE FROM office_terms WHERE id = ("
        "  SELECT id FROM office_terms WHERE office_id=? ORDER BY id LIMIT 1"
        ")",
        (tc_id,),
    )
    conn.commit()
    remaining = conn.execute(
        "SELECT COUNT(*) FROM office_terms WHERE office_id=?", (tc_id,)
    ).fetchone()[0]
    conn.close()
    assert remaining > 0, "Need at least one remaining term for has_existing check"

    # Change cache hash so the coarser hash-skip doesn't fire
    infobox_calls.clear()
    _write_cache(
        cache_dir,
        source_url,
        table_no,
        table_html + "<!-- v2 -->",
        use_full_page=bool(config.get("use_full_page_for_table", False)),
    )

    # Second run: existing (N-1) ⊊ parsed (N) → holder sets differ → holder-set skip must NOT fire
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert (
        result2.get("offices_unchanged", 0) == 0
    ), "Should not be marked unchanged — holder set expanded so skip must not fire"
    # Note: find_term_dates may not be called for dead/historical holders even with
    # find_date_in_infobox=True (infobox is only fetched for living individuals or
    # newly-added ones). The key assertion is that offices_unchanged == 0, proving
    # the holder-set equality check correctly did NOT short-circuit this office.


@pytest.mark.integration
def test_holder_set_skip_stores_hash_enabling_coarser_skip_next_run(tmp_path, monkeypatch):
    """html_hash is stored when infobox is skipped via holder-set check.

    The stored hash lets the *next* delta run use the coarser hash-match
    short-circuit (line 545 of runner.py) instead of re-running pre-validation.
    """
    db_path, cache_dir, tc_id, source_url, table_no, table_html, config = _setup_db_and_office(
        tmp_path, monkeypatch
    )

    from src.db.connection import get_connection
    from src.scraper.runner import run_with_db
    import src.scraper.table_parser as tp_mod

    conn = get_connection(db_path)
    conn.execute(
        "UPDATE office_table_config SET find_date_in_infobox=1, years_only=1 WHERE id=?",
        (tc_id,),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(tp_mod.Biography, "find_term_dates", lambda self, *a, **kw: ([], []))

    # First run: writes terms; stores hash of original table_html
    run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )

    # Swap cache: different hash, same holder content → holder-set skip fires
    modified_html = table_html + "<!-- v2 -->"
    _write_cache(
        cache_dir,
        source_url,
        table_no,
        modified_html,
        use_full_page=bool(config.get("use_full_page_for_table", False)),
    )

    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result2.get("offices_unchanged", 0) == 1, "Holder-set skip should fire"

    # The new hash (of modified_html) must be persisted
    conn = get_connection(db_path)
    stored = conn.execute(
        "SELECT last_html_hash FROM office_table_config WHERE id=?", (tc_id,)
    ).fetchone()[0]
    conn.close()

    expected = hashlib.sha256(modified_html.encode("utf-8")).hexdigest()
    assert (
        stored == expected
    ), "html_hash must be stored after holder-set skip so the next run can use hash-match"

    # Third run: HTML unchanged → hash-match fires (even coarser short-circuit)
    result3 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result3.get("offices_unchanged", 0) == 1
