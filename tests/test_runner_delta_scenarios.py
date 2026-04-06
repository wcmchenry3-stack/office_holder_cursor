"""
Runner scenario tests using minimal fake Wikipedia fixtures.

Covers the three runner behaviors relevant to the daily delta failure (PR #331):
  A. scheduled_job_runs create/finish round-trip (regression guard for RETURNING id fix)
  B. Delta runner: first run, hash-skip, new-holder detection
  C. Auto table update: runner detects table number shift and persists new table_no
  D. Live person runner: only re-scrapes individuals marked is_living=1

Note: wikipedia.org URL strings below are test input values only.
No HTTP requests to Wikipedia are made here.
All actual Wikipedia HTTP requests go through wiki_fetch.py (wiki_session)
which sets the required User-Agent header and enforces rate limiting / retry/backoff logic.

All tests are zero-network: wiki cache is pre-filled from test_scripts/fixtures/simple/.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.conftest import PROJECT_ROOT, _extract_table, _write_fixture_to_cache

SIMPLE_FIXTURES = PROJECT_ROOT / "test_scripts" / "fixtures" / "simple"

FAKE_URL = "https://en.wikipedia.org/wiki/List_of_Fake_Office_Holders"

# Config matching the column layout of the fake wikitable fixtures.
# DB uses 1-based column numbers (0 = "no column" for optional cols like district).
# Fake table layout: col1=Name/Link, col2=Party, col3=Term Start, col4=Term End
FAKE_CONFIG = {
    "table_no": 2,
    "link_column": 1,  # 1-based: first column
    "party_column": 2,  # 1-based: second column
    "term_start_column": 3,  # 1-based: third column
    "term_end_column": 4,  # 1-based: fourth column
    "years_only": True,
    "find_date_in_infobox": False,
    "district_column": 0,  # 0 = no district column
    "district_ignore": True,
    "party_ignore": False,
}

LIVING_CONFIG = {
    **FAKE_CONFIG,
    "table_no": 1,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_fixture(filename: str) -> str:
    path = SIMPLE_FIXTURES / filename
    assert path.exists(), f"Fixture missing: {path}"
    return path.read_text(encoding="utf-8")


def _fill_cache_both_tables(cache_dir, url, full_html, *, num_tables: int = 2) -> tuple[str, str]:
    """Pre-fill cache entries for both table 1 and table 2 from full-page HTML.

    Returns (table1_html, table2_html).
    """
    t1_html, _ = _extract_table(full_html, 1)
    t2_html, _ = _extract_table(full_html, 2)
    _write_fixture_to_cache(cache_dir, url, 1, t1_html, num_tables=num_tables)
    _write_fixture_to_cache(cache_dir, url, 2, t2_html, num_tables=num_tables)
    return t1_html, t2_html


def _seed_office(conn, country_id, config, url, name):
    """Insert an office and its table config. Returns tc_id (office_table_config.id)."""
    from src.db import offices as db_offices

    office_data = {
        "country_id": country_id,
        "url": url,
        "name": name,
        "enabled": 1,
        **config,
    }
    office_details_id = db_offices.create_office(office_data, conn=conn)
    conn.commit()

    tc_row = conn.execute(
        "SELECT id FROM office_table_config WHERE office_details_id = ? ORDER BY id LIMIT 1",
        (office_details_id,),
    ).fetchone()
    assert tc_row, f"No office_table_config row for office '{name}'"
    return tc_row[0]


def _get_country_id(conn):
    row = conn.execute(
        "SELECT id FROM countries WHERE name = ? LIMIT 1",
        ("United States of America",),
    ).fetchone()
    assert row, "Seed data missing: United States country"
    return row[0]


# ---------------------------------------------------------------------------
# Group A — scheduled_job_runs round-trip (PR #331 regression guard)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_create_run_returns_valid_id(db_with_cache):
    """create_run() returns a positive int; finish_run() completes without error."""
    from src.db.scheduled_job_runs import create_run, finish_run, get_last_run_for_job

    run_id = create_run("daily_delta")
    assert (
        isinstance(run_id, int) and run_id > 0
    ), f"create_run() should return a positive int, got {run_id!r}"

    finish_run(run_id, "complete", result={"office_count": 1})

    record = get_last_run_for_job("daily_delta")
    assert record is not None
    assert record["status"] == "complete"
    assert record["id"] == run_id


# ---------------------------------------------------------------------------
# Group B — Delta runner
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_delta_first_run_inserts_terms(db_with_cache):
    """First delta run with baseline fixture inserts 3 office terms."""
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import office_terms as db_office_terms
    from src.scraper.runner import run_with_db

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        tc_id = _seed_office(conn, country_id, FAKE_CONFIG, FAKE_URL, "Fake Office (delta first)")
    finally:
        conn.close()

    full_html = _load_fixture("fake_office_baseline.html")
    _fill_cache_both_tables(cache_dir, FAKE_URL, full_html)

    result = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )

    assert result.get("office_count", 0) >= 1, "Runner processed no offices"
    terms = db_office_terms.get_existing_terms_for_office(tc_id)
    assert len(terms) == 3, f"Expected 3 terms from baseline fixture, got {len(terms)}"


@pytest.mark.integration
def test_delta_unchanged_html_skips(db_with_cache):
    """Second delta run with identical cached HTML is skipped via html_hash comparison."""
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.scraper.runner import run_with_db

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        tc_id = _seed_office(conn, country_id, FAKE_CONFIG, FAKE_URL, "Fake Office (hash skip)")
    finally:
        conn.close()

    full_html = _load_fixture("fake_office_baseline.html")
    _fill_cache_both_tables(cache_dir, FAKE_URL, full_html)

    # First run: stores html_hash, inserts terms
    result1 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert result1["terms_parsed"] > 0, "First run should insert terms"
    assert result1.get("offices_unchanged", 0) == 0

    # Second run: same cache → hash match → skip
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert (
        result2.get("offices_unchanged", 0) == 1
    ), "Second run with unchanged HTML should report offices_unchanged=1"
    assert result2["terms_parsed"] == 0, "No new terms should be written on hash-skip run"


@pytest.mark.integration
def test_delta_new_holder_detected(db_with_cache):
    """After initial seed, swapping cache to v2 fixture causes delta to insert the new term."""
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import office_terms as db_office_terms
    from src.scraper.runner import run_with_db

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        tc_id = _seed_office(conn, country_id, FAKE_CONFIG, FAKE_URL, "Fake Office (new holder)")
    finally:
        conn.close()

    # Seed with 3-holder baseline
    baseline_html = _load_fixture("fake_office_baseline.html")
    _fill_cache_both_tables(cache_dir, FAKE_URL, baseline_html)
    run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert len(db_office_terms.get_existing_terms_for_office(tc_id)) == 3

    # Swap cache to v2 (4 holders: adds Dave)
    newterm_html = _load_fixture("fake_office_newterm.html")
    _fill_cache_both_tables(cache_dir, FAKE_URL, newterm_html)

    # No refresh_table_cache needed: the overwritten cache has different HTML so the
    # delta hash-skip is bypassed automatically (stored hash no longer matches).
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )

    terms = db_office_terms.get_existing_terms_for_office(tc_id)
    assert (
        len(terms) == 4
    ), f"Expected 4 terms after adding Dave, got {len(terms)}; result={result2}"


# ---------------------------------------------------------------------------
# Group C — Auto table update
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_auto_table_update_detects_shift(db_with_cache):
    """Runner detects table shift (data moves from table 2 → table 1) and persists new table_no."""
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import office_terms as db_office_terms
    from src.scraper.runner import run_with_db

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        # Office starts configured at table_no=2 (correct for baseline page)
        tc_id = _seed_office(conn, country_id, FAKE_CONFIG, FAKE_URL, "Fake Office (auto-update)")
    finally:
        conn.close()

    # Seed cache with baseline (table 2 = 3 holders)
    baseline_html = _load_fixture("fake_office_baseline.html")
    _fill_cache_both_tables(cache_dir, FAKE_URL, baseline_html)

    # First delta run: inserts 3 terms, stores html_hash
    result1 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )
    assert (
        result1["terms_parsed"] == 3
    ), f"Expected 3 terms on first run, got {result1['terms_parsed']}"

    # Swap cache to shifted page (same 3 holders now at table 1; table 2 is stub)
    shifted_html = _load_fixture("fake_office_shifted.html")
    _fill_cache_both_tables(cache_dir, FAKE_URL, shifted_html)

    # Second run: table_no=2 now has a partial wrong row — hash differs from baseline so
    # delta hash-skip is bypassed automatically; auto-update then finds table 1 as better match.
    result2 = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )

    # All 3 terms should still exist (auto-update found them at table 1)
    terms = db_office_terms.get_existing_terms_for_office(tc_id)
    assert len(terms) == 3, f"Terms should still be 3 after table shift, got {len(terms)}"

    # The office_table_config.table_no should now be 1
    conn2 = get_connection(db_path)
    try:
        row = conn2.execute(
            "SELECT table_no FROM office_table_config WHERE id = ?", (tc_id,)
        ).fetchone()
    finally:
        conn2.close()
    assert (
        row is not None and row[0] == 1
    ), f"Expected table_no updated to 1, got {row[0] if row else 'no row'}"


# ---------------------------------------------------------------------------
# Group D — Live person runner
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_live_person_only_fetches_living(db_with_cache, monkeypatch):
    """live_person mode calls get_living_individual_wiki_urls() and only scrapes those individuals."""
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.scraper.runner import run_with_db

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        tc_id = _seed_office(
            conn, country_id, LIVING_CONFIG, FAKE_URL + "_living", "Fake Living Office"
        )
    finally:
        conn.close()

    # Pre-fill cache with the living office fixture (table_no=1, single table)
    living_html = _load_fixture("fake_living_office.html")
    t1_html, _ = _extract_table(living_html, 1)
    _write_fixture_to_cache(cache_dir, FAKE_URL + "_living", 1, t1_html, num_tables=1)

    # First delta run: inserts Eve, Frank (no term_end = living) and Grace (term_end 2019)
    run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        run_office_bio=False,
        office_ids=[tc_id],
    )

    # Mark Grace as not living so live_person skips her
    conn2 = get_connection(db_path)
    try:
        conn2.execute("UPDATE individuals SET is_living = 0 WHERE wiki_url LIKE '%Grace_Former%'")
        conn2.commit()
        living_urls = conn2.execute(
            "SELECT wiki_url FROM individuals WHERE is_living = 1"
        ).fetchall()
    finally:
        conn2.close()

    # Should be 2 living individuals (Eve and Frank)
    assert (
        len(living_urls) == 2
    ), f"Expected 2 living individuals before live_person run, got {len(living_urls)}"

    # Monkeypatch requests.get so bio fetches return fake_bio_living.html instead of hitting network
    bio_html = _load_fixture("fake_bio_living.html")

    class _FakeResponse:
        status_code = 200
        text = bio_html

        def raise_for_status(self):
            pass

    def _fake_get(url, *args, **kwargs):
        return _FakeResponse()

    import requests

    monkeypatch.setattr(requests, "get", _fake_get)

    # Also patch wiki_fetch module if it has its own session
    try:
        import src.scraper.wiki_fetch as wiki_fetch_mod

        if hasattr(wiki_fetch_mod, "wiki_session"):
            monkeypatch.setattr(
                wiki_fetch_mod.wiki_session, "get", lambda *a, **kw: _FakeResponse()
            )
    except Exception:
        pass

    result = run_with_db(
        run_mode="live_person",
        run_bio=True,
        run_office_bio=True,
        office_ids=[tc_id],
        dry_run=False,
        test_run=False,
    )

    # Eve and Frank should each get a successful bio update; Grace is excluded
    assert (
        result.get("living_success_count", 0) + result.get("living_error_count", 0) == 2
    ), f"Expected 2 living bio attempts (Eve + Frank), got result={result}"
