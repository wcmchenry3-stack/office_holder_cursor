"""
End-to-end integration tests for the runner pipeline.

All tests are zero-network: fixture HTML is served via a monkeypatched requests.get.
Covers:
  1. All manifest entries: DB terms match expected_json (regression guard for RunPageCache + bio-batch).
  2. Idempotency: re-run produces identical term counts.
  3. Bio-batch: run_with_db with run_bio=True stamps bio_refreshed_at.
  4. RunPageCache: two offices sharing a URL trigger only one HTTP call.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import src.scraper.wiki_fetch as _wiki_fetch_module
from tests.conftest import PROJECT_ROOT, _extract_table, _write_fixture_to_cache

FIXTURES_DIR = PROJECT_ROOT / "test_scripts" / "fixtures"
MANIFEST_PATH = PROJECT_ROOT / "test_scripts" / "manifest" / "parser_tests.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_manifest() -> list[dict]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _load_html(html_file: str) -> str:
    path = PROJECT_ROOT / html_file.replace("/", os.sep)
    return path.read_text(encoding="utf-8")


def _build_member_html_map(entries: list[dict]) -> dict[str, str]:
    """
    Build {url: html} for all _member_fixtures across the given manifest entries.

    Maps each wiki URL to its fixture HTML under test_scripts/fixtures/.
    Both the canonical wiki URL and the REST API URL are registered so that
    find_term_dates and biography_extract can find the fixture regardless of
    which URL form they request.
    """
    from src.scraper.wiki_fetch import normalize_wiki_url, wiki_url_to_rest_html_url

    result: dict[str, str] = {}
    for entry in entries:
        fixtures = (entry.get("config_json") or {}).get("_member_fixtures") or {}
        for wiki_url, basename in fixtures.items():
            path = FIXTURES_DIR / basename
            if not path.exists():
                continue
            html = path.read_text(encoding="utf-8")
            norm = normalize_wiki_url(wiki_url) or wiki_url
            result[norm] = html
            rest = wiki_url_to_rest_html_url(norm)
            if rest:
                result[rest] = html
    return result


def _make_requests_mock(member_html: dict[str, str], call_counter: dict | None = None):
    """
    Return a mock wiki_session()-compatible object whose .get() method:
    - Serves fixture HTML (200) when the URL is in member_html.
    - Returns HTTP 404 for unknown individual bio URLs (matches live behaviour for
      red-link/non-existent Wikipedia pages; expected_json already records these as 404).
    """
    from src.scraper.wiki_fetch import normalize_wiki_url

    class _FakeResp:
        def __init__(self, status_code: int, text: str = ""):
            self.status_code = status_code
            self.text = text

    def _patched_get(url, *args, **kwargs):
        if call_counter is not None:
            call_counter["n"] += 1
        norm = normalize_wiki_url(url) or url
        if norm in member_html:
            return _FakeResp(200, member_html[norm])
        if url in member_html:
            return _FakeResp(200, member_html[url])
        # Unknown URL: return 404 (mirrors Wikipedia's behaviour for red-links and
        # missing pages).  The parser handles 404 gracefully and the expected_json
        # was generated with those same 404s already incorporated.
        return _FakeResp(404)

    class _MockSession:
        get = staticmethod(_patched_get)

    return _MockSession()


def _seed_office(conn, country_id: int, entry: dict) -> tuple[int, int]:
    """Insert one office from a manifest entry. Returns (office_details_id, tc_id)."""
    from src.db import offices as db_offices

    source_url = (entry.get("source_url") or "").strip()
    config = dict(entry.get("config_json") or {})
    # Remove internal manifest keys that are not DB columns
    config.pop("_member_fixtures", None)

    office_data = {
        "country_id": country_id,
        "url": source_url,
        "name": entry.get("name", "Test Office"),
        "enabled": 1,
        **config,
    }
    office_details_id = db_offices.create_office(office_data, conn=conn)
    conn.commit()

    tc_row = conn.execute(
        "SELECT id FROM office_table_config WHERE office_details_id = ? ORDER BY id LIMIT 1",
        (office_details_id,),
    ).fetchone()
    assert tc_row, f"No office_table_config row for office_details_id={office_details_id}"
    return office_details_id, tc_row[0]


def _get_country_id(conn) -> int:
    row = conn.execute(
        "SELECT id FROM countries WHERE name = ? LIMIT 1", ("United States of America",)
    ).fetchone()
    assert row, "Seed data missing: no 'United States of America' country"
    return row[0]


# ---------------------------------------------------------------------------
# Test 1: all enabled manifest entries produce DB terms that match expected_json
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_all_manifest_entries_match_expected(db_with_cache, monkeypatch):
    """
    Run all enabled manifest entries through run_with_db and assert DB terms match expected_json.
    This is the primary regression guard for RunPageCache + the full parser pipeline.
    Zero network calls — all HTML served from fixtures.
    """
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import office_terms as db_office_terms
    from src.scraper.runner import run_with_db
    from src.scraper.wiki_fetch import normalize_wiki_url

    manifest = _load_manifest()
    enabled = [e for e in manifest if e.get("enabled")]
    assert enabled, "No enabled manifest entries"

    # Build member HTML map (for find_date_in_infobox entries)
    member_html = _build_member_html_map(enabled)

    # Patch requests.get to serve member HTML; raise for any unknown URL
    monkeypatch.setattr(_wiki_fetch_module, "_session", _make_requests_mock(member_html))

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)

        tc_ids: list[int] = []
        entry_by_tc: dict[int, dict] = {}

        for entry in enabled:
            source_url = (entry.get("source_url") or "").strip()
            html_file = entry.get("html_file") or ""
            config = dict(entry.get("config_json") or {})
            table_no = int(config.get("table_no", 1))

            full_html = _load_html(html_file)
            table_html, num_tables = _extract_table(full_html, table_no)

            # Pre-fill disk cache so the runner never requests the main page
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
            entry_by_tc[tc_id] = entry
    finally:
        conn.close()

    # Run the full pipeline
    result = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=tc_ids,
        run_office_bio=False,
    )
    assert result.get("office_count", 0) == len(
        enabled
    ), f"Expected {len(enabled)} offices processed, got {result.get('office_count')}"

    # Compare each office's DB terms to expected_json
    conn2 = get_connection(db_path)
    try:
        failures: list[str] = []
        for tc_id in tc_ids:
            entry = entry_by_tc[tc_id]
            expected = entry.get("expected_json") or []
            terms = db_office_terms.get_existing_terms_for_office(tc_id)

            # Count must match
            if len(terms) != len(expected):
                failures.append(
                    f"[{entry['name']}] term count: expected {len(expected)}, got {len(terms)}"
                )
                continue

            # Build URL set from expected (for existence check)
            exp_urls: set[str] = set()
            for row in expected:
                link = (row.get("Wiki Link") or "").strip()
                if link:
                    exp_urls.add(normalize_wiki_url(link) or link)

            # Check all DB wiki_urls appear in expected
            for term in terms:
                wiki_url = (term.get("wiki_url") or "").strip()
                norm_url = normalize_wiki_url(wiki_url) or wiki_url
                if norm_url not in exp_urls:
                    failures.append(f"[{entry['name']}] unexpected wiki_url in DB: {wiki_url!r}")

            # Build date tuple sets per URL for pairwise comparison.
            # A person may hold office multiple times (multiple rows with same wiki_url).
            # We compare the SET of (term_start, term_end) pairs per URL — order-independent.
            exp_dates_by_url: dict[str, set[tuple]] = {}
            for row in expected:
                link = (row.get("Wiki Link") or "").strip()
                if not link:
                    continue
                norm = normalize_wiki_url(link) or link
                key = (str(row.get("Term Start") or ""), str(row.get("Term End") or ""))
                exp_dates_by_url.setdefault(norm, set()).add(key)

            db_dates_by_url: dict[str, set[tuple]] = {}
            for term in terms:
                wiki_url = (term.get("wiki_url") or "").strip()
                norm = normalize_wiki_url(wiki_url) or wiki_url
                key = (str(term.get("term_start") or ""), str(term.get("term_end") or ""))
                db_dates_by_url.setdefault(norm, set()).add(key)

            for norm_url, exp_dates in exp_dates_by_url.items():
                db_dates = db_dates_by_url.get(norm_url, set())
                if exp_dates != db_dates:
                    failures.append(
                        f"[{entry['name']}] {norm_url}: date tuples mismatch\n"
                        f"  expected: {sorted(exp_dates)}\n"
                        f"  got:      {sorted(db_dates)}"
                    )
    finally:
        conn2.close()

    assert not failures, "E2E comparison failures:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# Test 2: idempotency — re-run produces identical term counts
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_runner_is_idempotent(db_with_cache, monkeypatch):
    """
    Running the same set of offices twice must produce identical term counts.
    Tests both the parser and the DB upsert logic.
    """
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import office_terms as db_office_terms
    from src.scraper.runner import run_with_db

    manifest = _load_manifest()
    enabled = [e for e in manifest if e.get("enabled")]

    member_html = _build_member_html_map(enabled)
    monkeypatch.setattr(_wiki_fetch_module, "_session", _make_requests_mock(member_html))

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        tc_ids: list[int] = []
        for entry in enabled:
            source_url = (entry.get("source_url") or "").strip()
            html_file = entry.get("html_file") or ""
            config = dict(entry.get("config_json") or {})
            table_no = int(config.get("table_no", 1))
            full_html = _load_html(html_file)
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

    kwargs = dict(
        run_mode="delta", dry_run=False, test_run=False, office_ids=tc_ids, run_office_bio=False
    )
    run_with_db(**kwargs)

    conn2 = get_connection(db_path)
    try:
        counts_run1 = {
            tc_id: len(db_office_terms.get_existing_terms_for_office(tc_id)) for tc_id in tc_ids
        }
    finally:
        conn2.close()

    # Second run: hash-skip kicks in for unchanged HTML, but counts must stay the same
    run_with_db(**kwargs)

    conn3 = get_connection(db_path)
    try:
        counts_run2 = {
            tc_id: len(db_office_terms.get_existing_terms_for_office(tc_id)) for tc_id in tc_ids
        }
    finally:
        conn3.close()

    assert counts_run1 == counts_run2, "Re-run changed term counts:\n" + "\n".join(
        f"  tc_id={k}: {counts_run1[k]} → {counts_run2[k]}"
        for k in tc_ids
        if counts_run1[k] != counts_run2[k]
    )


# ---------------------------------------------------------------------------
# Test 3: bio-batch — run_with_db stamps bio_refreshed_at
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_bio_batch_stamps_refreshed_at(db_with_cache, monkeypatch):
    """
    After an office run, living individuals are assigned bio_batch = id % 7.
    Running with run_bio=True and the correct bio_batch stamps bio_refreshed_at.
    """
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import individuals as db_individuals
    from src.scraper.runner import run_with_db

    manifest = _load_manifest()
    # Use the infobox entry (entry 1): has 21 member fixtures, find_date_in_infobox=True
    infobox_entry = next(
        e for e in manifest if e.get("enabled") and e.get("config_json", {}).get("_member_fixtures")
    )

    member_html = _build_member_html_map([infobox_entry])
    monkeypatch.setattr(_wiki_fetch_module, "_session", _make_requests_mock(member_html))

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        source_url = (infobox_entry.get("source_url") or "").strip()
        html_file = infobox_entry.get("html_file") or ""
        config = dict(infobox_entry.get("config_json") or {})
        table_no = int(config.get("table_no", 1))
        full_html = _load_html(html_file)
        table_html, num_tables = _extract_table(full_html, table_no)
        _write_fixture_to_cache(
            cache_dir,
            source_url,
            table_no,
            table_html,
            use_full_page=bool(config.get("use_full_page_for_table", False)),
            num_tables=num_tables,
        )
        _, tc_id = _seed_office(conn, country_id, infobox_entry)
    finally:
        conn.close()

    # Phase 1: run office parse + initial bio fetch (no living batch yet)
    run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=[tc_id],
        run_office_bio=True,
        run_bio=False,
    )

    # Find a bio_batch that has living individuals
    conn2 = get_connection(db_path)
    try:
        rows = conn2.execute(
            "SELECT DISTINCT bio_batch FROM individuals WHERE is_living = 1 LIMIT 1"
        ).fetchall()
        living_count = conn2.execute(
            "SELECT COUNT(*) FROM individuals WHERE is_living = 1"
        ).fetchone()[0]
    finally:
        conn2.close()

    assert living_count > 0, "No living individuals created after office parse"
    assert rows, "No bio_batch values found for living individuals"
    batch = rows[0][0]

    # Phase 2: living batch refresh — stamps bio_refreshed_at
    run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=[tc_id],
        run_office_bio=True,
        run_bio=True,
        bio_batch=batch,
    )

    conn3 = get_connection(db_path)
    try:
        refreshed_count = conn3.execute(
            "SELECT COUNT(*) FROM individuals WHERE bio_refreshed_at IS NOT NULL"
        ).fetchone()[0]
    finally:
        conn3.close()

    assert (
        refreshed_count > 0
    ), f"bio_refreshed_at not set for any individual after bio_batch={batch} run"


# ---------------------------------------------------------------------------
# Test 4: RunPageCache deduplicates HTTP fetches within a single run
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_run_cache_prevents_duplicate_http(db_with_cache, monkeypatch):
    """
    Two offices with the same source URL produce only one HTTP request within a single run.

    RunPageCache caches the full Wikipedia REST HTML in memory so that when a second
    office requests the same page (even a different table_no), no duplicate HTTP call
    is made.  Only 1 HTTP round-trip for N offices that share a URL.
    """
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import office_terms as db_office_terms
    from src.scraper.runner import run_with_db
    from src.scraper.wiki_fetch import wiki_url_to_rest_html_url

    manifest = _load_manifest()
    # Use the Commerce Secretary entry: find_date_in_infobox=False, has multiple tables
    base_entry = next(
        e
        for e in manifest
        if e.get("enabled") and not e.get("config_json", {}).get("find_date_in_infobox")
    )

    source_url = (base_entry.get("source_url") or "").strip()
    html_file = base_entry.get("html_file") or ""
    full_html = _load_html(html_file)

    # Build REST-URL → full page HTML so the mock can serve it
    rest_url = wiki_url_to_rest_html_url(source_url) or source_url
    page_html_map: dict[str, str] = {rest_url: full_html}

    call_counter: dict[str, int] = {"n": 0}
    monkeypatch.setattr(_wiki_fetch_module, "_session", _make_requests_mock(page_html_map, call_counter))

    # Create two offices pointing at the SAME URL with the SAME table config.
    # Do NOT pre-fill the disk cache — force the HTTP path on the first office so
    # RunPageCache is populated, then disk-cache serves the second office.
    config_base = dict(base_entry.get("config_json") or {})
    config_base.pop("_member_fixtures", None)

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        from src.db import offices as db_offices

        def _make_office(name: str) -> int:
            od = db_offices.create_office(
                {
                    "country_id": country_id,
                    "url": source_url,
                    "name": name,
                    "enabled": 1,
                    **config_base,
                },
                conn=conn,
            )
            conn.commit()
            return conn.execute(
                "SELECT id FROM office_table_config WHERE office_details_id = ? ORDER BY id LIMIT 1",
                (od,),
            ).fetchone()[0]

        tc1 = _make_office("RunCacheTest Office A")
        tc2 = _make_office("RunCacheTest Office B")
    finally:
        conn.close()

    run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=[tc1, tc2],
        run_office_bio=False,
    )

    conn2 = get_connection(db_path)
    try:
        terms1 = db_office_terms.get_existing_terms_for_office(tc1)
        terms2 = db_office_terms.get_existing_terms_for_office(tc2)
    finally:
        conn2.close()

    assert len(terms1) > 0, "Office A produced no terms"
    assert len(terms2) > 0, "Office B produced no terms"

    # The disk cache written by Office A serves Office B without HTTP.
    # Either RunPageCache (within the fetch) or the disk cache (written during Office A's
    # processing) avoids the duplicate HTTP call — both are the optimisation we're guarding.
    assert (
        call_counter["n"] == 1
    ), f"Expected 1 HTTP call for 2 offices sharing the same URL, got {call_counter['n']}"


# ---------------------------------------------------------------------------
# P3.3  HTTP failure-mode tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_network_timeout_skips_office_but_run_continues(db_with_cache, monkeypatch):
    """
    When wiki_session().get() raises Timeout for one office's table URL, the
    runner records the error for that office (0 terms) but completes the run
    and writes terms for other offices whose cache is pre-populated.
    """
    import requests.exceptions

    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import office_terms as db_office_terms
    from src.db import offices as db_offices
    from src.scraper.runner import run_with_db

    GOOD_URL = "https://en.wikipedia.org/wiki/TestOfficeGood"
    BAD_URL = "https://en.wikipedia.org/wiki/TestOfficeBad"
    TABLE_NO = 1

    # Minimal table HTML that the parser can process (no terms produced is fine —
    # what matters is the run doesn't crash and BAD_URL produces no terms).
    good_table_html = (
        "<table><tr><th>Name</th><th>From</th><th>To</th></tr>"
        "<tr><td><a href='/wiki/Alice'>Alice</a></td><td>2000</td><td>2004</td></tr>"
        "</table>"
    )

    # Pre-fill disk cache only for the good office.
    _write_fixture_to_cache(cache_dir, GOOD_URL, TABLE_NO, good_table_html)

    # Mock session: raises Timeout for any URL not in member_html (covers BAD_URL).
    class _TimeoutSession:
        def get(self, url, *args, **kwargs):
            raise requests.exceptions.Timeout(f"Simulated timeout for {url}")

    monkeypatch.setattr(_wiki_fetch_module, "_session", _TimeoutSession())

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)

        config_base = {
            "table_no": TABLE_NO,
            "table_rows": 3,
            "link_column": 1,
            "party_column": 0,
            "term_start_column": 2,
            "term_end_column": 3,
            "district_column": 0,
            "term_dates_merged": False,
            "party_ignore": True,
            "district_ignore": True,
            "district_at_large": False,
            "dynamic_parse": True,
            "read_right_to_left": False,
            "find_date_in_infobox": False,
            "years_only": True,
            "parse_rowspan": False,
            "consolidate_rowspan_terms": False,
            "rep_link": False,
            "party_link": False,
            "use_full_page_for_table": False,
        }

        def _make_tc(url, name):
            od = db_offices.create_office(
                {"country_id": country_id, "url": url, "name": name, "enabled": 1, **config_base},
                conn=conn,
            )
            conn.commit()
            return conn.execute(
                "SELECT id FROM office_table_config WHERE office_details_id = ? ORDER BY id LIMIT 1",
                (od,),
            ).fetchone()[0]

        good_tc = _make_tc(GOOD_URL, "Good Office")
        bad_tc = _make_tc(BAD_URL, "Bad Office")
    finally:
        conn.close()

    # Run must not raise even though BAD_URL will trigger a Timeout.
    result = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=[good_tc, bad_tc],
        run_office_bio=False,
    )

    # Run completes for both offices (office_count includes errored offices).
    assert result.get("office_count", 0) == 2

    conn2 = get_connection(db_path)
    try:
        good_terms = db_office_terms.get_existing_terms_for_office(good_tc)
        bad_terms = db_office_terms.get_existing_terms_for_office(bad_tc)
    finally:
        conn2.close()

    # Good office: cache hit → terms produced.
    assert len(good_terms) > 0, "Good office should have produced terms from cached HTML"
    # Bad office: Timeout → error recorded → no terms written.
    assert len(bad_terms) == 0, "Bad office should have 0 terms after network timeout"


@pytest.mark.integration
def test_cancel_check_stops_run_before_all_offices(db_with_cache, monkeypatch):
    """
    When cancel_check() returns True, run_with_db stops processing remaining
    offices and returns {"cancelled": True} without raising.
    """
    db_path, cache_dir = db_with_cache

    from src.db.connection import get_connection
    from src.db import offices as db_offices
    from src.scraper.runner import run_with_db

    # Build enough offices so at least one is pending when cancel fires.
    manifest = _load_manifest()
    enabled = [e for e in manifest if e.get("enabled")][:3]
    assert len(enabled) >= 2, "Need at least 2 manifest entries for cancel test"

    member_html = _build_member_html_map(enabled)
    monkeypatch.setattr(_wiki_fetch_module, "_session", _make_requests_mock(member_html))

    conn = get_connection(db_path)
    try:
        country_id = _get_country_id(conn)
        tc_ids: list[int] = []
        for entry in enabled:
            source_url = (entry.get("source_url") or "").strip()
            config = dict(entry.get("config_json") or {})
            table_no = int(config.get("table_no", 1))
            full_html = _load_html(entry["html_file"])
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

    # cancel_check fires immediately on first call.
    cancel_calls = {"n": 0}

    def _immediate_cancel():
        cancel_calls["n"] += 1
        return True

    result = run_with_db(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        office_ids=tc_ids,
        run_office_bio=False,
        cancel_check=_immediate_cancel,
    )

    assert result.get("cancelled") is True, f"Expected cancelled=True, got: {result}"
