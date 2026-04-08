"""Unit tests for get_table_html_cached — focus on max_age_seconds TTL behaviour.

These tests never make real HTTP requests. They write cache files directly and
manipulate mtime to simulate stale vs fresh cache entries.
"""
from __future__ import annotations

import gzip
import json
import os
import time

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_cache_file(cache_path, table_no: int, html: str, num_tables: int = 5) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(cache_path, "wt", encoding="utf-8") as f:
        json.dump({"table_no": table_no, "num_tables": num_tables, "html": html}, f)


def _make_cache_path(tmp_path, url: str, table_no: int) -> "Path":
    import hashlib
    key = hashlib.sha256(
        (url.strip() + "|" + str(table_no) + "|0").encode("utf-8")
    ).hexdigest()[:32]
    return tmp_path / f"{key}.json.gz"


# ---------------------------------------------------------------------------
# max_age_seconds: fresh cache is served without HTTP
# ---------------------------------------------------------------------------


def test_fresh_cache_served_without_fetch(tmp_path, monkeypatch):
    """If cache file is younger than max_age_seconds, it is served without HTTP."""
    from src.scraper import table_cache

    url = "https://en.wikipedia.org/wiki/Test_Page"
    table_no = 1
    cache_path = _make_cache_path(tmp_path, url, table_no)
    _write_cache_file(cache_path, table_no, "<table>fresh</table>")

    monkeypatch.setattr(table_cache, "_cache_dir", lambda: tmp_path)

    fetched: list[str] = []

    def _fake_fetch(u, t, use_full_page=False, run_cache=None):
        fetched.append(u)
        return {"error": "should not reach HTTP"}

    monkeypatch.setattr(table_cache, "_fetch_table_from_url", _fake_fetch)

    result = table_cache.get_table_html_cached(url, table_no, max_age_seconds=3600)
    assert result.get("html") == "<table>fresh</table>"
    assert fetched == [], "HTTP fetch must not be called for a fresh cache hit"


# ---------------------------------------------------------------------------
# max_age_seconds: stale cache triggers re-fetch
# ---------------------------------------------------------------------------


def test_stale_cache_triggers_refetch(tmp_path, monkeypatch):
    """If cache file is older than max_age_seconds, a fresh HTTP fetch is made."""
    from src.scraper import table_cache

    url = "https://en.wikipedia.org/wiki/Test_Page"
    table_no = 1
    cache_path = _make_cache_path(tmp_path, url, table_no)
    _write_cache_file(cache_path, table_no, "<table>stale</table>")

    # Backdate mtime by 8 days (> 7-day TTL)
    eight_days_ago = time.time() - 8 * 24 * 3600
    os.utime(cache_path, (eight_days_ago, eight_days_ago))

    monkeypatch.setattr(table_cache, "_cache_dir", lambda: tmp_path)

    fetched: list[str] = []

    def _fake_fetch(u, t, use_full_page=False, run_cache=None):
        fetched.append(u)
        return {"table_no": t, "num_tables": 5, "html": "<table>fresh</table>"}

    monkeypatch.setattr(table_cache, "_fetch_table_from_url", _fake_fetch)

    result = table_cache.get_table_html_cached(url, table_no, max_age_seconds=7 * 24 * 3600)
    assert result.get("html") == "<table>fresh</table>", "Stale cache must be replaced by fresh fetch"
    assert len(fetched) == 1, "Exactly one HTTP fetch must have been made"


def test_no_max_age_ignores_file_age(tmp_path, monkeypatch):
    """Without max_age_seconds, cache is served regardless of how old it is."""
    from src.scraper import table_cache

    url = "https://en.wikipedia.org/wiki/Test_Page"
    table_no = 1
    cache_path = _make_cache_path(tmp_path, url, table_no)
    _write_cache_file(cache_path, table_no, "<table>ancient</table>")

    # Backdate mtime by 30 days
    thirty_days_ago = time.time() - 30 * 24 * 3600
    os.utime(cache_path, (thirty_days_ago, thirty_days_ago))

    monkeypatch.setattr(table_cache, "_cache_dir", lambda: tmp_path)

    fetched: list[str] = []

    def _fake_fetch(u, t, use_full_page=False, run_cache=None):
        fetched.append(u)
        return {"error": "should not reach HTTP"}

    monkeypatch.setattr(table_cache, "_fetch_table_from_url", _fake_fetch)

    result = table_cache.get_table_html_cached(url, table_no, max_age_seconds=None)
    assert result.get("html") == "<table>ancient</table>"
    assert fetched == [], "Without max_age_seconds, even ancient cache must be served"


def test_refresh_true_bypasses_max_age(tmp_path, monkeypatch):
    """refresh=True always fetches fresh even if cache is young."""
    from src.scraper import table_cache

    url = "https://en.wikipedia.org/wiki/Test_Page"
    table_no = 1
    cache_path = _make_cache_path(tmp_path, url, table_no)
    _write_cache_file(cache_path, table_no, "<table>cached</table>")

    monkeypatch.setattr(table_cache, "_cache_dir", lambda: tmp_path)

    fetched: list[str] = []

    def _fake_fetch(u, t, use_full_page=False, run_cache=None):
        fetched.append(u)
        return {"table_no": t, "num_tables": 5, "html": "<table>fresh</table>"}

    monkeypatch.setattr(table_cache, "_fetch_table_from_url", _fake_fetch)

    result = table_cache.get_table_html_cached(
        url, table_no, refresh=True, max_age_seconds=3600
    )
    assert result.get("html") == "<table>fresh</table>"
    assert len(fetched) == 1, "refresh=True must bypass max_age_seconds check"


# ---------------------------------------------------------------------------
# Delta run wires 7-day TTL
# ---------------------------------------------------------------------------


def test_delta_run_passes_7day_ttl(monkeypatch):
    """_process_one_office passes max_age_seconds=7d to get_table_html_cached during delta."""
    import src.scraper.runner as runner

    received_max_age: list = []

    def _capture_cache(url, table_no, *, refresh=False, use_full_page=False,
                       run_cache=None, max_age_seconds=None):
        received_max_age.append(max_age_seconds)
        return {"error": "abort early"}  # short-circuit the rest of processing

    monkeypatch.setattr(runner, "get_table_html_cached", _capture_cache)

    from src.scraper.runner import _RunConfig, _process_single_office

    cfg = _RunConfig(
        run_mode="delta",
        dry_run=False,
        test_run=False,
        force_overwrite=False,
        force_replace_office_ids=None,
        refresh_table_cache=False,
        max_rows_per_table=None,
        party_list=[],
        offices_parser=None,
        run_cache=None,
        cancel_check=None,
        report=lambda *a, **kw: None,
    )

    office_row = {
        "office_table_config_id": 99,
        "id": 99,
        "name": "Test Office",
        "url": "https://en.wikipedia.org/wiki/Test",
        "table_no": 1,
        "use_full_page_for_table": 0,
        "find_date_in_infobox": 0,
        "years_only": 0,
        "last_html_hash": None,
        "office_details_id": 99,
    }

    _process_single_office(office_row, cfg, office_index=1, office_total=1)

    assert received_max_age, "get_table_html_cached was not called"
    assert received_max_age[0] == 7 * 24 * 3600, (
        f"Expected 7-day TTL (604800s), got {received_max_age[0]}"
    )
