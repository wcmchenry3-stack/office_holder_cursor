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

    def _fake_fetch(u, t, use_full_page=False, run_cache=None,
                    if_none_match=None, if_modified_since=None):
        fetched.append(u)
        return {"error": "should not reach HTTP"}

    monkeypatch.setattr(table_cache, "_fetch_table_from_url", _fake_fetch)

    result = table_cache.get_table_html_cached(url, table_no, max_age_seconds=3600)
    assert result.get("html") == "<table>fresh</table>"
    assert fetched == [], "HTTP fetch must not be called for a fresh cache hit"


# ---------------------------------------------------------------------------
# max_age_seconds: stale cache triggers re-fetch
# ---------------------------------------------------------------------------


def test_stale_cache_triggers_conditional_get(tmp_path, monkeypatch):
    """Stale cache sends a conditional GET; 200 response replaces cache with fresh HTML."""
    from src.scraper import table_cache

    url = "https://en.wikipedia.org/wiki/Test_Page"
    table_no = 1
    cache_path = _make_cache_path(tmp_path, url, table_no)
    _write_cache_file(cache_path, table_no, "<table>stale</table>", num_tables=5)

    # Backdate mtime by 8 days (> 7-day TTL)
    eight_days_ago = time.time() - 8 * 24 * 3600
    os.utime(cache_path, (eight_days_ago, eight_days_ago))

    monkeypatch.setattr(table_cache, "_cache_dir", lambda: tmp_path)

    calls: list[dict] = []

    def _fake_fetch(u, t, use_full_page=False, run_cache=None,
                    if_none_match=None, if_modified_since=None):
        calls.append({"if_none_match": if_none_match, "if_modified_since": if_modified_since})
        return {"table_no": t, "num_tables": 5, "html": "<table>fresh</table>"}

    monkeypatch.setattr(table_cache, "_fetch_table_from_url", _fake_fetch)

    result = table_cache.get_table_html_cached(url, table_no, max_age_seconds=7 * 24 * 3600)
    assert result.get("html") == "<table>fresh</table>", "200 response must replace stale cache"
    assert len(calls) == 1, "Exactly one conditional GET must have been made"


def test_stale_cache_304_resets_ttl(tmp_path, monkeypatch):
    """304 Not Modified: cached HTML is reused and cache mtime is touched to reset the TTL."""
    from src.scraper import table_cache

    url = "https://en.wikipedia.org/wiki/Test_Page"
    table_no = 1
    cache_path = _make_cache_path(tmp_path, url, table_no)
    _write_cache_file(
        cache_path, table_no, "<table>cached</table>", num_tables=5
    )
    # Add a stored ETag so the conditional GET can send it
    import gzip, json
    with gzip.open(cache_path, "rt", encoding="utf-8") as f:
        data = json.load(f)
    data["etag"] = '"abc123"'
    with gzip.open(cache_path, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    eight_days_ago = time.time() - 8 * 24 * 3600
    os.utime(cache_path, (eight_days_ago, eight_days_ago))

    monkeypatch.setattr(table_cache, "_cache_dir", lambda: tmp_path)

    sent_etags: list[str | None] = []

    def _fake_fetch(u, t, use_full_page=False, run_cache=None,
                    if_none_match=None, if_modified_since=None):
        sent_etags.append(if_none_match)
        return {"not_modified": True}  # Wikipedia says page unchanged

    monkeypatch.setattr(table_cache, "_fetch_table_from_url", _fake_fetch)

    result = table_cache.get_table_html_cached(url, table_no, max_age_seconds=7 * 24 * 3600)
    assert result.get("html") == "<table>cached</table>", "304 must return cached HTML"
    assert sent_etags == ['"abc123"'], "ETag must have been sent in the conditional GET"
    # mtime should be refreshed (within the last 5 seconds)
    assert time.time() - cache_path.stat().st_mtime < 5, "Cache mtime must be touched after 304"


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

    def _fake_fetch(u, t, use_full_page=False, run_cache=None,
                    if_none_match=None, if_modified_since=None):
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

    def _fake_fetch(u, t, use_full_page=False, run_cache=None,
                    if_none_match=None, if_modified_since=None):
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


def test_delta_run_cache_batch_routing(monkeypatch):
    """Delta run passes 1-day max_age for today's batch office, None for other batches."""
    import datetime
    import src.scraper.runner as runner

    received_max_age: list = []

    def _capture_cache(url, table_no, *, refresh=False, use_full_page=False,
                       run_cache=None, max_age_seconds=None):
        received_max_age.append(max_age_seconds)
        return {"error": "abort early"}

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

    today_batch = datetime.date.today().weekday()
    other_batch = (today_batch + 1) % 7

    def _make_office(batch):
        return {
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
            "cache_batch": batch,
        }

    # Today's batch office: must get 1-day TTL (triggers conditional GET)
    received_max_age.clear()
    _process_single_office(_make_office(today_batch), cfg, office_index=1, office_total=1)
    assert received_max_age, "get_table_html_cached was not called"
    assert received_max_age[0] == 24 * 3600, (
        f"Today's batch must get 1-day TTL, got {received_max_age[0]}"
    )

    # Different batch office: must get None (use cache as-is)
    received_max_age.clear()
    _process_single_office(_make_office(other_batch), cfg, office_index=1, office_total=1)
    assert received_max_age, "get_table_html_cached was not called"
    assert received_max_age[0] is None, (
        f"Other batch must get None (no TTL), got {received_max_age[0]}"
    )
