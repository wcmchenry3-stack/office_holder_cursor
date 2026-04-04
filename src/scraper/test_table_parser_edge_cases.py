"""Pure unit tests for scraper utility functions.

No DB, no network, no fixtures required.

Run: pytest src/scraper/test_table_parser_edge_cases.py -v
"""

from __future__ import annotations

import pytest

from src.scraper.table_parser import (
    Biography,
    DataCleanup,
    Offices,
    parse_infobox_role_key_query,
)
from src.db.bulk_import import _bool_from_cell, _int_from_cell
from src.scheduled_tasks import _format_duration


class _NullLogger:
    def log(self, *a, **kw):
        pass

    def debug_log(self, *a, **kw):
        pass


def _offices():
    logger = _NullLogger()
    dc = DataCleanup(logger)
    bio = Biography(logger, dc)
    return Offices(logger, bio, dc)


# ---------------------------------------------------------------------------
# parse_infobox_role_key_query
# ---------------------------------------------------------------------------


def test_parse_infobox_role_key_query_empty_string():
    """Empty input returns empty includes and excludes."""
    includes, excludes = parse_infobox_role_key_query("")
    assert includes == []
    assert excludes == []


def test_parse_infobox_role_key_query_includes_and_excludes():
    """Quoted includes and excludes are parsed correctly."""
    includes, excludes = parse_infobox_role_key_query('"judge" "associate justice" -"chief judge"')
    assert "judge" in includes
    assert "associate justice" in includes
    assert "chief judge" in excludes


# ---------------------------------------------------------------------------
# _bool_from_cell
# ---------------------------------------------------------------------------


def test_bool_from_cell_variants():
    """_bool_from_cell converts CSV TRUE/FALSE/yes/1 values correctly."""
    assert _bool_from_cell("TRUE") == 1
    assert _bool_from_cell("true") == 1
    assert _bool_from_cell("yes") == 1
    assert _bool_from_cell("YES") == 1
    assert _bool_from_cell("1") == 1
    assert _bool_from_cell("FALSE") == 0
    assert _bool_from_cell("false") == 0
    assert _bool_from_cell("") == 0
    assert _bool_from_cell(None) == 0
    assert _bool_from_cell("no") == 0


# ---------------------------------------------------------------------------
# _int_from_cell
# ---------------------------------------------------------------------------


def test_int_from_cell_with_bad_values():
    """_int_from_cell returns default on non-integer input."""
    assert _int_from_cell("abc", default=99) == 99
    assert _int_from_cell(None, default=3) == 3
    assert _int_from_cell("", default=7) == 7
    assert _int_from_cell("5") == 5
    assert _int_from_cell("0") == 0


# ---------------------------------------------------------------------------
# _format_duration
# ---------------------------------------------------------------------------


def test_format_duration_minutes_and_seconds():
    """_format_duration formats seconds into human-readable duration."""
    assert _format_duration(90) == "1m 30s"
    assert _format_duration(45) == "45s"
    assert _format_duration(0) == "0s"
    assert _format_duration(60) == "1m 0s"
    assert _format_duration(3661) == "61m 1s"


# ---------------------------------------------------------------------------
# Offices._is_valid_wiki_link — congressional district pattern (bug #212)
# ---------------------------------------------------------------------------
# Note: URL strings below are test inputs only. No HTTP requests are made.
# Actual Wikipedia fetches use the Wikimedia REST API with User-Agent and
# rate limiting / retry/backoff logic in wiki_fetch.py (wiki_session).


WIKI_BASE = "https://en.wikipedia.org"


def test_is_valid_wiki_link_congressional_district_possessive_returns_false():
    """Congressional district URLs with possessive state names are ignored.

    Previously [\\w%]+ failed to match apostrophes so Mississippi's, Virginia's
    etc. slipped through and were stored as individual wiki_urls. Fixed by
    replacing with [^/]* which handles apostrophes and other characters.
    """
    offices = _offices()
    possessive_cases = [
        f"{WIKI_BASE}/wiki/Mississippi's_4th_congressional_district",
        f"{WIKI_BASE}/wiki/Virginia's_7th_congressional_district",
        f"{WIKI_BASE}/wiki/Georgia's_5th_congressional_district",
    ]
    for url in possessive_cases:
        assert offices._is_valid_wiki_link(url) is False, f"Should be ignored: {url}"


def test_is_valid_wiki_link_at_large_congressional_district_returns_false():
    """At-large congressional district URLs are ignored (no digit+suffix prefix)."""
    offices = _offices()
    cases = [
        f"{WIKI_BASE}/wiki/Mississippi's_at-large_congressional_district",
        f"{WIKI_BASE}/wiki/Wyoming's_at-large_congressional_district",
    ]
    for url in cases:
        assert offices._is_valid_wiki_link(url) is False, f"Should be ignored: {url}"


def test_is_valid_wiki_link_congressional_district_no_possessive_returns_false():
    """Congressional district URLs without possessives are still ignored."""
    offices = _offices()
    cases = [
        f"{WIKI_BASE}/wiki/Tennessee_2nd_congressional_district",
        f"{WIKI_BASE}/wiki/North_Carolina_1st_congressional_district",
    ]
    for url in cases:
        assert offices._is_valid_wiki_link(url) is False, f"Should be ignored: {url}"


def test_is_valid_wiki_link_real_person_not_filtered():
    """Real person pages are not accidentally caught by the district pattern."""
    offices = _offices()
    cases = [
        f"{WIKI_BASE}/wiki/Thomas_G._Abernethy",
        f"{WIKI_BASE}/wiki/John_Lewis_(Georgia_politician)",
    ]
    for url in cases:
        assert offices._is_valid_wiki_link(url) is True, f"Should be allowed: {url}"
