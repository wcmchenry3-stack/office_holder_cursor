"""Pure unit tests for scraper utility functions.

No DB, no network, no fixtures required.

Run: pytest src/scraper/test_table_parser_edge_cases.py -v
"""

from __future__ import annotations

import pytest

from src.scraper.table_parser import parse_infobox_role_key_query
from src.db.bulk_import import _bool_from_cell, _int_from_cell
from src.scheduled_tasks import _format_duration


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
