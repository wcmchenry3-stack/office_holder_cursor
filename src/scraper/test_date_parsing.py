"""Unit tests for date parsing helpers in table_parser.py."""

from datetime import date

from src.scraper.table_parser import DataCleanup, _parse_date


class _NullLogger:
    """Minimal Logger stand-in: absorbs all calls without writing files."""

    def log(self, *a, **kw):
        pass

    def debug_log(self, *a, **kw):
        pass


# ---------------------------------------------------------------------------
# _parse_date (standalone function)
# ---------------------------------------------------------------------------


def test_parse_date_iso_format():
    assert _parse_date("2020-01-15") == date(2020, 1, 15)


def test_parse_date_none_returns_none():
    assert _parse_date(None) is None


def test_parse_date_empty_string_returns_none():
    assert _parse_date("") is None


def test_parse_date_invalid_date_string_returns_none():
    assert _parse_date("Invalid date") is None


def test_parse_date_natural_language():
    assert _parse_date("January 15, 2020") == date(2020, 1, 15)


def test_parse_date_date_object_passthrough():
    d = date(2021, 6, 1)
    assert _parse_date(d) == d


# ---------------------------------------------------------------------------
# DataCleanup.format_date
# ---------------------------------------------------------------------------


def test_format_date_iso_passthrough():
    dc = DataCleanup(_NullLogger())
    assert dc.format_date("2020-01-15") == "2020-01-15"


def test_format_date_year_only_returns_invalid():
    dc = DataCleanup(_NullLogger())
    assert dc.format_date("2020") == "Invalid date"


def test_format_date_full_month_name():
    dc = DataCleanup(_NullLogger())
    assert dc.format_date("January 15, 2020") == "2020-01-15"


def test_format_date_abbreviated_month():
    dc = DataCleanup(_NullLogger())
    assert dc.format_date("Jan 15, 2020") == "2020-01-15"


def test_format_date_dd_month_yyyy():
    dc = DataCleanup(_NullLogger())
    assert dc.format_date("15 January 2020") == "2020-01-15"


def test_format_date_historical_date():
    dc = DataCleanup(_NullLogger())
    assert dc.format_date("18 June 1798") == "1798-06-18"
