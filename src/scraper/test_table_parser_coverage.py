"""Coverage-gap tests for table_parser.py.

Targets branches not exercised by the existing test suite:
  - _parse_date: datetime instance and parse-failure paths
  - _dates_from_cell_data_sort_value: None cell, single value, multiple values
  - parse_infobox_role_key_query: error conditions
  - DataCleanup.parse_year_range: delimiter variants and no-year-found
  - Biography.biography_extract: cache hit, 200 response, non-200, exception
  - Biography.parse_first_paragraph: HTML paragraph parsing
  - Biography.parse_infobox: Born/Died rows
  - Offices.extract_party: text-based matching, no column
  - Offices.extract_district: ordinal, at-large, territory, out-of-bounds

Run: pytest src/scraper/test_table_parser_coverage.py -v
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from bs4 import BeautifulSoup

from unittest.mock import patch

from src.scraper.table_parser import (
    Biography,
    DataCleanup,
    Offices,
    _dates_from_cell_data_sort_value,
    _emit_merged_run,
    _parse_date,
    parse_infobox_role_key_query,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


def _dc():
    return DataCleanup()


def _bio():
    return Biography(DataCleanup())


def _offices():
    dc = DataCleanup()
    bio = Biography(dc)
    return Offices(bio, dc)


def _cell(text: str = "", href: str | None = None) -> BeautifulSoup:
    """Build a minimal <td> BeautifulSoup tag."""
    if href:
        html = f'<td><a href="{href}">{text}</a></td>'
    else:
        html = f"<td>{text}</td>"
    return BeautifulSoup(html, "html.parser").find("td")


# ---------------------------------------------------------------------------
# _parse_date — datetime instance and exception paths
# ---------------------------------------------------------------------------


def test_parse_date_datetime_instance_returned_as_is():
    """A datetime is a subclass of date, so _parse_date returns it via the isinstance(s, date) branch."""
    dt = datetime(2021, 6, 15, 12, 30)
    result = _parse_date(dt)
    # datetime IS-A date: the isinstance(s, date) guard fires first, returns the datetime as-is
    assert result == dt


def test_parse_date_unparseable_string_returns_none():
    """A string that dateutil cannot parse should return None."""
    result = _parse_date("not a date at all !!!")
    assert result is None


# ---------------------------------------------------------------------------
# _dates_from_cell_data_sort_value
# ---------------------------------------------------------------------------


def test_dates_from_cell_data_sort_value_none_returns_none():
    """None cell → (None, None)."""
    assert _dates_from_cell_data_sort_value(None) == (None, None)


def _sort_cell(vals: list[str]) -> BeautifulSoup:
    """Build a <td> containing spans with data-sort-value attributes."""
    spans = "".join(f'<span data-sort-value="{v}">x</span>' for v in vals)
    html = f"<td>{spans}</td>"
    return BeautifulSoup(html, "html.parser").find("td")


def test_dates_from_cell_data_sort_value_single_date():
    """One data-sort-value → (val, val) — same start and end."""
    cell = _sort_cell(["000000001966-01-06"])
    start, end = _dates_from_cell_data_sort_value(cell)
    assert start == "1966-01-06"
    assert end == "1966-01-06"


def test_dates_from_cell_data_sort_value_two_dates():
    """Two data-sort-value entries → (first, last)."""
    cell = _sort_cell(["000000001966-01-06", "000000001974-12-31"])
    start, end = _dates_from_cell_data_sort_value(cell)
    assert start == "1966-01-06"
    assert end == "1974-12-31"


def test_dates_from_cell_data_sort_value_no_dates_in_cell():
    """Cell with no data-sort-value attrs → (None, None)."""
    cell = BeautifulSoup("<td>No dates here</td>", "html.parser").find("td")
    assert _dates_from_cell_data_sort_value(cell) == (None, None)


# ---------------------------------------------------------------------------
# parse_infobox_role_key_query — error conditions
# ---------------------------------------------------------------------------


def test_parse_infobox_role_key_query_trailing_dash_raises():
    """A query ending with a bare '-' must raise ValueError."""
    with pytest.raises(ValueError, match="trailing"):
        parse_infobox_role_key_query("-")


def test_parse_infobox_role_key_query_unquoted_exclude_raises():
    """An unquoted exclude token must raise ValueError."""
    with pytest.raises(ValueError, match="quoted"):
        parse_infobox_role_key_query("-chief")


def test_parse_infobox_role_key_query_empty_quoted_term_raises():
    """An empty quoted term must raise ValueError."""
    with pytest.raises(ValueError, match="empty"):
        parse_infobox_role_key_query('""')


# ---------------------------------------------------------------------------
# DataCleanup.parse_year_range
# ---------------------------------------------------------------------------


def test_parse_year_range_no_year_returns_none_none():
    dc = _dc()
    assert dc.parse_year_range("no years here") == (None, None)


def test_parse_year_range_em_dash_delimiter():
    dc = _dc()
    start, end = dc.parse_year_range("1966\u20131974")
    assert start == 1966
    assert end == 1974


def test_parse_year_range_hyphen_delimiter():
    dc = _dc()
    start, end = dc.parse_year_range("1966-1974")
    assert start == 1966
    assert end == 1974


def test_parse_year_range_to_delimiter_present():
    """'1966 to present' → (1966, None)."""
    dc = _dc()
    start, end = dc.parse_year_range("1966 to present")
    assert start == 1966
    assert end is None


def test_parse_year_range_single_year_no_delimiter():
    """Single year with no delimiter → (year, year)."""
    dc = _dc()
    start, end = dc.parse_year_range("2001")
    assert start == 2001
    assert end == 2001


def test_parse_year_range_none_input():
    dc = _dc()
    assert dc.parse_year_range(None) == (None, None)


# ---------------------------------------------------------------------------
# Biography.biography_extract — mocked HTTP
# ---------------------------------------------------------------------------

_INFOBOX_HTML = """
<html><body>
<table class="infobox vcard">
  <tr><th class="infobox-above">Jane Doe</th></tr>
  <tr><th>Born</th><td>January 1, 1950</td></tr>
</table>
</body></html>
"""

_PARAGRAPH_HTML = """
<html><body>
<p><b>Jane Doe</b> (born January 1, 1950) is a politician.</p>
</body></html>
"""


def test_biography_extract_cache_hit_skips_http(monkeypatch):
    """When run_cache returns HTML, wiki_session().get must not be called."""
    import src.scraper.wiki_fetch as _wf

    called = []

    class _BadSession:
        def get(self, *a, **kw):
            called.append(True)
            raise AssertionError("wiki_session().get should not be called on cache hit")

    monkeypatch.setattr(_wf, "_session", _BadSession())

    class _Cache:
        def get(self, key):
            return _INFOBOX_HTML

        def set(self, key, val):
            pass

    bio = _bio()
    result = bio.biography_extract("https://en.example.org/wiki/Jane_Doe", run_cache=_Cache())
    assert not called
    assert result  # should have details
    assert result.get("full_name") or result.get("name") or result.get("page_path")


def test_biography_extract_200_response_no_cache(monkeypatch):
    """Cache miss: wiki_session().get called, 200 response → parses infobox."""
    import src.scraper.wiki_fetch as _wf

    class _Cache:
        stored = {}

        def get(self, key):
            return None

        def set(self, key, val):
            _Cache.stored[key] = val

    class _MockSession:
        def get(self, *a, **kw):
            return _Resp(200, _INFOBOX_HTML)

    monkeypatch.setattr(_wf, "_session", _MockSession())

    bio = _bio()
    result = bio.biography_extract("https://en.example.org/wiki/Jane_Doe", run_cache=_Cache())
    assert result
    assert _Cache.stored  # verify run_cache.set was called


def test_biography_extract_non_200_returns_empty(monkeypatch):
    """Non-200 HTTP status → returns {}."""
    import src.scraper.wiki_fetch as _wf

    class _MockSession:
        def get(self, *a, **kw):
            return _Resp(404)

    monkeypatch.setattr(_wf, "_session", _MockSession())
    bio = _bio()
    result = bio.biography_extract("https://en.example.org/wiki/Jane_Doe")
    assert result == {}


def test_biography_extract_request_exception_returns_empty(monkeypatch):
    """Network error (RequestException) → returns {}."""
    from requests.exceptions import RequestException
    import src.scraper.wiki_fetch as _wf

    class _MockSession:
        def get(self, *a, **kw):
            raise RequestException("timeout")

    monkeypatch.setattr(_wf, "_session", _MockSession())
    bio = _bio()
    result = bio.biography_extract("https://en.example.org/wiki/Jane_Doe")
    assert result == {}


def test_biography_extract_no_infobox_no_paragraph_returns_empty(monkeypatch):
    """Page with no infobox and no <p> tag → returns {}."""
    import src.scraper.wiki_fetch as _wf

    class _MockSession:
        def get(self, *a, **kw):
            return _Resp(200, "<html><body><div>No paragraph here</div></body></html>")

    monkeypatch.setattr(_wf, "_session", _MockSession())
    bio = _bio()
    result = bio.biography_extract("https://en.example.org/wiki/Jane_Doe")
    assert result == {}


# ---------------------------------------------------------------------------
# Biography.parse_first_paragraph
# ---------------------------------------------------------------------------


def test_parse_first_paragraph_bold_name(monkeypatch):
    """Bold text in paragraph is extracted as full_name.
    parse_date_info is monkeypatched because it receives a BS4 tag, not a string.
    """
    monkeypatch.setattr(DataCleanup, "parse_date_info", lambda self, s, t: (None, None))
    html = "<p><b>John Smith</b> was born on January 1, 1945.</p>"
    soup = BeautifulSoup(html, "html.parser")
    para = soup.find("p")
    bio = _bio()
    details = bio.parse_first_paragraph(para)
    assert details["full_name"] == "John Smith"


def test_parse_first_paragraph_no_bold(monkeypatch):
    """Paragraph with no bold tag returns full_name=None."""
    monkeypatch.setattr(DataCleanup, "parse_date_info", lambda self, s, t: (None, None))
    html = "<p>Some text without bold.</p>"
    soup = BeautifulSoup(html, "html.parser")
    para = soup.find("p")
    bio = _bio()
    details = bio.parse_first_paragraph(para)
    assert details["full_name"] is None


# ---------------------------------------------------------------------------
# Biography.parse_infobox
# ---------------------------------------------------------------------------


def test_parse_infobox_extracts_name_and_birth():
    html = """
    <table class="infobox vcard">
      <tr><th class="infobox-above">Jane Doe</th></tr>
      <tr><th>Born</th><td>March 5, 1960</td></tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    infobox = soup.find("table")
    bio = _bio()
    details = bio.parse_infobox(infobox)
    assert details["name"] == "Jane Doe"
    assert details["birth_date"] is not None


def test_parse_infobox_extracts_death_date():
    html = """
    <table class="infobox vcard">
      <tr><th class="infobox-above">Jane Doe</th></tr>
      <tr><th>Born</th><td>March 5, 1960</td></tr>
      <tr><th>Died</th><td>June 10, 2020</td></tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    infobox = soup.find("table")
    bio = _bio()
    details = bio.parse_infobox(infobox)
    assert details["death_date"] is not None


# ---------------------------------------------------------------------------
# Offices.extract_party — text-based matching
# ---------------------------------------------------------------------------


def _make_table_config(party_column: int = 0, party_link: bool = False) -> dict:
    return {
        "party_column": party_column,
        "party_link": party_link,
        "term_start_column": 1,
        "term_end_column": 2,
        "link_column": 3,
        "district_column": -1,
        "table_rows": 0,
        "table_no": 1,
        "infobox_role_key": "",
        "years_only": False,
        "remove_duplicates": False,
        "consolidate_rowspan_terms": False,
        "use_full_page_for_table": False,
        "row_filter": None,
        "infobox_role_key_filter_id": None,
    }


def _make_office_details(country: str = "United States of America") -> dict:
    return {
        "office_country": country,
        "office_state": "",
        "office_level": "Federal",
        "office_branch": "Legislative",
    }


def test_extract_party_text_match_returns_party_name():
    """Party text matched case-insensitively by name."""
    tc = _make_table_config(party_column=0, party_link=False)
    od = _make_office_details()
    party_list = {"United States of America": [{"name": "Democratic", "link": ""}]}
    cells = [_cell("Democratic Party")]
    offices = _offices()
    result = offices.extract_party(
        "https://en.example.org/wiki/P", cells, od, tc, 0, party_list, ""
    )
    assert result == "Democratic"


def test_extract_party_no_match_returns_no_value():
    """No matching party → returns no_value_return."""
    tc = _make_table_config(party_column=0, party_link=False)
    od = _make_office_details()
    party_list = {"United States of America": [{"name": "Democratic", "link": ""}]}
    cells = [_cell("Green Party")]
    offices = _offices()
    result = offices.extract_party(
        "https://en.example.org/wiki/P", cells, od, tc, 0, party_list, "N/A"
    )
    assert result == "N/A"


def test_extract_party_column_out_of_bounds_returns_no_value():
    """Column index beyond cells length → no_value_return."""
    tc = _make_table_config(party_column=5, party_link=False)
    od = _make_office_details()
    offices = _offices()
    result = offices.extract_party("", [_cell("x")], od, tc, 5, {}, "NO_PARTY")
    assert result == "NO_PARTY"


def test_extract_party_unknown_country_returns_no_value():
    """Country not in party_list → no_value_return."""
    tc = _make_table_config(party_column=0, party_link=False)
    od = _make_office_details("Canada")
    party_list = {"United States of America": [{"name": "Democratic", "link": ""}]}
    cells = [_cell("Democratic")]
    offices = _offices()
    result = offices.extract_party("", cells, od, tc, 0, party_list, "NONE")
    assert result == "NONE"


# ---------------------------------------------------------------------------
# Offices.extract_district
# ---------------------------------------------------------------------------


def test_extract_district_ordinal():
    """Ordinal number in district cell → returns the text."""
    tc = _make_table_config()
    tc["district_column"] = 0
    od = _make_office_details()
    cells = [_cell("3rd")]
    offices = _offices()
    result = offices.extract_district("", cells, od, tc, 0, "No district")
    assert result == "3rd"


def test_extract_district_at_large():
    """At-large text → returns cell text."""
    tc = _make_table_config()
    tc["district_column"] = 0
    od = _make_office_details()
    cells = [_cell("At-large")]
    offices = _offices()
    result = offices.extract_district("", cells, od, tc, 0, "No district")
    assert result == "At-large"


def test_extract_district_territory():
    """Territory text → returns cell text."""
    tc = _make_table_config()
    tc["district_column"] = 0
    od = _make_office_details()
    cells = [_cell("Territory")]
    offices = _offices()
    result = offices.extract_district("", cells, od, tc, 0, "No district")
    assert result == "Territory"


def test_extract_district_non_matching_text_returns_no_district():
    """Cell text that doesn't match any pattern → returns 'No district'."""
    tc = _make_table_config()
    tc["district_column"] = 0
    od = _make_office_details()
    cells = [_cell("Statewide")]
    offices = _offices()
    result = offices.extract_district("", cells, od, tc, 0, "No district")
    assert result == "No district"


def test_extract_district_out_of_bounds_returns_no_district():
    """District column index beyond cells → 'No district'."""
    tc = _make_table_config()
    tc["district_column"] = 10
    od = _make_office_details()
    cells = [_cell("x")]
    offices = _offices()
    result = offices.extract_district("", cells, od, tc, 10, "No district")
    assert result == "No district"


# ---------------------------------------------------------------------------
# DataCleanup.remove_footnote — extract_text and strip_text branches
# ---------------------------------------------------------------------------


def test_remove_footnote_extract_text_from_bs4_tag():
    """extract_text=True calls .get_text() on the BS4 tag (line 565)."""
    dc = _dc()
    html = "<td>Some text [1]</td>"
    tag = BeautifulSoup(html, "html.parser").find("td")
    result = dc.remove_footnote(tag, extract_text=True)
    assert "[1]" not in result
    assert "Some text" in result


def test_remove_footnote_strip_text_strips_whitespace():
    """strip_text=True with a string input strips surrounding whitespace (line 574)."""
    dc = _dc()
    result = dc.remove_footnote("  hello world [3]  ", strip_text=True)
    assert result == "hello world"


# ---------------------------------------------------------------------------
# Offices._is_valid_wiki_link — branch coverage
# ---------------------------------------------------------------------------


def test_is_valid_wiki_link_non_string_returns_false():
    """Non-string input → False (line 612)."""
    offices = _offices()
    assert offices._is_valid_wiki_link(None) is False
    assert offices._is_valid_wiki_link(42) is False


def test_is_valid_wiki_link_empty_or_no_link():
    """Empty string or 'No link' → False."""
    offices = _offices()
    assert offices._is_valid_wiki_link("") is False
    assert offices._is_valid_wiki_link("No link") is False


def test_is_valid_wiki_link_non_wikipedia_url():
    """URL that doesn't start with https://en.example.org/wiki/ → False (line 617)."""
    offices = _offices()
    assert offices._is_valid_wiki_link("https://example.com/wiki/Test") is False


def test_is_valid_wiki_link_party_link_returns_false():
    """URL matching Party pattern → False (line 622)."""
    with patch("src.scraper.table_parser.WIKI_BASE_URL", "https://en.example.org"):
        offices = _offices()
        assert offices._is_valid_wiki_link("https://en.example.org/wiki/Republican_Party") is False


def test_is_valid_wiki_link_file_link_returns_false():
    """URL with /wiki/File: → False (line 624)."""
    with patch("src.scraper.table_parser.WIKI_BASE_URL", "https://en.example.org"):
        offices = _offices()
        assert offices._is_valid_wiki_link("https://en.example.org/wiki/File:Test.jpg") is False


def test_is_valid_wiki_link_special_link_returns_false():
    """URL with /wiki/Special: → False."""
    with patch("src.scraper.table_parser.WIKI_BASE_URL", "https://en.example.org"):
        offices = _offices()
        assert offices._is_valid_wiki_link("https://en.example.org/wiki/Special:Search") is False


def test_is_valid_wiki_link_valid_link():
    """Normal politician link → True."""
    with patch("src.scraper.table_parser.WIKI_BASE_URL", "https://en.example.org"):
        offices = _offices()
        assert offices._is_valid_wiki_link("https://en.example.org/wiki/Joe_Biden") is True


# ---------------------------------------------------------------------------
# Offices._row_matches_filter — filter branching
# ---------------------------------------------------------------------------


def _bs4_row(cells: list[str]) -> BeautifulSoup:
    """Build a <tr> with <td> cells."""
    tds = "".join(f"<td>{c}</td>" for c in cells)
    html = f"<table><tr>{tds}</tr></table>"
    return BeautifulSoup(html, "html.parser").find("tr")


def test_row_matches_filter_no_criteria_returns_true():
    """No filter criteria → always returns True."""
    offices = _offices()
    row = _bs4_row(["Alice", "D", "2001"])
    tc = {"row_filter_column": 1, "row_filter_criteria": ""}
    assert offices._row_matches_filter(row, tc) is True


def test_row_matches_filter_column_out_of_bounds_returns_false():
    """filter_col beyond row length → False (line 641)."""
    offices = _offices()
    row = _bs4_row(["Alice"])
    tc = {"row_filter_column": 5, "row_filter_criteria": "Senator"}
    assert offices._row_matches_filter(row, tc) is False


def test_row_matches_filter_matching_text_returns_true():
    """Cell text contains criteria → True (line 645-646)."""
    offices = _offices()
    row = _bs4_row(["Senator Alice", "D"])
    tc = {"row_filter_column": 0, "row_filter_criteria": "senator"}
    assert offices._row_matches_filter(row, tc) is True


def test_row_matches_filter_non_matching_text_returns_false():
    """Cell text does not contain criteria → False."""
    offices = _offices()
    row = _bs4_row(["Representative Alice", "R"])
    tc = {"row_filter_column": 0, "row_filter_criteria": "Senator"}
    assert offices._row_matches_filter(row, tc) is False


# ---------------------------------------------------------------------------
# _emit_merged_run — merge logic branches
# ---------------------------------------------------------------------------


def _make_row(
    start: str | None = None,
    end: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict:
    return {
        "Wiki Link": "https://en.example.org/wiki/Alice",
        "Party": "Democratic",
        "District": "",
        "Term Start": start,
        "Term End": end,
        "Term Start Year": start_year,
        "Term End Year": end_year,
    }


def test_emit_merged_run_empty_run():
    """Empty run → no row appended to out (line 142)."""
    out: list = []
    _emit_merged_run([], years_only=False, out=out)
    assert out == []


def test_emit_merged_run_single_row():
    """Single-row run → row appended as-is."""
    out: list = []
    row = _make_row("2000-01-01", "2004-01-01")
    _emit_merged_run([row], years_only=False, out=out)
    assert len(out) == 1
    assert out[0]["Term Start"] == "2000-01-01"


def test_emit_merged_run_multi_row_years_only():
    """Multi-row years_only run: min start, max end (lines 148-153)."""
    out: list = []
    rows = [
        _make_row(start_year=2000, end_year=2002),
        _make_row(start_year=2002, end_year=2006),
    ]
    _emit_merged_run(rows, years_only=True, out=out)
    assert len(out) == 1
    assert out[0]["Term Start Year"] == 2000
    assert out[0]["Term End Year"] == 2006


def test_emit_merged_run_multi_row_date_merge():
    """Multi-row non-years_only run: earliest start, latest end (lines 155-162)."""
    out: list = []
    rows = [
        _make_row("2000-01-01", "2004-06-01"),
        _make_row("2004-06-01", "2008-12-31"),
    ]
    _emit_merged_run(rows, years_only=False, out=out)
    assert len(out) == 1
    assert out[0]["Term Start"] == "2000-01-01"
    assert out[0]["Term End"] == "2008-12-31"


# ---------------------------------------------------------------------------
# Biography.parse_infobox — nickname div branch
# ---------------------------------------------------------------------------


def test_parse_infobox_with_nickname_div():
    """Infobox with .nickname div uses it as full_name (line 2288)."""
    html = """
    <table class="infobox vcard">
      <tr><th class="infobox-above">J. Smith</th></tr>
      <tr><td><div class="nickname">John Henry Smith</div></td></tr>
      <tr><th>Born</th><td>March 5, 1960</td></tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    infobox = soup.find("table")
    bio = _bio()
    details = bio.parse_infobox(infobox)
    assert details["full_name"] == "John Henry Smith"
    assert details["name"] == "J. Smith"


# ---------------------------------------------------------------------------
# Offices._is_valid_wiki_link — patterns_to_ignore branch (line 620)
# ---------------------------------------------------------------------------


def test_is_valid_wiki_link_congress_link_returns_false():
    """URL matching patterns_to_ignore (Congress pattern) → False (line 620)."""
    with patch("src.scraper.table_parser.WIKI_BASE_URL", "https://en.example.org"):
        offices = _offices()
        # Matches r"/wiki/\d{1,3}(th|st|nd|rd)_United_States_Congress"
        url = "https://en.example.org/wiki/117th_United_States_Congress"
        assert offices._is_valid_wiki_link(url) is False


def test_is_valid_wiki_link_year_link_returns_false():
    """URL matching year pattern → False."""
    with patch("src.scraper.table_parser.WIKI_BASE_URL", "https://en.example.org"):
        offices = _offices()
        # Matches r"/wiki/(19|20)\d{2}(_\d)?$"
        url = "https://en.example.org/wiki/2024"
        assert offices._is_valid_wiki_link(url) is False


# ---------------------------------------------------------------------------
# Offices._row_matches_filter — None column and invalid type branches
# ---------------------------------------------------------------------------


def test_row_matches_filter_none_column_treated_as_no_filter():
    """filter_col=None is set to -1 → no filter applied, returns True (line 632)."""
    offices = _offices()
    row = _bs4_row(["Alice"])
    tc = {"row_filter_column": None, "row_filter_criteria": "Senator"}
    assert offices._row_matches_filter(row, tc) is True


def test_row_matches_filter_non_int_column_defaults_to_no_filter():
    """Non-integer filter_col → ValueError caught → -1 → returns True (lines 635-636)."""
    offices = _offices()
    row = _bs4_row(["Alice"])
    tc = {"row_filter_column": "not-an-int", "row_filter_criteria": "Senator"}
    assert offices._row_matches_filter(row, tc) is True


# ---------------------------------------------------------------------------
# Log-level classification: skipped-row paths emit WARNING
# ---------------------------------------------------------------------------


def _table_html_with_short_row() -> str:
    """HTML table whose second row has too few cells to satisfy table_rows=4."""
    return (
        "<table>"
        "<tr><th>Name</th><th>Party</th><th>Start</th><th>End</th></tr>"
        "<tr><td>Only one cell</td></tr>"
        "</table>"
    )


def test_short_row_logs_warning(caplog):
    """parse_table_row should emit a WARNING when a row has too few cells."""
    import logging

    offices = _offices()
    html = _table_html_with_short_row()
    table_config = {
        "url": "https://en.example.org/wiki/Test",
        "table_no": 1,
        "table_rows": 4,
        "link_column": 0,
        "party_column": 1,
        "term_start_column": 2,
        "term_end_column": 3,
        "district_column": 0,
        "dynamic_parse": False,
        "read_columns_right_to_left": False,
        "find_date_in_infobox": False,
        "years_only": False,
        "parse_rowspan": False,
        "consolidate_rowspan_terms": False,
        "rep_link": False,
        "party_link": False,
        "alt_links": [],
        "alt_link_include_main": False,
        "use_full_page_for_table": False,
        "term_dates_merged": False,
        "party_ignore": False,
        "district_ignore": False,
        "district_at_large": False,
        "ignore_non_links": False,
        "infobox_role_key": "",
        "row_filter_column": None,
        "row_filter_criteria": "",
        "run_dynamic_parse": False,
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Legislative",
        "office_department": "",
        "office_name": "Test Office",
        "office_state": "",
        "office_notes": "",
    }
    with caplog.at_level(logging.WARNING, logger="src.scraper.table_parser"):
        offices.process_table(
            html, table_config, office_details, "https://en.example.org/wiki/Test", []
        )
    assert any(
        "issue with table rows" in r.message for r in caplog.records
    ), "Expected a WARNING about too-few rows"


# ---------------------------------------------------------------------------
# #372 — find_date_in_infobox guard: "No link" must not trigger HTTP call
# ---------------------------------------------------------------------------


def _table_html_no_link_row() -> str:
    """HTML table whose data row has no <a> tag (produces wiki_link='No link')."""
    return (
        "<table>"
        "<tr><th>Name</th><th>Party</th><th>Start</th><th>End</th></tr>"
        "<tr><td>Jane Doe</td><td>Independent</td><td>2020</td><td>2024</td></tr>"
        "</table>"
    )


def _infobox_table_config() -> dict:
    """Minimal table_config with find_date_in_infobox=True."""
    return {
        "url": "https://en.example.org/wiki/Test",
        "table_no": 1,
        "table_rows": 4,
        "link_column": 0,
        "party_column": 1,
        "term_start_column": 2,
        "term_end_column": 3,
        "district_column": 0,
        "dynamic_parse": False,
        "read_columns_right_to_left": False,
        "find_date_in_infobox": True,
        "years_only": False,
        "parse_rowspan": False,
        "consolidate_rowspan_terms": False,
        "rep_link": False,
        "party_link": False,
        "alt_links": [],
        "alt_link_include_main": False,
        "use_full_page_for_table": False,
        "term_dates_merged": False,
        "party_ignore": False,
        "district_ignore": False,
        "district_at_large": False,
        "ignore_non_links": False,
        "infobox_role_key": "",
        "row_filter_column": None,
        "row_filter_criteria": "",
        "run_dynamic_parse": False,
        "skip_infobox_for_urls": None,
        "existing_dates_lookup": {},
    }


def _office_details() -> dict:
    return {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Legislative",
        "office_department": "",
        "office_name": "Test Office",
        "office_state": "",
        "office_notes": "",
    }


def test_find_date_in_infobox_skips_no_link_row(monkeypatch):
    """When wiki_link=='No link' and find_date_in_infobox=True, find_term_dates must not be called."""
    import src.scraper.table_parser as tp_mod

    calls: list[str] = []

    def fake_find_term_dates(self, wiki_link, url, table_config, office_details, district, run_cache=None):
        calls.append(wiki_link)
        return [], []

    monkeypatch.setattr(tp_mod.Biography, "find_term_dates", fake_find_term_dates)

    offices = _offices()
    offices.process_table(
        _table_html_no_link_row(),
        _infobox_table_config(),
        _office_details(),
        "https://en.example.org/wiki/Test",
        [],
    )

    assert calls == [], f"find_term_dates must not be called for 'No link' rows, got calls: {calls}"
