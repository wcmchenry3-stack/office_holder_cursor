"""
Note: wikipedia.org URL strings below are test input values only.
No HTTP requests to Wikipedia are made here.
All actual Wikipedia HTTP requests go through wiki_fetch.py (wiki_session)
which sets the required User-Agent header and enforces rate limiting / retry/backoff logic.
"""

import pytest

from src.scraper.table_parser import Biography, DataCleanup, parse_infobox_role_key_query
from src.scraper.test_script_runner import run_test_script_from_html


class _Resp:
    def __init__(self, text: str):
        self.status_code = 200
        self.text = text


def _build_infobox_html() -> str:
    return """
    <table class="infobox vcard">
      <tr><th>Chief Judge of the <a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a></th></tr>
      <tr><td><b>In office</b><br>November 18, 1988 – February 28, 2010</td></tr>
      <tr><th>Senior Judge of the <a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a></th></tr>
      <tr><td><b>In office</b><br>February 28, 2010 – December 5, 2025</td></tr>
    </table>
    """


def _build_infobox_html_with_non_office_first_link() -> str:
    return """
    <table class="infobox vcard">
      <tr>
        <th>
          <a href="/wiki/Senior_status">Senior Judge</a> of the
          <a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a>
        </th>
      </tr>
      <tr><td><b>In office</b><br>February 28, 2010 – December 5, 2025</td></tr>
      <tr><th>Chief Judge of the <a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a></th></tr>
      <tr><td><b>In office</b><br>November 18, 1988 – February 28, 2010</td></tr>
    </table>
    """


def _build_infobox_html_with_all_three_roles() -> str:
    return """
    <table class="infobox vcard">
      <tr><th>Senior Judge of the <a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a></th></tr>
      <tr><td><b>In office</b><br>January 1, 2000 – January 1, 2010</td></tr>
      <tr><th>Chief Judge of the <a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a></th></tr>
      <tr><td><b>In office</b><br>January 1, 1990 – January 1, 2000</td></tr>
      <tr><th>Judge of the <a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a></th></tr>
      <tr><td><b>In office</b><br>January 1, 1980 – January 1, 1990</td></tr>
      <tr><th>Associate Justice of the <a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a></th></tr>
      <tr><td><b>In office</b><br>January 1, 1970 – January 1, 1980</td></tr>
    </table>
    """


def test_find_term_dates_filters_by_infobox_role_key(monkeypatch):
    import src.scraper.wiki_fetch as _wf

    class _S:
        def get(self, *a, **kw):
            return (lambda *args, **kwargs: _Resp(_build_infobox_html()))(*a, **kw)

    monkeypatch.setattr(_wf, "_session", _S())

    cleanup = DataCleanup()
    biography = Biography(cleanup)

    terms, _ = biography.find_term_dates(
        "https://en.wikipedia.org/wiki/Alex_R._Munson",
        "https://en.wikipedia.org/wiki/List_of_judges",
        {
            "rep_link": False,
            "alt_links": ["/wiki/District_Court_for_the_Northern_Mariana_Islands"],
            "alt_link_include_main": False,
            "infobox_role_key": '"senior judge"',
        },
        {"office_state": ""},
        "",
    )

    assert terms == [("2010-02-28", "2025-12-05")]


def test_find_term_dates_without_infobox_role_key_returns_all_matching_rows(monkeypatch):
    import src.scraper.wiki_fetch as _wf

    class _S:
        def get(self, *a, **kw):
            return (lambda *args, **kwargs: _Resp(_build_infobox_html()))(*a, **kw)

    monkeypatch.setattr(_wf, "_session", _S())

    cleanup = DataCleanup()
    biography = Biography(cleanup)

    terms, _ = biography.find_term_dates(
        "https://en.wikipedia.org/wiki/Alex_R._Munson",
        "https://en.wikipedia.org/wiki/List_of_judges",
        {
            "rep_link": False,
            "alt_links": ["/wiki/District_Court_for_the_Northern_Mariana_Islands"],
            "alt_link_include_main": False,
        },
        {"office_state": ""},
        "",
    )

    assert terms == [("1988-11-18", "2010-02-28"), ("2010-02-28", "2025-12-05")]


def test_find_term_dates_role_key_matches_when_first_link_is_not_office(monkeypatch):
    import src.scraper.wiki_fetch as _wf

    class _S:
        def get(self, *a, **kw):
            return (
                lambda *args, **kwargs: _Resp(_build_infobox_html_with_non_office_first_link())
            )(*a, **kw)

    monkeypatch.setattr(_wf, "_session", _S())

    cleanup = DataCleanup()
    biography = Biography(cleanup)

    terms, _ = biography.find_term_dates(
        "https://en.wikipedia.org/wiki/Alex_R._Munson",
        "https://en.wikipedia.org/wiki/List_of_judges",
        {
            "rep_link": False,
            "alt_links": ["/wiki/District_Court_for_the_Northern_Mariana_Islands"],
            "alt_link_include_main": False,
            "infobox_role_key": '"senior judge"',
        },
        {"office_state": ""},
        "",
    )

    assert terms == [("2010-02-28", "2025-12-05")]


def test_find_term_dates_role_key_supports_excludes(monkeypatch):
    import src.scraper.wiki_fetch as _wf

    class _S:
        def get(self, *a, **kw):
            return (lambda *args, **kwargs: _Resp(_build_infobox_html_with_all_three_roles()))(
                *a, **kw
            )

    monkeypatch.setattr(_wf, "_session", _S())

    cleanup = DataCleanup()
    biography = Biography(cleanup)

    terms, _ = biography.find_term_dates(
        "https://en.wikipedia.org/wiki/Alex_R._Munson",
        "https://en.wikipedia.org/wiki/List_of_judges",
        {
            "rep_link": False,
            "alt_links": ["/wiki/District_Court_for_the_Northern_Mariana_Islands"],
            "alt_link_include_main": False,
            "infobox_role_key": '"judge" "associate justice" -"chief judge" -"senior judge"',
        },
        {"office_state": ""},
        "",
    )

    assert terms == [("1980-01-01", "1990-01-01"), ("1970-01-01", "1980-01-01")]


def test_infobox_role_key_allows_legacy_unquoted_include_with_quoted_excludes():
    includes, excludes = parse_infobox_role_key_query('judge -"chief judge" -"senior judge"')

    assert includes == ["judge"]
    assert excludes == ["chief judge", "senior judge"]


def test_infobox_role_key_requires_quoted_excludes_even_with_legacy_include():
    with pytest.raises(ValueError):
        parse_infobox_role_key_query("judge -chief")


def test_infobox_role_key_rejects_unclosed_quotes():
    with pytest.raises(ValueError):
        parse_infobox_role_key_query('"judge" -"chief judge')


def test_run_test_script_resolves_infobox_role_key_from_filter_id(monkeypatch):
    captured = {}

    def fake_parse(office_row, selected_table_html, _url):
        captured["role_key"] = office_row.get("infobox_role_key")
        return []

    monkeypatch.setattr(
        "src.scraper.test_script_runner.get_infobox_role_key_filter",
        lambda fid: {"id": fid, "role_key": '"senior judge"'},
    )
    monkeypatch.setattr("src.scraper.test_script_runner.parse_full_table_for_export", fake_parse)

    html = "<table><tr><th>Name</th></tr><tr><td>A</td></tr></table>"
    result = run_test_script_from_html(
        test_type="table_config",
        html_content=html,
        config_json={
            "table_no": 1,
            "infobox_role_key": '"chief judge"',
            "infobox_role_key_filter_id": 42,
        },
    )

    assert result["actual"] == []
    assert captured["role_key"] == '"senior judge"'


def test_run_test_script_keeps_legacy_infobox_role_key_when_filter_missing(monkeypatch):
    captured = {}

    def fake_parse(office_row, selected_table_html, _url):
        captured["role_key"] = office_row.get("infobox_role_key")
        return []

    monkeypatch.setattr(
        "src.scraper.test_script_runner.get_infobox_role_key_filter", lambda _fid: None
    )
    monkeypatch.setattr("src.scraper.test_script_runner.parse_full_table_for_export", fake_parse)

    html = "<table><tr><th>Name</th></tr><tr><td>A</td></tr></table>"
    run_test_script_from_html(
        test_type="table_config",
        html_content=html,
        config_json={
            "table_no": 1,
            "infobox_role_key": '"senior judge"',
            "infobox_role_key_filter_id": 9999,
        },
    )

    assert captured["role_key"] == '"senior judge"'
