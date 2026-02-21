from src.scraper.logger import Logger
from src.scraper.table_parser import Biography, DataCleanup


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


def test_find_term_dates_filters_by_infobox_role_key(monkeypatch):
    monkeypatch.setattr(
        "src.scraper.table_parser.requests.get",
        lambda *args, **kwargs: _Resp(_build_infobox_html()),
    )

    logger = Logger("test", "infobox_role_key")
    cleanup = DataCleanup(logger)
    biography = Biography(logger, cleanup)

    terms, _ = biography.find_term_dates(
        "https://en.wikipedia.org/wiki/Alex_R._Munson",
        "https://en.wikipedia.org/wiki/List_of_judges",
        {
            "rep_link": False,
            "alt_links": ["/wiki/District_Court_for_the_Northern_Mariana_Islands"],
            "alt_link_include_main": False,
            "infobox_role_key": "senior judge",
        },
        {"office_state": ""},
        "",
    )

    assert terms == [("2010-02-28", "2025-12-05")]


def test_find_term_dates_without_infobox_role_key_returns_all_matching_rows(monkeypatch):
    monkeypatch.setattr(
        "src.scraper.table_parser.requests.get",
        lambda *args, **kwargs: _Resp(_build_infobox_html()),
    )

    logger = Logger("test", "infobox_no_role_key")
    cleanup = DataCleanup(logger)
    biography = Biography(logger, cleanup)

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
