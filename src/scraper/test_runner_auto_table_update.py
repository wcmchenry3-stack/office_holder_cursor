from src.scraper import runner


def _row(url: str, start: str, end: str) -> dict:
    return {
        "Wiki Link": url,
        "Term Start": start,
        "Term End": end,
        "Party": "",
        "District": "",
    }


def test_auto_table_update_chooses_best_matching_table(monkeypatch):
    existing_terms = [
        {"wiki_url": "https://en.wikipedia.org/wiki/A", "term_start": "2000-01-01", "term_end": "2001-01-01"},
        {"wiki_url": "https://en.wikipedia.org/wiki/B", "term_start": "2001-01-02", "term_end": "2002-01-01"},
    ]

    calls: list[int] = []

    def fake_cache(url, table_no, refresh=False, use_full_page=False):
        if table_no == 1:
            return {"num_tables": 3, "html": "<table>current</table>"}
        return {"num_tables": 3, "html": f"<table>{table_no}</table>"}

    def fake_parse(office_row, html, url, party_list, offices_parser, **kwargs):
        tno = int(office_row.get("table_no") or 1)
        calls.append(tno)
        if tno == 2:
            return [_row("https://en.wikipedia.org/wiki/A", "2000-01-01", "2001-01-01")]
        if tno == 3:
            return [
                _row("https://en.wikipedia.org/wiki/A", "2000-01-01", "2001-01-01"),
                _row("https://en.wikipedia.org/wiki/B", "2001-01-02", "2002-01-01"),
            ]
        return []

    monkeypatch.setattr(runner, "get_table_html_cached", fake_cache)
    monkeypatch.setattr(runner, "_parse_office_html", fake_parse)

    table_no, rows = runner._try_auto_update_table_no(
        {
            "id": 123,
            "url": "https://en.wikipedia.org/wiki/List",
            "table_no": 1,
            "use_full_page_for_table": False,
            "disable_auto_table_update": False,
        },
        existing_terms,
        party_list=[],
        offices_parser=object(),
        refresh_table_cache=False,
        years_only=False,
        key_years_only=False,
        current_missing_count=2,
    )

    assert table_no == 3
    assert rows is not None and len(rows) == 2
    assert calls == [2, 3]


def test_auto_table_update_respects_disable_flag(monkeypatch):
    called = {"cache": 0}

    def fake_cache(*args, **kwargs):
        called["cache"] += 1
        return {"num_tables": 5, "html": "<table></table>"}

    monkeypatch.setattr(runner, "get_table_html_cached", fake_cache)

    table_no, rows = runner._try_auto_update_table_no(
        {
            "id": 7,
            "url": "https://en.wikipedia.org/wiki/List",
            "table_no": 1,
            "disable_auto_table_update": True,
        },
        existing_terms=[],
        party_list=[],
        offices_parser=object(),
        refresh_table_cache=False,
        years_only=False,
        key_years_only=False,
        current_missing_count=1,
    )

    assert table_no is None
    assert rows is None
    assert called["cache"] == 0
