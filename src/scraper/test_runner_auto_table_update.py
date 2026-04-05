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
        {
            "wiki_url": "https://en.wikipedia.org/wiki/A",
            "term_start": "2000-01-01",
            "term_end": "2001-01-01",
        },
        {
            "wiki_url": "https://en.wikipedia.org/wiki/B",
            "term_start": "2001-01-02",
            "term_end": "2002-01-01",
        },
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
    assert calls == [1, 2, 3]


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


def test_auto_table_update_uses_years_fallback_when_exact_dates_tie(monkeypatch):
    existing_terms = [
        {
            "wiki_url": "https://en.wikipedia.org/wiki/A",
            "term_start": "2000-01-01",
            "term_end": "2001-01-01",
        },
        {
            "wiki_url": "https://en.wikipedia.org/wiki/B",
            "term_start": "2001-01-02",
            "term_end": "2002-01-01",
        },
    ]

    def fake_cache(url, table_no, refresh=False, use_full_page=False):
        return {"num_tables": 3, "html": f"<table>{table_no}</table>"}

    def fake_parse(office_row, html, url, party_list, offices_parser, **kwargs):
        tno = int(office_row.get("table_no") or 1)
        # table 1 (current): exact mismatch for both, and years do NOT align
        if tno == 1:
            return [
                _row("https://en.wikipedia.org/wiki/A", "1999-06-01", "2000-06-01"),
                _row("https://en.wikipedia.org/wiki/B", "1999-06-02", "2000-06-01"),
            ]
        # table 2: same exact-mismatch count, but better years-only alignment (0 missing)
        if tno == 2:
            return [
                _row("https://en.wikipedia.org/wiki/A", "2000-07-01", "2001-07-01"),
                _row("https://en.wikipedia.org/wiki/B", "2001-07-02", "2002-07-01"),
            ]
        # table 3: same exact-mismatch count and poor years-only alignment
        return [
            _row("https://en.wikipedia.org/wiki/A", "1999-01-01", "2000-01-01"),
            _row("https://en.wikipedia.org/wiki/B", "1999-01-02", "2000-01-02"),
        ]

    monkeypatch.setattr(runner, "get_table_html_cached", fake_cache)
    monkeypatch.setattr(runner, "_parse_office_html", fake_parse)

    table_no, _rows = runner._try_auto_update_table_no(
        {
            "id": 12,
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

    assert table_no == 2


def test_find_best_matching_table_reports_before_after(monkeypatch):
    class _Dummy:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(runner, "init_db", lambda: None)
    monkeypatch.setattr(runner, "configure_run_logging", lambda *a, **kw: None)
    monkeypatch.setattr(runner.parse_core, "DataCleanup", _Dummy)
    monkeypatch.setattr(runner.parse_core, "Biography", _Dummy)
    monkeypatch.setattr(runner.parse_core, "Offices", _Dummy)
    monkeypatch.setattr(runner.db_parties, "get_party_list_for_scraper", lambda: [])

    def fake_cache(url, table_no, refresh=False, use_full_page=False):
        if table_no == 1:
            return {"num_tables": 3, "html": "<table>1</table>"}
        return {"num_tables": 3, "html": f"<table>{table_no}</table>"}

    def fake_parse(office_row, html, url, party_list, offices_parser, **kwargs):
        tno = int(office_row.get("table_no") or 1)
        if tno == 1:
            return [_row("https://en.wikipedia.org/wiki/A", "2000-01-01", "2001-01-01")]
        if tno == 2:
            return [
                _row("https://en.wikipedia.org/wiki/A", "2000-01-01", "2001-01-01"),
                _row("https://en.wikipedia.org/wiki/B", "2001-01-02", "2002-01-01"),
            ]
        return [_row("https://en.wikipedia.org/wiki/A", "2000-01-01", "2001-01-01")]

    monkeypatch.setattr(runner, "get_table_html_cached", fake_cache)
    monkeypatch.setattr(runner, "_parse_office_html", fake_parse)

    existing_terms = [
        {
            "wiki_url": "https://en.wikipedia.org/wiki/A",
            "term_start": "2000-01-01",
            "term_end": "2001-01-01",
        },
        {
            "wiki_url": "https://en.wikipedia.org/wiki/B",
            "term_start": "2001-01-02",
            "term_end": "2002-01-01",
        },
    ]
    result = runner.find_best_matching_table_for_existing_terms(
        {
            "id": 99,
            "url": "https://en.wikipedia.org/wiki/List",
            "table_no": 1,
            "years_only": False,
            "use_full_page_for_table": False,
            "disable_auto_table_update": False,
        },
        existing_terms,
    )

    assert result["found_table_no"] == 2
    assert result["missing_before"] == 1
    assert result["missing_after"] == 0


def test_preview_reports_new_list_mismatch(monkeypatch):
    class _Dummy:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(runner, "init_db", lambda: None)
    monkeypatch.setattr(runner, "configure_run_logging", lambda *a, **kw: None)
    monkeypatch.setattr(runner.parse_core, "DataCleanup", _Dummy)
    monkeypatch.setattr(runner.parse_core, "Biography", _Dummy)
    monkeypatch.setattr(runner.parse_core, "Offices", _Dummy)
    monkeypatch.setattr(runner.db_parties, "get_party_list_for_scraper", lambda: [])
    monkeypatch.setattr(
        runner,
        "get_table_html_cached",
        lambda *args, **kwargs: {"table_no": 1, "num_tables": 1, "html": "<table>1</table>"},
    )
    monkeypatch.setattr(
        runner,
        "_parse_office_html",
        lambda *args, **kwargs: [
            _row("https://en.wikipedia.org/wiki/A", "2000-01-01", "2001-01-01")
        ],
    )
    monkeypatch.setattr(
        runner.db_office_terms,
        "get_existing_terms_for_office",
        lambda _id: [
            {
                "wiki_url": "https://en.wikipedia.org/wiki/A",
                "term_start": "2000-01-01",
                "term_end": "2001-01-01",
            },
            {
                "wiki_url": "https://en.wikipedia.org/wiki/B",
                "term_start": "2001-01-02",
                "term_end": "2002-01-01",
            },
        ],
    )

    result = runner.preview_with_config(
        {
            "url": "https://en.wikipedia.org/wiki/List",
            "table_no": 1,
            "years_only": False,
            "office_table_config_id": 42,
        },
        max_rows=10,
    )

    assert result["revalidate_failed"] is True
    assert result["revalidate_missing_holders"]
    assert "New list found" in (result["revalidate_message"] or "")


def test_url_only_matching_ignores_wikipedia_query_params():
    existing = [
        {
            "wiki_url": "https://en.wikipedia.org/wiki/Albert_Williams_(Michigan_Attorney_General)?action=edit&redlink=1"
        }
    ]
    parsed = [
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Albert_Williams_(Michigan_Attorney_General)",
            "Term Start": "",
            "Term End": "",
            "Term Start Year": 1863,
            "Term End Year": 1867,
            "Party": "",
            "District": "",
        }
    ]

    missing = runner._missing_holder_keys(existing, parsed, office_id=1, years_only=True)
    assert missing == set()


def test_url_only_matching_ignores_scheme_and_host_differences():
    existing = [{"wiki_url": "http://en.wikipedia.org/wiki/Daniel_LeRoy"}]
    parsed = [
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Daniel_LeRoy",
            "Term Start": "",
            "Term End": "",
            "Term Start Year": 1836,
            "Term End Year": 1837,
            "Party": "",
            "District": "",
        }
    ]

    missing = runner._missing_holder_keys(existing, parsed, office_id=1, years_only=True)
    assert missing == set()


def test_url_only_matching_ignores_encoding_and_title_case_differences():
    existing = [{"wiki_url": "https://en.wikipedia.org/wiki/Ra%C3%BAl_Labrador"}]
    parsed = [
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Raúl_Labrador",
            "Term Start": "",
            "Term End": "",
            "Term Start Year": 2023,
            "Term End Year": 2027,
            "Party": "",
            "District": "",
        }
    ]

    missing = runner._missing_holder_keys(existing, parsed, office_id=1, years_only=True)
    assert missing == set()


def test_matching_uses_active_links_even_when_dates_are_invalid():
    existing = [{"wiki_url": "https://en.wikipedia.org/wiki/Daniel_LeRoy"}]
    parsed = [
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Daniel_LeRoy",
            "Term Start": "Invalid date",
            "Term End": "Invalid date",
            "Term Start Year": None,
            "Term End Year": None,
            "_dead_link": False,
            "Party": "",
            "District": "",
        }
    ]

    missing = runner._missing_holder_keys(existing, parsed, office_id=1, years_only=False)
    assert missing == set()


def test_matching_ignores_deadlinks_from_existing_terms():
    existing = [
        {
            "wiki_url": "https://en.wikipedia.org/wiki/Albert_Williams_(Michigan_Attorney_General)?action=edit&redlink=1"
        }
    ]
    parsed = []

    missing = runner._missing_holder_keys(existing, parsed, office_id=1, years_only=False)
    assert missing == set()


def test_filtered_existing_keys_excludes_deadlinks_and_display_hides_them():
    existing = [
        {
            "wiki_url": "https://en.wikipedia.org/wiki/Alive_Person",
            "term_start_year": 1900,
            "term_end_year": 1901,
        },
        {
            "wiki_url": "https://en.wikipedia.org/wiki/Dead_Link?action=edit&redlink=1",
            "term_start_year": 1902,
            "term_end_year": 1903,
        },
    ]
    keys = runner._filtered_existing_holder_keys(existing, runner._holder_key_from_existing_term)
    assert ("/wiki/alive_person", "", "") in keys
    assert all(k[0] for k in keys)

    missing_keys = {("", "", ""), ("/wiki/alive_person", "", "")}
    labels = runner._missing_holders_display(
        existing, missing_keys, runner._holder_key_from_existing_term
    )
    assert labels == ["Alive Person (1900–1901)"]
