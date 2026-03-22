"""Unit tests for pure helper functions in runner.py."""

from src.scraper.runner import (
    _canonical_holder_url,
    _dedupe_parsed_rows,
    _holder_key_from_existing_term,
    _is_dead_wiki_url,
)

# ---------------------------------------------------------------------------
# _is_dead_wiki_url
# ---------------------------------------------------------------------------


def test_is_dead_url_redlink_param():
    assert (
        _is_dead_wiki_url("https://en.wikipedia.org/w/index.php?title=Foo&action=edit&redlink=1")
        is True
    )


def test_is_dead_url_normal_wiki_url():
    assert _is_dead_wiki_url("https://en.wikipedia.org/wiki/Barack_Obama") is False


def test_is_dead_url_empty_string():
    assert _is_dead_wiki_url("") is False


def test_is_dead_url_none():
    assert _is_dead_wiki_url(None) is False


# ---------------------------------------------------------------------------
# _canonical_holder_url
# ---------------------------------------------------------------------------


def test_canonical_url_lowercased():
    result = _canonical_holder_url("https://en.wikipedia.org/wiki/Barack_Obama")
    assert result == "/wiki/barack_obama"


def test_canonical_url_no_link_passthrough():
    url = "No link: Some Person"
    assert _canonical_holder_url(url) == url


def test_canonical_url_empty_returns_empty():
    assert _canonical_holder_url("") == ""


def test_canonical_url_strips_query_params():
    result = _canonical_holder_url("https://en.wikipedia.org/wiki/Barack_Obama?oldid=123")
    assert result == "/wiki/barack_obama"


# ---------------------------------------------------------------------------
# _holder_key_from_existing_term
# ---------------------------------------------------------------------------


def test_holder_key_with_valid_url():
    term = {"wiki_url": "https://en.wikipedia.org/wiki/Barack_Obama"}
    assert _holder_key_from_existing_term(term) == ("/wiki/barack_obama", "", "")


def test_holder_key_dead_link_returns_empty_tuple():
    term = {"wiki_url": "https://en.wikipedia.org/w/index.php?title=Foo&redlink=1"}
    assert _holder_key_from_existing_term(term) == ("", "", "")


def test_holder_key_no_url_returns_empty_tuple():
    assert _holder_key_from_existing_term({}) == ("", "", "")


def test_holder_key_empty_url_returns_empty_tuple():
    assert _holder_key_from_existing_term({"wiki_url": ""}) == ("", "", "")


# ---------------------------------------------------------------------------
# _dedupe_parsed_rows
# ---------------------------------------------------------------------------


def test_dedupe_empty_list():
    assert _dedupe_parsed_rows([], years_only=False) == []


def test_dedupe_preserves_rows_without_wiki_link():
    # Rows with no Wiki Link: _normalize_row_for_import returns None → rows pass through
    rows = [
        {"Wiki Link": "", "Term Start": "2020-01-01"},
        {"Wiki Link": "", "Term Start": "2021-01-01"},
    ]
    result = _dedupe_parsed_rows(rows, years_only=False)
    assert len(result) == 2


def test_dedupe_distinct_urls_all_kept():
    rows = [
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Alice",
            "Term Start": "2020-01-01",
            "Term End": None,
            "Party": "",
            "District": "",
        },
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Bob",
            "Term Start": "2020-01-01",
            "Term End": None,
            "Party": "",
            "District": "",
        },
    ]
    result = _dedupe_parsed_rows(rows, years_only=False)
    assert len(result) == 2
