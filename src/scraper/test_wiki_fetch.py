"""Unit tests for wiki_fetch.py — all pure functions, no mocking needed."""

import pytest

from src.scraper.wiki_fetch import (
    canonical_holder_url,
    normalize_wiki_url,
    wiki_url_to_rest_html_url,
)

# ---------------------------------------------------------------------------
# normalize_wiki_url
# ---------------------------------------------------------------------------


def test_normalize_none_returns_none():
    assert normalize_wiki_url(None) is None


def test_normalize_empty_returns_none():
    assert normalize_wiki_url("") is None


def test_normalize_whitespace_returns_none():
    assert normalize_wiki_url("   ") is None


def test_normalize_already_normal():
    url = "https://en.wikipedia.org/wiki/Barack_Obama"
    assert normalize_wiki_url(url) == url


def test_normalize_missing_wiki_prefix():
    result = normalize_wiki_url("https://en.wikipedia.org/Thomas_Van_Lear")
    assert result == "https://en.wikipedia.org/wiki/Thomas_Van_Lear"


def test_normalize_trailing_dot_on_host_stripped():
    result = normalize_wiki_url("https://en.wikipedia.org./wiki/Barack_Obama")
    assert result is not None
    assert "en.wikipedia.org." not in result
    assert "en.wikipedia.org" in result


def test_normalize_non_wikipedia_url_returns_none():
    assert normalize_wiki_url("https://example.com/wiki/Foo") is None


def test_normalize_non_wikipedia_plain_url_returns_none():
    assert normalize_wiki_url("https://example.com/person") is None


# ---------------------------------------------------------------------------
# canonical_holder_url
# ---------------------------------------------------------------------------


def test_canonical_empty_returns_empty():
    assert canonical_holder_url("") == ""


def test_canonical_lowercased():
    result = canonical_holder_url("https://en.wikipedia.org/wiki/Barack_Obama")
    assert result == "/wiki/barack_obama"


def test_canonical_no_link_prefix_passthrough():
    url = "No link: Some Person"
    assert canonical_holder_url(url) == url


def test_canonical_non_wiki_url_returned_as_is():
    url = "https://example.com/person"
    assert canonical_holder_url(url) == url


def test_canonical_strips_query_and_fragment():
    result = canonical_holder_url("https://en.wikipedia.org/wiki/Barack_Obama?oldid=123#section")
    assert result == "/wiki/barack_obama"


def test_canonical_underscores_preserved():
    result = canonical_holder_url("https://en.wikipedia.org/wiki/John_Quincy_Adams")
    assert result == "/wiki/john_quincy_adams"


# ---------------------------------------------------------------------------
# wiki_url_to_rest_html_url
# ---------------------------------------------------------------------------


def test_rest_html_url_basic():
    result = wiki_url_to_rest_html_url("https://en.wikipedia.org/wiki/Barack_Obama")
    assert result == "https://en.wikipedia.org/w/rest.php/v1/page/Barack_Obama/html"


def test_rest_html_url_none_input():
    assert wiki_url_to_rest_html_url(None) is None


def test_rest_html_url_empty_returns_none():
    assert wiki_url_to_rest_html_url("") is None


def test_rest_html_url_non_wikipedia_returns_none():
    assert wiki_url_to_rest_html_url("https://example.com/page") is None


def test_rest_html_url_preserves_page_title():
    result = wiki_url_to_rest_html_url("https://en.wikipedia.org/wiki/John_Quincy_Adams")
    assert "John_Quincy_Adams" in result
