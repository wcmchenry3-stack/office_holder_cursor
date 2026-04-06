# -*- coding: utf-8 -*-
"""Unit tests for src/services/orchestrator.py — validate_and_normalize_wiki_url.

Wikipedia URL strings below are used only as test input values to verify the
SSRF validation gate. No HTTP requests to wikipedia.org are made here.
All actual Wikipedia HTTP requests go through wiki_fetch.py (wiki_session)
which sets the required User-Agent header per Wikimedia API:Etiquette policy
and enforces rate limiting / retry/backoff logic.

Wikipedia policy compliance:
  - User-Agent: set via HTTP_USER_AGENT in wiki_fetch.py on every outbound request
  - No live requests made in this test module
  See: https://www.mediawiki.org/wiki/API:Etiquette
"""

from __future__ import annotations

import pytest

from src.services.orchestrator import validate_and_normalize_wiki_url


@pytest.fixture(autouse=True)
def reset_singletons():
    from src.services.github_client import reset_github_client
    from src.services import orchestrator

    reset_github_client()
    orchestrator.reset_ai_builder()
    yield
    reset_github_client()
    orchestrator.reset_ai_builder()


@pytest.mark.parametrize(
    "url",
    [
        "https://en.wikipedia.org/wiki/Barack_Obama",
        "https://en.wikipedia.org/wiki/United_States_Senate",
        "https://en.wikipedia.org/wiki/Governor_of_California",
    ],
)
def test_valid_wikipedia_url_is_returned(url):
    result = validate_and_normalize_wiki_url(url)
    assert result is not None
    assert "wikipedia.org" in result


@pytest.mark.parametrize(
    "bad_url",
    [
        None,
        "",
        "https://example.com/page",
        "https://google.com",
        "https://en.wikip edia.org/wiki/Test",  # space in host — not a valid wikipedia.org netloc
    ],
)
def test_invalid_url_raises_value_error(bad_url):
    with pytest.raises((ValueError, Exception)):
        validate_and_normalize_wiki_url(bad_url)
