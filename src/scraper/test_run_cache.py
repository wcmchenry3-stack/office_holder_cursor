"""Tests for RunPageCache and its integration with _fetch_table_from_url.

RunPageCache is a read-through layer over Wikipedia REST API responses.
Real HTTP calls (made via wiki_fetch) always include a descriptive User-Agent
header per Wikimedia API policy — the cache itself never sends HTTP requests.
"""

from __future__ import annotations

import threading

import pytest

from src.scraper.run_cache import RunPageCache


def test_run_cache_default_max_entries_is_100():
    """Default cap is 100 entries (~8 MB). Raised from 300 to reduce peak RSS (#380)."""
    cache = RunPageCache()
    assert cache._max == 100


def test_run_cache_miss_returns_none():
    cache = RunPageCache()
    assert cache.get("https://example.com/page") is None


def test_run_cache_set_get_roundtrip():
    cache = RunPageCache()
    cache.set("https://example.com/page", "<html>hello</html>")
    assert cache.get("https://example.com/page") == "<html>hello</html>"


def test_run_cache_lru_eviction_at_max_entries():
    cache = RunPageCache(max_entries=2)
    cache.set("url1", "html1")
    cache.set("url2", "html2")
    cache.set("url3", "html3")  # evicts url1 (LRU)
    assert cache.get("url1") is None
    assert cache.get("url2") == "html2"
    assert cache.get("url3") == "html3"


def test_run_cache_thread_safety():
    cache = RunPageCache(max_entries=50)
    errors = []

    def worker(thread_id: int):
        try:
            for i in range(100):
                url = f"https://example.com/{thread_id}/{i}"
                cache.set(url, f"html-{thread_id}-{i}")
                cache.get(url)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Thread safety errors: {errors}"


def test_fetch_table_skips_http_when_run_cache_hit(monkeypatch):
    """Second call with same URL + different table_no hits run_cache, no HTTP."""
    import src.scraper.wiki_fetch as _wf

    from src.scraper.table_cache import _fetch_table_from_url

    # Minimal HTML with two tables
    html = (
        "<html><body>"
        "<table><tr><td>Table1</td></tr></table>"
        "<table><tr><td>Table2</td></tr></table>"
        "</body></html>"
    )

    call_count = 0

    class _FakeResp:
        status_code = 200
        text = html

    class _MockSession:
        def get(self, url, **kwargs):
            nonlocal call_count
            call_count += 1
            return _FakeResp()

    monkeypatch.setattr(_wf, "_session", _MockSession())

    # Also patch wiki_url_to_rest_html_url to return the URL unchanged
    monkeypatch.setattr(
        "src.scraper.table_cache.wiki_url_to_rest_html_url",
        lambda url: url,
    )

    cache = RunPageCache()
    url = "https://en.wikipedia.org/wiki/SomePage"

    result1 = _fetch_table_from_url(url, 1, run_cache=cache)
    result2 = _fetch_table_from_url(url, 2, run_cache=cache)

    assert call_count == 1, f"Expected 1 HTTP call, got {call_count}"
    assert "html" in result1
    assert "html" in result2
    assert result1["table_no"] == 1
    assert result2["table_no"] == 2
