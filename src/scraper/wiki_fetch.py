# -*- coding: utf-8 -*-
"""
Wikipedia fetch helpers: REST API URL, shared request headers, and a retry-aware session.
Uses Wikimedia REST API for HTML when possible (policy-friendly and CDN-cached).
The shared session automatically retries on transient errors and respects Retry-After on 429.
"""

import threading
import time
from urllib.parse import urlparse, unquote, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.scraper.logger import HTTP_USER_AGENT

# Headers for all Wikipedia requests (User-Agent + gzip per Wikimedia policy).
WIKIPEDIA_REQUEST_HEADERS = {
    "User-Agent": HTTP_USER_AGENT,
    "Accept-Encoding": "gzip",
}

_REQUEST_TIMEOUT = 30

# One session per process; thread-safe for concurrent reads.
_session: requests.Session | None = None

# Global rate limiter: enforce ≤1 Wikipedia HTTP request per second across all threads.
_throttle_lock = threading.Lock()
_last_request_at: list[float] = [0.0]  # mutable container so closures can update it
_MIN_REQUEST_INTERVAL = 1.0  # seconds


def wiki_throttle() -> None:
    """Block until at least 1 s has elapsed since the last Wikipedia HTTP request.

    Must be called immediately before every wiki_session().get() call.  Using a global
    lock ensures the ≤1 req/s limit is respected even with ThreadPoolExecutor workers.
    """
    with _throttle_lock:
        now = time.monotonic()
        wait = _MIN_REQUEST_INTERVAL - (now - _last_request_at[0])
        if wait > 0:
            time.sleep(wait)
        _last_request_at[0] = time.monotonic()


def wiki_session() -> requests.Session:
    """
    Return a shared requests.Session configured for Wikipedia access.

    Retry policy: 3 attempts, exponential backoff (1s, 2s, 4s), on transient HTTP
    errors (429, 500, 502, 503, 504) and connection errors.  Respects Retry-After
    headers on 429 so we stay within Wikimedia rate limits (~1 req/s sustained).
    """
    global _session
    if _session is None:
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s = requests.Session()
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update(WIKIPEDIA_REQUEST_HEADERS)
        _session = s
    return _session


def normalize_wiki_url(wiki_url: str) -> str | None:
    """
    Normalize a Wikipedia URL: strip trailing dot from host (e.g. en.wikipedia.org. -> en.wikipedia.org)
    and ensure path has /wiki/ prefix (e.g. /Thomas_Van_Lear -> /wiki/Thomas_Van_Lear).
    Returns normalized URL or None if not a Wikipedia URL.
    """
    if not (wiki_url or "").strip():
        return None
    try:
        p = urlparse(wiki_url.strip())
    except Exception:
        return None
    netloc = (p.netloc or "").rstrip(".")
    if "wikipedia.org" not in netloc:
        return None
    path = (p.path or "").strip().rstrip("/")
    parts = [x for x in path.split("/") if x]
    if len(parts) == 1:
        path = f"/wiki/{parts[0]}"
    return urlunparse((p.scheme or "https", netloc, path, p.params, p.query, p.fragment))


def canonical_holder_url(url: str) -> str:
    """
    Canonicalize holder URL for comparisons (e.g. matching existing terms to table rows).
    Normalizes via normalize_wiki_url and produces a stable /wiki/<title> key (lowercased)
    so scheme/host/query/encoding/case differences don't create false mismatches.
    """
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("No link:"):
        return u
    normalized = normalize_wiki_url(u)
    if normalized:
        try:
            p = urlparse(normalized)
            path = (p.path or "").rstrip("/")
            parts = [x for x in path.split("/") if x]
            if len(parts) >= 2 and parts[0].lower() == "wiki":
                title = unquote(parts[1]).replace(" ", "_").strip().lower()
                return f"/wiki/{title}"
            return urlunparse(("https", (p.netloc or "").lower(), path, "", "", ""))
        except Exception:
            return normalized
    return u


def wiki_url_to_rest_html_url(wiki_url: str) -> str | None:
    """
    If wiki_url is a Wikipedia page URL, return the REST API HTML URL for the same page.
    Otherwise return None.
    Example: https://en.wikipedia.org/wiki/Barack_Obama -> https://en.wikipedia.org/w/rest.php/v1/page/Barack_Obama/html
    """
    normalized = normalize_wiki_url(wiki_url)
    if not normalized:
        return None
    try:
        p = urlparse(normalized)
    except Exception:
        return None
    path = (p.path or "").strip().rstrip("/")
    parts = [x for x in path.split("/") if x]
    if len(parts) >= 2 and parts[0].lower() == "wiki":
        title = unquote(parts[1])
        if not title:
            return None
        scheme = p.scheme or "https"
        netloc = (p.netloc or "en.wikipedia.org").rstrip(".")
        return f"{scheme}://{netloc}/w/rest.php/v1/page/{title}/html"
    return None
