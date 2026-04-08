# -*- coding: utf-8 -*-
"""
Local cache for Wikipedia table HTML. One file per (url, table_no); raw HTML stored in gzipped JSON.
Preview / test / run use cache by default; use Refresh to refetch from Wikipedia.
"""

import gzip
import hashlib
import json
import logging
import time
import threading
import weakref
from pathlib import Path

from bs4 import BeautifulSoup

from src.db.connection import get_cache_dir
from src.scraper.wiki_fetch import wiki_session, wiki_url_to_rest_html_url

logger = logging.getLogger(__name__)

TIMEOUT = 30
_LOCK = threading.Lock()

# WeakValueDictionary: entries are removed automatically once no thread holds
# a reference to the _KeyLock, so the dict never grows unboundedly.
_key_locks: weakref.WeakValueDictionary = weakref.WeakValueDictionary()
_key_locks_lock = threading.Lock()


class _KeyLock:
    """Thin wrapper around threading.Lock that is weakly referenceable."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *args):
        self._lock.release()


def _cache_dir() -> Path:
    """Return the wiki cache directory, respecting WIKI_CACHE_DIR env var (for persistent disk storage)."""
    return get_cache_dir()


def _cache_key(url: str, table_no: int, use_full_page: bool = False) -> str:
    normalized = (url.strip() + "|" + str(table_no) + "|" + ("1" if use_full_page else "0")).encode(
        "utf-8"
    )
    return hashlib.sha256(normalized).hexdigest()[:32]


def _key_lock(key: str) -> _KeyLock:
    """Return a per-key lock, creating one if needed. Caller must hold a strong
    reference for the duration of the critical section — the WeakValueDictionary
    will GC the lock once the caller's local variable goes out of scope."""
    with _key_locks_lock:
        lock = _key_locks.get(key)
        if lock is None:
            lock = _KeyLock()
            _key_locks[key] = lock
        return lock


def _fetch_table_from_url(
    url: str,
    table_no: int,
    use_full_page: bool = False,
    run_cache=None,
    if_none_match: str | None = None,
    if_modified_since: str | None = None,
) -> dict:
    """Fetch page, extract table at table_no. Returns dict with table_no, num_tables, html or error.
    Default: use Wikipedia REST API (content-only). If use_full_page=True, use the original page URL
    so table indices match the full Wikipedia page (nav/sidebar included).
    run_cache: optional RunPageCache for within-run dedup.
    if_none_match / if_modified_since: conditional GET headers — if the server returns 304,
    returns {"not_modified": True} instead of re-downloading the page.
    """
    if use_full_page:
        fetch_url = url
    else:
        fetch_url = wiki_url_to_rest_html_url(url) or url

    # Check run-level in-memory cache before HTTP
    if run_cache is not None:
        cached_html = run_cache.get(fetch_url)
        if cached_html is not None:
            soup = BeautifulSoup(cached_html, "html.parser")
            tables = soup.find_all("table")
            num_tables = len(tables)
            if not (1 <= table_no <= num_tables):
                return {"error": f"Table {table_no} not found (page has {num_tables} tables)"}
            return {
                "table_no": table_no,
                "num_tables": num_tables,
                "html": str(tables[table_no - 1]),
            }

    try:
        headers: dict[str, str] = {}
        if if_none_match:
            headers["If-None-Match"] = if_none_match
        if if_modified_since:
            headers["If-Modified-Since"] = if_modified_since
        resp = wiki_session().get(fetch_url, timeout=TIMEOUT, headers=headers)
        if resp.status_code == 304:
            return {"not_modified": True}
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}"}
        html_content = resp.text
        if run_cache is not None:
            run_cache.set(fetch_url, html_content)
    except Exception as e:
        return {"error": str(e)}
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all("table")
    num_tables = len(tables)
    if not (1 <= table_no <= num_tables):
        return {"error": f"Table {table_no} not found (page has {num_tables} tables)"}
    target = tables[table_no - 1]
    result: dict = {"table_no": table_no, "num_tables": num_tables, "html": str(target)}
    etag = resp.headers.get("ETag")
    last_modified = resp.headers.get("Last-Modified")
    if etag:
        result["etag"] = etag
    if last_modified:
        result["last_modified"] = last_modified
    return result


def write_table_html_cache(
    url: str,
    table_no: int,
    html: str,
    num_tables: int,
    use_full_page: bool = False,
) -> None:
    """
    Write table HTML directly into the disk cache without an HTTP fetch.
    Used by the AI office builder to prime the cache from already-fetched page HTML,
    so retry validations never re-fetch Wikipedia.
    """
    url = (url or "").strip()
    if not url or not html:
        return
    key = _cache_key(url, table_no, use_full_page)
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{key}.json.gz"
    key_lock = _key_lock(key)
    with key_lock:
        try:
            with gzip.open(cache_path, "wt", encoding="utf-8") as f:
                json.dump(
                    {"table_no": table_no, "num_tables": num_tables, "html": html},
                    f,
                )
        except OSError as e:
            logger.warning("write_table_html_cache: failed to write %s: %s", cache_path, e)


def get_table_html_cached(
    url: str,
    table_no: int = 1,
    refresh: bool = False,
    use_full_page: bool = False,
    run_cache=None,
    max_age_seconds: int | None = None,
) -> dict:
    """
    Return table HTML for (url, table_no). Uses local cache unless refresh=True or cache miss.
    Default: fetch via Wikipedia REST API. use_full_page=True: fetch full page URL (table indices match full page).
    max_age_seconds: if set and the cached file is older than this many seconds, treat as a miss
    and re-fetch. Prevents stale cached pages from masking Wikipedia changes across runs.
    Returns {"table_no", "num_tables", "html": "<table>...</table>"} or {"error": "..."}.
    """

    url = (url or "").strip()
    if not url:
        return {"error": "No URL"}
    key = _cache_key(url, table_no, use_full_page)
    cache_dir = _cache_dir()
    cache_path = cache_dir / f"{key}.json.gz"
    key_lock = _key_lock(key)
    with key_lock:
        cached_data: dict | None = None
        if not refresh and cache_path.exists():
            try:
                with gzip.open(cache_path, "rt", encoding="utf-8") as f:
                    cached_data = json.load(f)
                if not (isinstance(cached_data, dict) and "html" in cached_data and "table_no" in cached_data):
                    cached_data = None
            except (OSError, json.JSONDecodeError, KeyError):
                cached_data = None

        if cached_data is not None:
            cache_age = time.time() - cache_path.stat().st_mtime
            cache_too_old = max_age_seconds is not None and cache_age > max_age_seconds

            if not cache_too_old:
                # Cache is fresh — serve without any HTTP request.
                return {
                    "table_no": cached_data["table_no"],
                    "num_tables": cached_data.get("num_tables", 0),
                    "html": cached_data["html"],
                    "cache_file": str(cache_path),
                }

            # Cache is stale — do a conditional GET using stored ETag / Last-Modified.
            # 304 Not Modified means Wikipedia hasn't changed; reset the TTL clock and reuse HTML.
            logger.debug(
                "Cache stale (%.0fh old): %s — sending conditional GET", cache_age / 3600, url
            )
            result = _fetch_table_from_url(
                url,
                table_no,
                use_full_page,
                run_cache=run_cache,
                if_none_match=cached_data.get("etag"),
                if_modified_since=cached_data.get("last_modified"),
            )
            if result.get("not_modified"):
                # Page unchanged — touch cache file to reset TTL, return cached HTML.
                logger.debug("304 Not Modified: %s — cache TTL reset", url)
                try:
                    cache_path.touch()
                except OSError:
                    pass
                return {
                    "table_no": cached_data["table_no"],
                    "num_tables": cached_data.get("num_tables", 0),
                    "html": cached_data["html"],
                    "cache_file": str(cache_path),
                }
            if "error" in result:
                # Conditional GET failed — fall back to cached HTML to avoid breaking the run.
                logger.warning("Conditional GET failed for %s (%s) — using stale cache", url, result["error"])
                return {
                    "table_no": cached_data["table_no"],
                    "num_tables": cached_data.get("num_tables", 0),
                    "html": cached_data["html"],
                    "cache_file": str(cache_path),
                }
            # 200 with new content — fall through to cache update below.
        else:
            # No usable cache — plain fetch.
            result = _fetch_table_from_url(url, table_no, use_full_page, run_cache=run_cache)
            if "error" in result:
                return result

        # Write (or overwrite) the cache file with fresh content + validator headers.
        cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            with gzip.open(cache_path, "wt", encoding="utf-8") as f:
                payload: dict = {
                    "table_no": result["table_no"],
                    "num_tables": result["num_tables"],
                    "html": result["html"],
                }
                if result.get("etag"):
                    payload["etag"] = result["etag"]
                if result.get("last_modified"):
                    payload["last_modified"] = result["last_modified"]
                json.dump(payload, f)
            result["cache_file"] = str(cache_path)
        except OSError as e:
            logger.warning("Failed to write cache %s: %s", cache_path, e)
        return result
