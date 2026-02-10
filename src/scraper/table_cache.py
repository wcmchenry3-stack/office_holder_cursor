# -*- coding: utf-8 -*-
"""
Local cache for Wikipedia table HTML. One file per (url, table_no); raw HTML stored in gzipped JSON.
Preview / test / run use cache by default; use Refresh to refetch from Wikipedia.
"""

import gzip
import hashlib
import json
import threading
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from src.scraper.wiki_fetch import WIKIPEDIA_REQUEST_HEADERS, wiki_url_to_rest_html_url

TIMEOUT = 30
CACHE_DIR_NAME = "wiki_cache"
_LOCK = threading.Lock()
_key_locks: dict[str, threading.Lock] = {}
_key_locks_lock = threading.Lock()


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _cache_dir() -> Path:
    return _project_root() / "data" / CACHE_DIR_NAME


def _cache_key(url: str, table_no: int, use_full_page: bool = False) -> str:
    normalized = (url.strip() + "|" + str(table_no) + "|" + ("1" if use_full_page else "0")).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()[:32]


def _key_lock(key: str) -> threading.Lock:
    with _key_locks_lock:
        if key not in _key_locks:
            _key_locks[key] = threading.Lock()
        return _key_locks[key]


def _fetch_table_from_url(url: str, table_no: int, use_full_page: bool = False) -> dict:
    """Fetch page, extract table at table_no. Returns dict with table_no, num_tables, html or error.
    Default: use Wikipedia REST API (content-only). If use_full_page=True, use the original page URL
    so table indices match the full Wikipedia page (nav/sidebar included)."""
    if use_full_page:
        fetch_url = url
    else:
        fetch_url = wiki_url_to_rest_html_url(url) or url
    try:
        resp = requests.get(fetch_url, headers=WIKIPEDIA_REQUEST_HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}"}
        html_content = resp.text
    except requests.RequestException as e:
        return {"error": str(e)}
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all("table")
    num_tables = len(tables)
    if not (1 <= table_no <= num_tables):
        return {"error": f"Table {table_no} not found (page has {num_tables} tables)"}
    target = tables[table_no - 1]
    return {"table_no": table_no, "num_tables": num_tables, "html": str(target)}


def get_table_html_cached(url: str, table_no: int = 1, refresh: bool = False, use_full_page: bool = False) -> dict:
    """
    Return table HTML for (url, table_no). Uses local cache unless refresh=True or cache miss.
    Default: fetch via Wikipedia REST API. use_full_page=True: fetch full page URL (table indices match full page).
    Returns {"table_no", "num_tables", "html": "<table>...</table>"} or {"error": "..."}.
    """
    # #region agent log
    _debug_log = lambda loc, msg, data: (_open := open(Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log", "a", encoding="utf-8"), _open.write(json.dumps({"location": loc, "message": msg, "data": data, "timestamp": __import__("time").time() * 1000}) + "\n"), _open.close())
    # #endregion
    url = (url or "").strip()
    if not url:
        return {"error": "No URL"}
    key = _cache_key(url, table_no, use_full_page)
    cache_dir = _cache_dir()
    cache_path = cache_dir / f"{key}.json.gz"
    # #region agent log
    _debug_log("table_cache.py:get_table_html_cached:entry", "get_table_html_cached called", {"url": url[:80], "table_no": table_no, "refresh": refresh, "use_full_page": use_full_page, "key": key, "cache_path": str(cache_path), "hypothesisId": "H1"})
    # #endregion
    key_lock = _key_lock(key)
    with key_lock:
        if not refresh:
            if cache_path.exists():
                try:
                    with gzip.open(cache_path, "rt", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, dict) and "html" in data and "table_no" in data:
                        # #region agent log
                        _debug_log("table_cache.py:get_table_html_cached:hit", "cache hit, returning", {"key": key, "hypothesisId": "H2"})
                        # #endregion
                        return {
                            "table_no": data["table_no"],
                            "num_tables": data.get("num_tables", 0),
                            "html": data["html"],
                        }
                except (OSError, json.JSONDecodeError, KeyError) as e:
                    # #region agent log
                    _debug_log("table_cache.py:get_table_html_cached:read_fail", "cache file exists but read failed", {"key": key, "error": str(e), "hypothesisId": "H4"})
                    # #endregion
                    pass
            else:
                # #region agent log
                _debug_log("table_cache.py:get_table_html_cached:miss", "cache file does not exist", {"key": key, "hypothesisId": "H2"})
                # #endregion
        result = _fetch_table_from_url(url, table_no, use_full_page)
        # #region agent log
        _debug_log("table_cache.py:get_table_html_cached:after_fetch", "after _fetch_table_from_url", {"has_error": "error" in result, "hypothesisId": "H2"})
        # #endregion
        if "error" in result:
            return result
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"{key}.json.gz"
        write_ok = False
        try:
            with gzip.open(cache_path, "wt", encoding="utf-8") as f:
                json.dump(
                    {
                        "table_no": result["table_no"],
                        "num_tables": result["num_tables"],
                        "html": result["html"],
                    },
                    f,
                )
            result["cache_file"] = str(cache_path)
            write_ok = True
        except OSError as e:
            # #region agent log
            _debug_log("table_cache.py:get_table_html_cached:write_fail", "cache write failed", {"key": key, "error": str(e), "hypothesisId": "H2"})
            # #endregion
            pass
        # #region agent log
        _debug_log("table_cache.py:get_table_html_cached:write_done", "cache write result", {"key": key, "write_ok": write_ok, "hypothesisId": "H2"})
        # #endregion
        return result
