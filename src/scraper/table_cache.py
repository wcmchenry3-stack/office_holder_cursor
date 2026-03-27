# -*- coding: utf-8 -*-
"""
Local cache for Wikipedia table HTML. One file per (url, table_no); raw HTML stored in gzipped JSON.
Preview / test / run use cache by default; use Refresh to refetch from Wikipedia.
"""

import gzip
import hashlib
import json
import logging
import threading
from pathlib import Path

from bs4 import BeautifulSoup

from src.scraper.wiki_fetch import wiki_session, wiki_url_to_rest_html_url

logger = logging.getLogger(__name__)

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
    normalized = (url.strip() + "|" + str(table_no) + "|" + ("1" if use_full_page else "0")).encode(
        "utf-8"
    )
    return hashlib.sha256(normalized).hexdigest()[:32]


def _key_lock(key: str) -> threading.Lock:
    with _key_locks_lock:
        if key not in _key_locks:
            _key_locks[key] = threading.Lock()
        return _key_locks[key]


def _fetch_table_from_url(
    url: str, table_no: int, use_full_page: bool = False, run_cache=None
) -> dict:
    """Fetch page, extract table at table_no. Returns dict with table_no, num_tables, html or error.
    Default: use Wikipedia REST API (content-only). If use_full_page=True, use the original page URL
    so table indices match the full Wikipedia page (nav/sidebar included).
    run_cache: optional RunPageCache for within-run dedup."""
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
        resp = wiki_session().get(fetch_url, timeout=TIMEOUT)
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
    return {"table_no": table_no, "num_tables": num_tables, "html": str(target)}


def get_table_html_cached(
    url: str,
    table_no: int = 1,
    refresh: bool = False,
    use_full_page: bool = False,
    run_cache=None,
) -> dict:
    """
    Return table HTML for (url, table_no). Uses local cache unless refresh=True or cache miss.
    Default: fetch via Wikipedia REST API. use_full_page=True: fetch full page URL (table indices match full page).
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
        if not refresh:
            if cache_path.exists():
                try:
                    with gzip.open(cache_path, "rt", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, dict) and "html" in data and "table_no" in data:
                        return {
                            "table_no": data["table_no"],
                            "num_tables": data.get("num_tables", 0),
                            "html": data["html"],
                        }
                except (OSError, json.JSONDecodeError, KeyError) as e:
                    pass
            else:
                pass  # cache miss — fall through to fetch
        result = _fetch_table_from_url(url, table_no, use_full_page, run_cache=run_cache)
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
            pass
        return result
