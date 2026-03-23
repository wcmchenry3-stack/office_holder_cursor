"""
Shared pytest helpers and fixtures for integration tests.

Helpers are also importable by unit tests in other directories:
    from tests.conftest import _cache_key, _extract_table, _write_fixture_to_cache
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Standalone helpers (reused across test files)
# ---------------------------------------------------------------------------


def _cache_key(url: str, table_no: int, use_full_page: bool = False) -> str:
    """Match table_cache._cache_key so we write the same key the runner will read."""
    normalized = (url.strip() + "|" + str(table_no) + "|" + ("1" if use_full_page else "0")).encode(
        "utf-8"
    )
    return hashlib.sha256(normalized).hexdigest()[:32]


def _extract_table(html: str, table_no: int) -> tuple[str, int]:
    """Extract the N-th <table> from full-page HTML (1-based). Returns (table_html, num_tables)."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    num_tables = len(tables)
    if not (1 <= table_no <= num_tables):
        raise ValueError(f"Table {table_no} not found (page has {num_tables} tables)")
    return str(tables[table_no - 1]), num_tables


def _write_fixture_to_cache(
    cache_dir: Path,
    url: str,
    table_no: int,
    table_html: str,
    use_full_page: bool = False,
    num_tables: int = 1,
) -> None:
    """Write fixture HTML to wiki_cache so get_table_html_cached hits cache."""
    key = _cache_key(url, table_no, use_full_page)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{key}.json.gz"
    with gzip.open(cache_path, "wt", encoding="utf-8") as f:
        json.dump(
            {"table_no": table_no, "num_tables": num_tables, "html": table_html},
            f,
            ensure_ascii=False,
        )


# ---------------------------------------------------------------------------
# Shared pytest fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_with_cache(tmp_path, monkeypatch):
    """
    Initialise a fresh DB and an empty wiki-cache dir, both in tmp_path.

    Sets OFFICE_HOLDER_DB_PATH and monkeypatches table_cache._cache_dir so
    every table-cache read/write goes to the temp dir.

    Yields (db_path, cache_dir).
    """
    db_path = tmp_path / "test_run.db"
    cache_dir = tmp_path / "wiki_cache"
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))

    from src.db.connection import DB_PATH, init_db
    import src.scraper.table_cache as table_cache_mod

    assert str(db_path) != str(DB_PATH), "test DB must not point at production DB"
    monkeypatch.setattr(table_cache_mod, "_cache_dir", lambda: cache_dir)

    init_db(path=db_path)
    return db_path, cache_dir
