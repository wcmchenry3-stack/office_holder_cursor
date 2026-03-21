"""Shared pytest fixtures available to all test files."""

import sqlite3
from pathlib import Path

import pytest

# Playwright is an optional dev dependency. Skip collection of Playwright tests
# when the package isn't installed (e.g. in CI) rather than erroring at import time.
try:
    import playwright  # noqa: F401
except ImportError:
    collect_ignore_glob = ["src/test_ui_edit_office_playwright.py"]

from src.db.connection import get_connection, init_db


@pytest.fixture
def tmp_db_path(tmp_path):
    """Create a fully initialized SQLite DB at a temp path. Returns the Path."""
    db = tmp_path / "test.db"
    init_db(path=db)
    return db


@pytest.fixture
def tmp_db(tmp_db_path):
    """Open a connection to the temp DB. Yields sqlite3.Connection, closes after test."""
    conn = get_connection(tmp_db_path)
    yield conn
    conn.close()


@pytest.fixture
def load_fixture():
    """Factory fixture: load raw HTML bytes from test_scripts/fixtures/<filename>."""
    fixtures_dir = Path(__file__).parent / "test_scripts" / "fixtures"

    def _load(filename: str) -> bytes:
        return (fixtures_dir / filename).read_bytes()

    return _load
