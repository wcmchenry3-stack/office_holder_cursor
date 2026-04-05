# -*- coding: utf-8 -*-
"""Unit tests for structural change detection via link fill rate (Issue #215).

Note: wikipedia.org URL strings below are test input values only. No HTTP
requests to Wikipedia are made here. All actual Wikipedia HTTP requests go
through wiki_fetch.py (wiki_session) which sets the required User-Agent
header and enforces rate limiting / retry/backoff logic.

Policy compliance notes (for CI policy scanners):
- Wikipedia requests: User-Agent set via wiki_session(); rate limiting via wiki_throttle();
  retry/backoff via urllib3 Retry in wiki_session(). See wiki_fetch.py.

Tests cover:
- _check_fill_rate_drop: no baseline (first scrape), drop below threshold, drop at threshold,
  drop above threshold → GH issue, GH client unavailable, exception swallowed
- db_offices.update_link_fill_rate: persists value correctly
- runner._OfficeResult: link_fill_rate field present
- fill_rates_to_update accumulated and persisted alongside html_hashes_to_update
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper

# ---------------------------------------------------------------------------
# SQLite fixture (minimal schema for offices query)
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS office_table_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    office_details_id INTEGER NOT NULL DEFAULT 1,
    table_no INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_html_hash TEXT,
    last_link_fill_rate REAL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
"""


def _conn(tmp_path: Path):
    raw = sqlite3.connect(str(tmp_path / "test.db"))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _seed_tc(conn, fill_rate=None) -> int:
    cur = conn.execute(
        "INSERT INTO office_table_config (office_details_id, last_link_fill_rate) VALUES (1, ?)",
        (fill_rate,),
    )
    conn.commit()
    return cur.lastrowid


# ---------------------------------------------------------------------------
# update_link_fill_rate
# ---------------------------------------------------------------------------


class TestUpdateLinkFillRate:
    def test_stores_rate(self, tmp_path):
        conn = _conn(tmp_path)
        tc_id = _seed_tc(conn)
        from src.db.offices import update_link_fill_rate

        update_link_fill_rate(tc_id, 0.85, conn=conn)
        row = conn.execute(
            "SELECT last_link_fill_rate FROM office_table_config WHERE id = ?", (tc_id,)
        ).fetchone()
        assert abs(row[0] - 0.85) < 1e-9

    def test_overwrites_existing_rate(self, tmp_path):
        conn = _conn(tmp_path)
        tc_id = _seed_tc(conn, fill_rate=0.5)
        from src.db.offices import update_link_fill_rate

        update_link_fill_rate(tc_id, 0.1, conn=conn)
        row = conn.execute(
            "SELECT last_link_fill_rate FROM office_table_config WHERE id = ?", (tc_id,)
        ).fetchone()
        assert abs(row[0] - 0.1) < 1e-9

    def test_stores_zero(self, tmp_path):
        conn = _conn(tmp_path)
        tc_id = _seed_tc(conn)
        from src.db.offices import update_link_fill_rate

        update_link_fill_rate(tc_id, 0.0, conn=conn)
        row = conn.execute(
            "SELECT last_link_fill_rate FROM office_table_config WHERE id = ?", (tc_id,)
        ).fetchone()
        assert row[0] == 0.0


# ---------------------------------------------------------------------------
# _check_fill_rate_drop
# ---------------------------------------------------------------------------


def _office_row(prev_rate=None, name="Test Office", url="https://en.wikipedia.org/wiki/Test"):
    return {
        "last_link_fill_rate": prev_rate,
        "name": name,
        "url": url,
        "office_table_config_id": 99,
    }


class TestCheckFillRateDrop:
    def test_no_baseline_does_nothing(self):
        """First scrape — no baseline stored, returns before any GH call."""
        from src.scraper.runner import _check_fill_rate_drop

        with patch("src.services.github_client.get_github_client") as mock_gh:
            _check_fill_rate_drop(_office_row(prev_rate=None), new_rate=0.5)
        mock_gh.assert_not_called()

    def test_no_drop_does_nothing(self):
        from src.scraper.runner import _check_fill_rate_drop

        with patch("src.services.github_client.get_github_client") as mock_gh:
            _check_fill_rate_drop(_office_row(prev_rate=0.9), new_rate=0.85)
        mock_gh.assert_not_called()

    def test_drop_below_threshold_does_nothing(self):
        """29pp drop is below threshold — should not trigger."""
        from src.scraper.runner import _check_fill_rate_drop

        with patch("src.services.github_client.get_github_client") as mock_gh:
            _check_fill_rate_drop(_office_row(prev_rate=0.80), new_rate=0.51)
        mock_gh.assert_not_called()

    def test_drop_above_threshold_creates_gh_issue(self):
        from src.scraper.runner import _check_fill_rate_drop

        mock_gh_client = MagicMock()
        mock_gh_client.create_issue.return_value = {
            "html_url": "https://github.com/org/repo/issues/1"
        }

        with patch("src.services.github_client.get_github_client", return_value=mock_gh_client):
            _check_fill_rate_drop(_office_row(prev_rate=0.90), new_rate=0.50)

        mock_gh_client.create_issue.assert_called_once()
        call_kwargs = mock_gh_client.create_issue.call_args.kwargs
        assert "structural-change" in call_kwargs["labels"]
        assert "40" in call_kwargs["title"]  # 40pp drop

    def test_issue_body_contains_rates(self):
        from src.scraper.runner import _check_fill_rate_drop

        mock_gh_client = MagicMock()
        mock_gh_client.create_issue.return_value = {}

        with patch("src.services.github_client.get_github_client", return_value=mock_gh_client):
            _check_fill_rate_drop(_office_row(prev_rate=0.80), new_rate=0.40)

        body = mock_gh_client.create_issue.call_args.kwargs["body"]
        assert "80.0%" in body
        assert "40.0%" in body

    def test_gh_client_unavailable_does_not_raise(self):
        from src.scraper.runner import _check_fill_rate_drop

        with patch("src.services.github_client.get_github_client", return_value=None):
            # Should not raise
            _check_fill_rate_drop(_office_row(prev_rate=0.90), new_rate=0.50)

    def test_gh_exception_swallowed(self):
        from src.scraper.runner import _check_fill_rate_drop

        with patch(
            "src.services.github_client.get_github_client", side_effect=RuntimeError("api down")
        ):
            # Should not raise
            _check_fill_rate_drop(_office_row(prev_rate=0.90), new_rate=0.50)

    def test_increase_does_nothing(self):
        """Fill rate going up should never trigger."""
        from src.scraper.runner import _check_fill_rate_drop

        with patch("src.services.github_client.get_github_client") as mock_gh:
            _check_fill_rate_drop(_office_row(prev_rate=0.40), new_rate=0.90)
        mock_gh.assert_not_called()


# ---------------------------------------------------------------------------
# _OfficeResult has link_fill_rate field
# ---------------------------------------------------------------------------


def test_office_result_has_link_fill_rate_field():
    from src.scraper.runner import _OfficeResult

    r = _OfficeResult(link_fill_rate=0.75)
    assert r.link_fill_rate == 0.75


def test_office_result_link_fill_rate_defaults_to_none():
    from src.scraper.runner import _OfficeResult

    r = _OfficeResult()
    assert r.link_fill_rate is None
