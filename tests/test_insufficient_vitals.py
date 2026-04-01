# -*- coding: utf-8 -*-
"""
Unit tests for Feature E: Insufficient Vitals Batch Tracking.

Tests cover:
- DB CRUD: get_insufficient_vitals_individuals_for_batch, mark_insufficient_vitals_checked
- Batch filtering: id%30, dead-link exclusion, No-link exclusion, 30-day cooldown
- Runner: delta_insufficient_vitals dispatch, correct batch, marks checked on success + error

Wikipedia URLs in fixtures are static test data only — no live requests are made.
All live Wikimedia API calls go through wiki_fetch.py which sets the User-Agent header
per Wikimedia etiquette and enforces rate_limit / throttle via wiki_throttle().
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import individuals as db_individuals

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW_ISO = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
_OLD_ISO = (datetime.now(timezone.utc) - timedelta(days=31)).strftime("%Y-%m-%dT%H:%M:%SZ")
_RECENT_ISO = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_conn(tmp_path: Path):
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS individuals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wiki_url TEXT NOT NULL UNIQUE,
            page_path TEXT,
            full_name TEXT,
            birth_date TEXT,
            death_date TEXT,
            birth_date_imprecise INTEGER NOT NULL DEFAULT 0,
            death_date_imprecise INTEGER NOT NULL DEFAULT 0,
            birth_place TEXT,
            death_place TEXT,
            is_dead_link INTEGER NOT NULL DEFAULT 0,
            is_living INTEGER NOT NULL DEFAULT 1,
            bio_batch INTEGER NOT NULL DEFAULT 0,
            bio_refreshed_at TEXT,
            insufficient_vitals_checked_at TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        """)
    conn.commit()
    return conn


def _insert(
    conn,
    id: int,
    wiki_url: str,
    birth_date=None,
    death_date=None,
    is_living=1,
    is_dead_link=0,
    insuf_checked_at=None,
):
    conn.execute(
        "INSERT INTO individuals (id, wiki_url, birth_date, death_date, is_living,"
        " is_dead_link, insufficient_vitals_checked_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (id, wiki_url, birth_date, death_date, is_living, is_dead_link, insuf_checked_at),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# get_insufficient_vitals_individuals_for_batch
# ---------------------------------------------------------------------------


class TestGetInsufficientVitalsBatch:
    def test_returns_matching_individual(self, tmp_path):
        conn = _make_conn(tmp_path)
        # id=30 → 30%30=0, batch 0
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A")
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert len(rows) == 1
        assert rows[0]["wiki_url"] == "https://en.wikipedia.org/wiki/A"

    def test_batch_filtering_by_id_mod_30(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://en.wikipedia.org/wiki/Batch1")  # 1%30=1
        _insert(conn, 2, "https://en.wikipedia.org/wiki/Batch2")  # 2%30=2
        _insert(conn, 30, "https://en.wikipedia.org/wiki/Batch0")  # 30%30=0
        assert len(db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)) == 1
        assert len(db_individuals.get_insufficient_vitals_individuals_for_batch(1, conn=conn)) == 1
        assert len(db_individuals.get_insufficient_vitals_individuals_for_batch(2, conn=conn)) == 1

    def test_excludes_if_birth_date_present(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", birth_date="1950-01-01")
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert rows == []

    def test_excludes_dead_links(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", is_dead_link=1)
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert rows == []

    def test_excludes_no_link_urls(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "No link: John Smith")
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert rows == []

    def test_excludes_recently_checked(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", insuf_checked_at=_RECENT_ISO)
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert rows == []

    def test_includes_never_checked(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", insuf_checked_at=None)
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert len(rows) == 1

    def test_includes_stale_checked(self, tmp_path):
        """Checked > 30 days ago should be included again."""
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", insuf_checked_at=_OLD_ISO)
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert len(rows) == 1

    def test_includes_dead_with_no_death_date(self, tmp_path):
        """is_living=0 but death_date IS NULL should be included."""
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", is_living=0, death_date=None)
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert len(rows) == 1

    def test_excludes_dead_with_death_date(self, tmp_path):
        """is_living=0 and death_date set: no missing data, skip."""
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", is_living=0, death_date="2000-01-01")
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert rows == []

    def test_empty_batch(self, tmp_path):
        conn = _make_conn(tmp_path)
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(5, conn=conn)
        assert rows == []

    def test_returns_id_wiki_url_full_name(self, tmp_path):
        conn = _make_conn(tmp_path)
        conn.execute(
            "INSERT INTO individuals (id, wiki_url, full_name, is_living)" " VALUES (?, ?, ?, ?)",
            (30, "https://en.wikipedia.org/wiki/A", "Alice Smith", 1),
        )
        conn.commit()
        rows = db_individuals.get_insufficient_vitals_individuals_for_batch(0, conn=conn)
        assert rows[0]["id"] == 30
        assert rows[0]["full_name"] == "Alice Smith"


# ---------------------------------------------------------------------------
# mark_insufficient_vitals_checked
# ---------------------------------------------------------------------------


class TestMarkInsufficientVitalsChecked:
    def test_sets_timestamp(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://en.wikipedia.org/wiki/A")
        db_individuals.mark_insufficient_vitals_checked(1, conn=conn)
        row = conn.execute(
            "SELECT insufficient_vitals_checked_at FROM individuals WHERE id = ?", (1,)
        ).fetchone()
        assert row[0] is not None

    def test_idempotent(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://en.wikipedia.org/wiki/A")
        db_individuals.mark_insufficient_vitals_checked(1, conn=conn)
        db_individuals.mark_insufficient_vitals_checked(1, conn=conn)
        row = conn.execute(
            "SELECT insufficient_vitals_checked_at FROM individuals WHERE id = ?", (1,)
        ).fetchone()
        assert row[0] is not None

    def test_unknown_id_does_not_crash(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_individuals.mark_insufficient_vitals_checked(9999, conn=conn)  # no-op, no error


# ---------------------------------------------------------------------------
# Runner: delta_insufficient_vitals mode
# ---------------------------------------------------------------------------


class TestRunnerInsufficientVitals:
    def _make_ctx(self, bio_batch=None):
        from src.scraper.runner import _RunContext

        return _RunContext(
            run_mode="delta_insufficient_vitals",
            run_bio=False,
            run_office_bio=False,
            refresh_table_cache=False,
            dry_run=False,
            test_run=False,
            max_rows_per_table=None,
            office_ids=None,
            individual_ref=None,
            individual_ids=None,
            cancel_check=None,
            force_replace_office_ids=None,
            force_overwrite=False,
            bio_batch=bio_batch,
        )

    def test_uses_explicit_batch(self):
        """When bio_batch is set on ctx, that batch number is used."""
        ctx = self._make_ctx(bio_batch=7)
        mock_logger = MagicMock()
        mock_logger.close = MagicMock()
        report = MagicMock()

        with (
            patch.object(
                db_individuals,
                "get_insufficient_vitals_individuals_for_batch",
                return_value=[],
            ) as mock_get,
            patch("src.scraper.runner._fetch_bio_batch", return_value=False),
        ):
            from src.scraper.runner import _run_insufficient_vitals

            _run_insufficient_vitals(ctx, mock_logger, report)
            mock_get.assert_called_once_with(7)

    def test_uses_today_batch_when_none(self):
        """When bio_batch is None, uses date.today().day % 30."""
        from datetime import date

        expected_batch = date.today().day % 30
        ctx = self._make_ctx(bio_batch=None)
        mock_logger = MagicMock()
        mock_logger.close = MagicMock()
        report = MagicMock()

        with (
            patch.object(
                db_individuals,
                "get_insufficient_vitals_individuals_for_batch",
                return_value=[],
            ) as mock_get,
            patch("src.scraper.runner._fetch_bio_batch", return_value=False),
        ):
            from src.scraper.runner import _run_insufficient_vitals

            _run_insufficient_vitals(ctx, mock_logger, report)
            mock_get.assert_called_once_with(expected_batch)

    def test_marks_checked_on_success(self):
        """mark_insufficient_vitals_checked called for each successful bio fetch."""
        ctx = self._make_ctx(bio_batch=0)
        mock_logger = MagicMock()
        mock_logger.close = MagicMock()
        report = MagicMock()

        individuals = [{"id": 30, "wiki_url": "https://en.wikipedia.org/wiki/A", "full_name": "A"}]

        def fake_fetch(urls, biography, cancel_check, progress_cb, success_cb, error_cb):
            for url in urls:
                success_cb(url, {"birth_date": "1950-01-01"})
            return False

        with (
            patch.object(
                db_individuals,
                "get_insufficient_vitals_individuals_for_batch",
                return_value=individuals,
            ),
            patch.object(db_individuals, "upsert_individual", return_value=30),
            patch.object(db_individuals, "mark_insufficient_vitals_checked") as mock_mark,
            patch("src.scraper.runner._fetch_bio_batch", side_effect=fake_fetch),
            patch("src.scraper.runner.normalize_date", return_value=("1950-01-01", False)),
        ):
            from src.scraper.runner import _run_insufficient_vitals

            result = _run_insufficient_vitals(ctx, mock_logger, report)

        mock_mark.assert_called_once_with(30)
        assert result["bio_success_count"] == 1

    def test_marks_checked_on_error(self):
        """mark_insufficient_vitals_checked also called when bio fetch fails."""
        ctx = self._make_ctx(bio_batch=0)
        mock_logger = MagicMock()
        mock_logger.close = MagicMock()
        report = MagicMock()

        individuals = [{"id": 30, "wiki_url": "https://en.wikipedia.org/wiki/A", "full_name": "A"}]

        def fake_fetch(urls, biography, cancel_check, progress_cb, success_cb, error_cb):
            for url in urls:
                error_cb(url, "timeout")
            return False

        with (
            patch.object(
                db_individuals,
                "get_insufficient_vitals_individuals_for_batch",
                return_value=individuals,
            ),
            patch.object(db_individuals, "mark_insufficient_vitals_checked") as mock_mark,
            patch("src.scraper.runner._fetch_bio_batch", side_effect=fake_fetch),
        ):
            from src.scraper.runner import _run_insufficient_vitals

            result = _run_insufficient_vitals(ctx, mock_logger, report)

        mock_mark.assert_called_once_with(30)
        assert result["bio_error_count"] == 1

    def test_empty_batch_returns_zero_counts(self):
        ctx = self._make_ctx(bio_batch=5)
        mock_logger = MagicMock()
        mock_logger.close = MagicMock()
        report = MagicMock()

        with (
            patch.object(
                db_individuals, "get_insufficient_vitals_individuals_for_batch", return_value=[]
            ),
            patch("src.scraper.runner._fetch_bio_batch", return_value=False),
        ):
            from src.scraper.runner import _run_insufficient_vitals

            result = _run_insufficient_vitals(ctx, mock_logger, report)

        assert result["bio_success_count"] == 0
        assert result["bio_error_count"] == 0
        assert result["insufficient_vitals_checked"] == 0

    def test_result_includes_batch_metadata(self):
        ctx = self._make_ctx(bio_batch=3)
        mock_logger = MagicMock()
        mock_logger.close = MagicMock()
        report = MagicMock()

        with (
            patch.object(
                db_individuals, "get_insufficient_vitals_individuals_for_batch", return_value=[]
            ),
            patch("src.scraper.runner._fetch_bio_batch", return_value=False),
        ):
            from src.scraper.runner import _run_insufficient_vitals

            result = _run_insufficient_vitals(ctx, mock_logger, report)

        assert result["insufficient_vitals_batch"] == 3
        assert "insufficient_vitals_checked" in result

    def test_run_with_db_dispatches_mode(self):
        """run_with_db with run_mode='delta_insufficient_vitals' calls _run_insufficient_vitals."""
        with (
            patch("src.scraper.runner._run_insufficient_vitals") as mock_fn,
            patch("src.scraper.runner.init_db"),
        ):
            mock_fn.return_value = {"bio_success_count": 0}
            from src.scraper.runner import run_with_db

            run_with_db(run_mode="delta_insufficient_vitals", bio_batch=0)
            mock_fn.assert_called_once()
