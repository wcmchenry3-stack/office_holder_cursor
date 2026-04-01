# -*- coding: utf-8 -*-
"""
Unit tests for Feature F: Job Queue.

Tests cover:
- DB CRUD: enqueue_job, pop_next_queued_job, count_queued_jobs, count_active_jobs
- Router: api_run queues when busy, rejects when queue full, starts immediately when idle
- Worker: _maybe_start_next_queued_job drains queue on job completion
- Scheduler: run_daily_delta skips when active jobs exist
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper, get_connection
from src.db import scraper_jobs as db_scraper_jobs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn(tmp_path: Path):
    """Create an in-memory SQLite DB with the scraper_jobs schema."""
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scraper_jobs (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            queued_at TEXT,
            job_params_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            result_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scraper_jobs_status ON scraper_jobs(status);
        """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# CRUD tests
# ---------------------------------------------------------------------------


class TestEnqueueJob:
    def test_enqueue_creates_queued_record(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_scraper_jobs.enqueue_job("jid1", "delta", '{"mode":"delta"}', conn=conn)
        row = conn.execute(
            "SELECT status, job_params_json FROM scraper_jobs WHERE id = ?", ("jid1",)
        ).fetchone()
        assert row["status"] == "queued"
        assert json.loads(row["job_params_json"]) == {"mode": "delta"}

    def test_enqueue_sets_queued_at(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_scraper_jobs.enqueue_job("jid2", "full", "{}", conn=conn)
        row = conn.execute("SELECT queued_at FROM scraper_jobs WHERE id = ?", ("jid2",)).fetchone()
        assert row["queued_at"] is not None


class TestCountQueuedJobs:
    def test_empty_queue(self, tmp_path):
        conn = _make_conn(tmp_path)
        assert db_scraper_jobs.count_queued_jobs(conn=conn) == 0

    def test_counts_queued_only(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_scraper_jobs.enqueue_job("q1", "delta", "{}", conn=conn)
        db_scraper_jobs.enqueue_job("q2", "delta", "{}", conn=conn)
        db_scraper_jobs.create_job("r1", "delta", conn=conn)  # running
        assert db_scraper_jobs.count_queued_jobs(conn=conn) == 2

    def test_does_not_count_running(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_scraper_jobs.create_job("r1", "delta", conn=conn)
        assert db_scraper_jobs.count_queued_jobs(conn=conn) == 0


class TestCountActiveJobs:
    def test_counts_running_and_queued(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_scraper_jobs.create_job("r1", "delta", conn=conn)  # running
        db_scraper_jobs.enqueue_job("q1", "delta", "{}", conn=conn)  # queued
        assert db_scraper_jobs.count_active_jobs(conn=conn) == 2

    def test_does_not_count_complete(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_scraper_jobs.create_job("r1", "delta", conn=conn)
        db_scraper_jobs.update_job("r1", "complete", conn=conn)
        assert db_scraper_jobs.count_active_jobs(conn=conn) == 0


class TestPopNextQueuedJob:
    def test_returns_none_when_empty(self, tmp_path):
        conn = _make_conn(tmp_path)
        assert db_scraper_jobs.pop_next_queued_job(conn=conn) is None

    def test_pops_oldest_first(self, tmp_path):
        conn = _make_conn(tmp_path)
        # Insert two queued jobs with different queued_at
        now = db_scraper_jobs._now_iso()
        conn.execute(
            "INSERT INTO scraper_jobs (id, type, status, queued_at, job_params_json, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("old", "delta", "queued", "2000-01-01T00:00:00Z", '{"order":1}', now, now),
        )
        conn.execute(
            "INSERT INTO scraper_jobs (id, type, status, queued_at, job_params_json, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("new", "delta", "queued", "2000-01-02T00:00:00Z", '{"order":2}', now, now),
        )
        conn.commit()
        result = db_scraper_jobs.pop_next_queued_job(conn=conn)
        assert result["id"] == "old"

    def test_marks_popped_job_as_running(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_scraper_jobs.enqueue_job("q1", "delta", '{"mode":"delta"}', conn=conn)
        db_scraper_jobs.pop_next_queued_job(conn=conn)
        row = conn.execute("SELECT status FROM scraper_jobs WHERE id = ?", ("q1",)).fetchone()
        assert row["status"] == "running"

    def test_returns_job_params(self, tmp_path):
        conn = _make_conn(tmp_path)
        params = {"mode": "full", "run_bio": True}
        db_scraper_jobs.enqueue_job("q1", "full", json.dumps(params), conn=conn)
        result = db_scraper_jobs.pop_next_queued_job(conn=conn)
        assert result["id"] == "q1"
        assert json.loads(result["job_params_json"]) == params

    def test_decrements_queue_count(self, tmp_path):
        conn = _make_conn(tmp_path)
        db_scraper_jobs.enqueue_job("q1", "delta", "{}", conn=conn)
        db_scraper_jobs.enqueue_job("q2", "delta", "{}", conn=conn)
        db_scraper_jobs.pop_next_queued_job(conn=conn)
        assert db_scraper_jobs.count_queued_jobs(conn=conn) == 1


# ---------------------------------------------------------------------------
# Router tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_job_store():
    """Clear in-memory job store between tests."""
    from src.routers import run_scraper as rs

    with rs._run_job_lock:
        rs._run_job_store.clear()
    yield
    with rs._run_job_lock:
        rs._run_job_store.clear()


def _run_api_run_in_thread(rs, **kwargs):
    """Run the unwrapped api_run coroutine in a fresh OS thread with its own event loop.

    Patches applied BEFORE calling this helper (using patch.object) are module-level
    and therefore visible across threads.
    """
    import asyncio
    import concurrent.futures

    mock_request = MagicMock()
    mock_request.state = MagicMock()

    defaults = dict(
        run_mode="delta",
        individual_ref="",
        office_category_id="",
        force_overwrite="",
        living_only="",
        valid_page_paths_only="",
    )
    defaults.update(kwargs)

    # Use __wrapped__ to bypass the slowapi rate-limiter decorator
    unwrapped = getattr(rs.api_run, "__wrapped__", rs.api_run)

    def _thread_body():
        # Create a fresh event loop for this thread — asyncio.run() raises
        # RuntimeError if called inside a running loop (e.g. pytest-anyio).
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(unwrapped(request=mock_request, **defaults))
        finally:
            loop.close()
            asyncio.set_event_loop(None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_thread_body)
        return future.result(timeout=15)


class TestApiRunQueueBehavior:
    """Test the queue branching logic in api_run by calling it directly."""

    def test_starts_immediately_when_idle(self):
        """No running job -> starts immediately, returns queued=False."""
        import src.routers.run_scraper as rs

        # Patch _run_job_worker so the thread finishes quickly;
        # patch _maybe_start_next_queued_job so its pop_next_queued_job DB call is avoided.
        with (
            patch.object(db_scraper_jobs, "create_job"),
            patch.object(rs, "_run_job_worker"),
            patch.object(rs, "_maybe_start_next_queued_job"),
        ):
            resp = _run_api_run_in_thread(rs)

        assert resp.status_code == 202
        body = json.loads(resp.body)
        assert "job_id" in body
        assert body.get("queued") is False

    def test_queues_when_job_running(self):
        """Running job present -> enqueues and returns queued=True."""
        import src.routers.run_scraper as rs

        with rs._run_job_lock:
            rs._run_job_store["existing"] = {
                "status": "running",
                "_created_at": time.monotonic(),
            }

        with (
            patch.object(db_scraper_jobs, "count_queued_jobs", return_value=0),
            patch.object(db_scraper_jobs, "enqueue_job") as mock_enqueue,
        ):
            resp = _run_api_run_in_thread(rs)

        assert resp.status_code == 202
        body = json.loads(resp.body)
        assert body.get("queued") is True
        assert "job_id" in body
        mock_enqueue.assert_called_once()

    def test_queue_full_returns_not_queued(self):
        """Running job + 1 in queue -> returns queued=False, reason=queue_full."""
        import src.routers.run_scraper as rs

        with rs._run_job_lock:
            rs._run_job_store["existing"] = {
                "status": "running",
                "_created_at": time.monotonic(),
            }

        with (
            patch.object(db_scraper_jobs, "count_queued_jobs", return_value=1),
            patch.object(db_scraper_jobs, "enqueue_job") as mock_enqueue,
        ):
            resp = _run_api_run_in_thread(rs)

        assert resp.status_code == 202
        body = json.loads(resp.body)
        assert body.get("queued") is False
        assert body.get("reason") == "queue_full"
        mock_enqueue.assert_not_called()


# ---------------------------------------------------------------------------
# _maybe_start_next_queued_job tests
# ---------------------------------------------------------------------------


class TestMaybeStartNextQueuedJob:
    def test_does_nothing_when_no_queued_jobs(self):
        with patch.object(db_scraper_jobs, "pop_next_queued_job", return_value=None) as mock_pop:
            from src.routers.run_scraper import _maybe_start_next_queued_job

            _maybe_start_next_queued_job()
            mock_pop.assert_called_once()

    def test_starts_worker_for_queued_job(self):
        params = {
            "mode": "delta",
            "run_bio": False,
            "run_office_bio": True,
            "refresh_table_cache": False,
            "max_rows_per_table": None,
            "office_id_list": None,
            "individual_ref": None,
            "individual_id_list": None,
            "force_overwrite": False,
        }
        next_job = {"id": "q1", "type": "delta", "job_params_json": json.dumps(params)}
        with (
            patch.object(db_scraper_jobs, "pop_next_queued_job", return_value=next_job),
            patch("threading.Thread") as mock_thread,
        ):
            mock_thread.return_value = MagicMock()
            from src.routers.run_scraper import (
                _maybe_start_next_queued_job,
                _run_job_store,
                _run_job_lock,
            )

            _maybe_start_next_queued_job()

            mock_thread.assert_called_once()
            with _run_job_lock:
                assert "q1" in _run_job_store
                assert _run_job_store["q1"]["status"] == "running"

    def test_bad_json_params_does_not_crash(self):
        next_job = {"id": "q1", "type": "delta", "job_params_json": "INVALID JSON!!!"}
        with (
            patch.object(db_scraper_jobs, "pop_next_queued_job", return_value=next_job),
            patch("threading.Thread") as mock_thread,
        ):
            mock_thread.return_value = MagicMock()
            from src.routers.run_scraper import _maybe_start_next_queued_job

            # Should not raise
            _maybe_start_next_queued_job()
            mock_thread.assert_called_once()


# ---------------------------------------------------------------------------
# Scheduler tests
# ---------------------------------------------------------------------------


class TestSchedulerSkipsWhenActive:
    def test_skips_when_active_jobs(self, monkeypatch):
        """run_daily_delta should skip when active jobs exist."""
        from src.scheduled_tasks import run_daily_delta

        monkeypatch.setenv("DAILY_DELTA_ENABLED", "1")
        with (
            patch.object(db_scraper_jobs, "count_active_jobs", return_value=1),
            patch("src.scheduled_tasks._run_daily_delta_in_subprocess") as mock_run,
        ):
            run_daily_delta()
            mock_run.assert_not_called()

    def test_runs_when_no_active_jobs(self, monkeypatch):
        """run_daily_delta should proceed when no active jobs exist."""
        from src.scheduled_tasks import run_daily_delta

        monkeypatch.setenv("DAILY_DELTA_ENABLED", "1")
        fake_result = {"offices_processed": 1, "individuals_added": 0, "errors": []}
        with (
            patch.object(db_scraper_jobs, "count_active_jobs", return_value=0),
            patch("src.scheduled_tasks._run_daily_delta_in_subprocess", return_value=fake_result),
            patch("src.scheduled_tasks._send_summary_email"),
            patch("src.scraper.runner._cleanup_disk_cache", return_value=0),
            patch.object(db_scraper_jobs, "delete_jobs_older_than", return_value=0),
        ):
            run_daily_delta()
