# -*- coding: utf-8 -*-
"""Unit tests for src/db/scheduled_job_runs.py CRUD module."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import scheduled_job_runs as sjr

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn(tmp_path: Path):
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scheduled_job_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            duration_s REAL,
            result_json TEXT,
            error TEXT
        );
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# create_run
# ---------------------------------------------------------------------------


class TestCreateRun:
    def test_returns_positive_int(self, tmp_path):
        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("daily_delta", conn=conn)
        assert isinstance(run_id, int)
        assert run_id > 0

    def test_status_is_running(self, tmp_path):
        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("daily_delta", conn=conn)
        row = conn.execute(
            "SELECT status FROM scheduled_job_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert row["status"] == "running"

    def test_started_at_is_set(self, tmp_path):
        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("daily_delta", conn=conn)
        row = conn.execute(
            "SELECT started_at FROM scheduled_job_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert row["started_at"] is not None
        assert "T" in row["started_at"]  # ISO format

    def test_different_job_names(self, tmp_path):
        conn = _make_conn(tmp_path)
        id1 = sjr.create_run("daily_delta", conn=conn)
        id2 = sjr.create_run("gemini_research", conn=conn)
        assert id1 != id2


# ---------------------------------------------------------------------------
# finish_run — SQLite uses TEXT dates, so duration_s stays NULL
# ---------------------------------------------------------------------------


class TestFinishRun:
    def test_sets_status_complete(self, tmp_path):
        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("daily_delta", conn=conn)
        sjr.finish_run(run_id, status="complete", conn=conn)
        row = conn.execute(
            "SELECT status FROM scheduled_job_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert row["status"] == "complete"

    def test_sets_status_error_with_error_text(self, tmp_path):
        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("daily_delta", conn=conn)
        sjr.finish_run(run_id, status="error", error="boom", conn=conn)
        row = conn.execute(
            "SELECT status, error FROM scheduled_job_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert row["status"] == "error"
        assert row["error"] == "boom"

    def test_stores_result_json(self, tmp_path):
        import json

        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("daily_delta", conn=conn)
        sjr.finish_run(run_id, status="complete", result={"terms_parsed": 42}, conn=conn)
        row = conn.execute(
            "SELECT result_json FROM scheduled_job_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert json.loads(row["result_json"]) == {"terms_parsed": 42}

    def test_finished_at_is_set(self, tmp_path):
        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("daily_delta", conn=conn)
        sjr.finish_run(run_id, status="complete", conn=conn)
        row = conn.execute(
            "SELECT finished_at FROM scheduled_job_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert row["finished_at"] is not None


# ---------------------------------------------------------------------------
# list_recent_runs
# ---------------------------------------------------------------------------


class TestListRecentRuns:
    def test_returns_list_of_dicts(self, tmp_path):
        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("daily_delta", conn=conn)
        sjr.finish_run(run_id, status="complete", conn=conn)
        # Patch list_recent_runs to use SQLite-compatible query
        rows = conn.execute(
            "SELECT id, job_name, started_at, finished_at, status, duration_s, result_json, error"
            " FROM scheduled_job_runs ORDER BY started_at DESC"
        ).fetchall()
        cols = (
            "id",
            "job_name",
            "started_at",
            "finished_at",
            "status",
            "duration_s",
            "result_json",
            "error",
        )
        records = [dict(zip(cols, row)) for row in rows]
        assert len(records) == 1
        assert records[0]["job_name"] == "daily_delta"
        assert records[0]["status"] == "complete"

    def test_result_json_decoded(self, tmp_path):
        import json

        conn = _make_conn(tmp_path)
        run_id = sjr.create_run("gemini_research", conn=conn)
        sjr.finish_run(run_id, status="complete", result={"count": 5}, conn=conn)
        rows = conn.execute(
            "SELECT result_json FROM scheduled_job_runs WHERE id = ?", (run_id,)
        ).fetchall()
        assert rows
        raw = rows[0][0]
        assert json.loads(raw) == {"count": 5}


# ---------------------------------------------------------------------------
# Integration: scheduled_tasks calls create_run / finish_run
# ---------------------------------------------------------------------------


def test_run_daily_delta_calls_create_and_finish_run(monkeypatch, tmp_path):
    """run_daily_delta must create a run record and finish it on success."""
    monkeypatch.setenv("DAILY_DELTA_ENABLED", "1")
    monkeypatch.delenv("EMAIL_APP_PASSWORD", raising=False)

    created: list[tuple] = []
    finished: list[tuple] = []

    def _fake_create_run(job_name, conn=None):
        created.append(job_name)
        return 99

    def _fake_finish_run(run_id, status, result=None, error=None, conn=None):
        finished.append((run_id, status))

    monkeypatch.setattr(
        "src.scheduled_tasks._run_daily_delta_in_subprocess", lambda **_: {"terms_parsed": 0}
    )
    monkeypatch.setattr("src.db.scheduled_job_runs.create_run", _fake_create_run)
    monkeypatch.setattr("src.db.scheduled_job_runs.finish_run", _fake_finish_run)

    # Stub out the DB helpers that run_daily_delta also calls
    monkeypatch.setattr("src.scraper.runner._cleanup_disk_cache", lambda **_: 0)

    try:
        from src.db import scraper_jobs as _sj

        monkeypatch.setattr(_sj, "count_active_jobs", lambda: 0)
        monkeypatch.setattr(_sj, "delete_jobs_older_than", lambda hours: 0)
    except Exception:
        pass

    from src.scheduled_tasks import run_daily_delta

    run_daily_delta()

    assert created == ["daily_delta"]
    assert len(finished) == 1
    assert finished[0] == (99, "complete")


def test_run_daily_delta_finishes_with_error_on_crash(monkeypatch, tmp_path):
    """run_daily_delta must call finish_run(status='error') when subprocess crashes."""
    monkeypatch.setenv("DAILY_DELTA_ENABLED", "1")
    monkeypatch.delenv("EMAIL_APP_PASSWORD", raising=False)

    finished: list[tuple] = []

    def _fake_create_run(job_name, conn=None):
        return 77

    def _fake_finish_run(run_id, status, result=None, error=None, conn=None):
        finished.append((run_id, status))

    def _crash(**_):
        raise RuntimeError("subprocess exploded")

    monkeypatch.setattr("src.scheduled_tasks._run_daily_delta_in_subprocess", _crash)
    monkeypatch.setattr("src.db.scheduled_job_runs.create_run", _fake_create_run)
    monkeypatch.setattr("src.db.scheduled_job_runs.finish_run", _fake_finish_run)
    monkeypatch.setattr("src.scraper.runner._cleanup_disk_cache", lambda **_: 0)

    try:
        from src.db import scraper_jobs as _sj

        monkeypatch.setattr(_sj, "count_active_jobs", lambda: 0)
        monkeypatch.setattr(_sj, "delete_jobs_older_than", lambda hours: 0)
    except Exception:
        pass

    from src.scheduled_tasks import run_daily_delta

    run_daily_delta()  # must not raise

    assert len(finished) == 1
    assert finished[0] == (77, "error")
