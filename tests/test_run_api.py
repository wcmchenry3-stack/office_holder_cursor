# -*- coding: utf-8 -*-
"""
HTTP API lifecycle tests for the run scraper endpoints.

Uses FastAPI TestClient (starlette) with run_with_db monkeypatched to a fast stub.
Tests the full HTTP layer: POST /api/run → GET /api/run/status → POST /api/run/cancel.
No network or DB writes are required.
"""
from __future__ import annotations

import time
import threading

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """
    Build a TestClient around the FastAPI app with a temp SQLite DB.
    Auth is bypassed when GOOGLE_CLIENT_ID is not set (see main.py).
    """
    import os
    from pathlib import Path

    tmp = tmp_path_factory.mktemp("run_api_db")
    db_path = tmp / "test.db"
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    # Ensure wiki cache goes to a temp dir too
    cache_dir = tmp / "wiki_cache"
    cache_dir.mkdir()
    os.environ["WIKI_CACHE_DIR"] = str(cache_dir)

    from src.main import app
    from src.db.connection import init_db
    init_db()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_run_with_db(**kwargs):
    """
    Fast in-process stub for run_with_db that sleeps briefly then returns a result.
    Accepts cancel_check and calls it so cancel tests work.
    """
    cancel_check = kwargs.get("cancel_check")
    progress_callback = kwargs.get("progress_callback")
    if progress_callback:
        progress_callback("office", 0, 1, "Starting…", {})
    # Brief sleep so status polls can observe 'running'
    for _ in range(5):
        time.sleep(0.05)
        if cancel_check and cancel_check():
            return {"cancelled": True, "terms": 0}
        if progress_callback:
            progress_callback("office", 1, 1, "Done", {})
    return {"cancelled": False, "terms": 0}


# ---------------------------------------------------------------------------
# P3.1 Run job lifecycle
# ---------------------------------------------------------------------------


def test_api_run_returns_job_id(client, monkeypatch):
    """POST /api/run returns 202 with a job_id."""
    import src.routers.run_scraper as rs
    monkeypatch.setattr(rs, "run_with_db", _stub_run_with_db)

    resp = client.post("/api/run", data={"run_mode": "delta"})
    assert resp.status_code == 202
    data = resp.json()
    assert "job_id" in data
    assert isinstance(data["job_id"], str)


def test_api_run_status_running_then_complete(client, monkeypatch):
    """Status is 'running' immediately, then 'complete' after the job finishes."""
    import src.routers.run_scraper as rs
    monkeypatch.setattr(rs, "run_with_db", _stub_run_with_db)

    resp = client.post("/api/run", data={"run_mode": "delta"})
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    # Poll: should see 'running' at least once
    seen_running = False
    for _ in range(30):
        s = client.get(f"/api/run/status/{job_id}")
        assert s.status_code == 200
        if s.json()["status"] == "running":
            seen_running = True
        elif s.json()["status"] in ("complete", "cancelled", "error"):
            break
        time.sleep(0.1)

    final = client.get(f"/api/run/status/{job_id}").json()
    assert final["status"] == "complete", f"Expected complete, got: {final['status']}"


def test_api_run_status_unknown_id_returns_404(client):
    """GET /api/run/status/<unknown> returns 404."""
    resp = client.get("/api/run/status/nonexistent-job-id-xyz")
    assert resp.status_code == 404


def test_api_run_cancel_sets_cancelled_status(client, monkeypatch):
    """POST /api/run/cancel/{job_id} cancels a running job."""
    import src.routers.run_scraper as rs

    def _slow_run(**kwargs):
        cancel_check = kwargs.get("cancel_check")
        for _ in range(100):
            time.sleep(0.05)
            if cancel_check and cancel_check():
                return {"cancelled": True, "terms": 0}
        return {"cancelled": False, "terms": 0}

    monkeypatch.setattr(rs, "run_with_db", _slow_run)

    resp = client.post("/api/run", data={"run_mode": "delta"})
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    # Wait until job is confirmed running
    for _ in range(20):
        s = client.get(f"/api/run/status/{job_id}")
        if s.json()["status"] == "running":
            break
        time.sleep(0.05)

    cancel_resp = client.post(f"/api/run/cancel/{job_id}")
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["ok"] is True

    # Wait for worker to see cancel flag
    for _ in range(20):
        s = client.get(f"/api/run/status/{job_id}")
        if s.json()["status"] == "cancelled":
            break
        time.sleep(0.1)

    assert client.get(f"/api/run/status/{job_id}").json()["status"] == "cancelled"


def test_api_run_cancel_nonexistent_job_returns_404(client):
    """POST /api/run/cancel/<unknown> returns 404."""
    resp = client.post("/api/run/cancel/nonexistent-job-id-xyz")
    assert resp.status_code == 404


def test_job_store_eviction_removes_old_completed_jobs(client, monkeypatch):
    """Completed jobs older than TTL are removed when a new job is created."""
    import src.routers.run_scraper as rs

    def _instant_run(**kwargs):
        progress_callback = kwargs.get("progress_callback")
        if progress_callback:
            progress_callback("office", 1, 1, "Done", {})
        return {"cancelled": False, "terms": 0}

    monkeypatch.setattr(rs, "run_with_db", _instant_run)

    # Create a job and let it complete
    resp = client.post("/api/run", data={"run_mode": "delta"})
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    # Wait for completion
    for _ in range(30):
        s = client.get(f"/api/run/status/{job_id}")
        if s.json()["status"] != "running":
            break
        time.sleep(0.1)

    # Backdate the job's _created_at so it looks old
    import time as _time
    with rs._run_job_lock:
        if job_id in rs._run_job_store:
            rs._run_job_store[job_id]["_created_at"] = _time.monotonic() - (3 * 3600)

    # Creating a new job should trigger eviction
    resp2 = client.post("/api/run", data={"run_mode": "delta"})
    assert resp2.status_code == 202

    # The old job should have been evicted from the in-memory store
    with rs._run_job_lock:
        assert job_id not in rs._run_job_store

    # But the job is still accessible via the DB fallback (returns 200 with completed status)
    status_resp = client.get(f"/api/run/status/{job_id}")
    assert status_resp.status_code == 200
    assert status_resp.json()["status"] == "complete"
