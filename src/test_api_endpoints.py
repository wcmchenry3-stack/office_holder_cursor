"""Tests for the scraper run and table-cache-refresh API endpoints.

Uses FastAPI TestClient (no live server). The DB is initialised into a
temp directory; Datasette startup is suppressed so no subprocess is spawned.

Run: pytest src/test_api_endpoints.py -v
"""

from __future__ import annotations

import os
import time
import importlib
from pathlib import Path

import pytest
from starlette.testclient import TestClient

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """TestClient with a temp DB and Datasette suppressed."""
    tmp = tmp_path_factory.mktemp("api_ep_db")
    db_path = tmp / "api_ep_test.db"

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    import src.main as main_mod

    original_start = main_mod._start_datasette
    original_stop = main_mod._stop_datasette
    main_mod._start_datasette = lambda: None
    main_mod._stop_datasette = lambda: None

    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c

    main_mod._start_datasette = original_start
    main_mod._stop_datasette = original_stop
    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)


# ---------------------------------------------------------------------------
# /api/run
# ---------------------------------------------------------------------------


def test_api_run_returns_202_and_job_id(client):
    """POST /api/run returns 202 with a job_id key."""
    resp = client.post("/api/run", data={"run_mode": "delta"})
    assert resp.status_code == 202
    body = resp.json()
    assert "job_id" in body
    assert body["job_id"]


def test_api_run_status_returns_valid_shape_for_new_job(client):
    """Immediately after starting a run, status endpoint returns a valid shape."""
    resp = client.post("/api/run", data={"run_mode": "delta"})
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    status_resp = client.get(f"/api/run/status/{job_id}")
    assert status_resp.status_code == 200
    body = status_resp.json()
    assert body["status"] in ("running", "complete", "error")
    assert "progress" in body
    assert "office" in body["progress"]


def test_api_run_status_404_for_unknown_job_id(client):
    """Status endpoint returns 404 for a job ID that does not exist."""
    resp = client.get("/api/run/status/nonexistent-uuid-abc")
    assert resp.status_code == 404


def test_api_run_cancel_404_for_unknown_job_id(client):
    """Cancel endpoint returns 404 for a job ID that does not exist."""
    resp = client.post("/api/run/cancel/nonexistent-uuid-abc")
    assert resp.status_code == 404


def test_api_run_cancel_409_when_job_already_complete(client):
    """Cancel endpoint returns 409 when the job is not running."""
    import uuid
    from src.routers.run_scraper import _run_job_store, _run_job_lock

    fake_job_id = str(uuid.uuid4())
    with _run_job_lock:
        _run_job_store[fake_job_id] = {
            "status": "complete",
            "progress": {},
            "cancelled": False,
        }

    try:
        resp = client.post(f"/api/run/cancel/{fake_job_id}")
        assert resp.status_code == 409
        assert resp.json()["ok"] is False
    finally:
        with _run_job_lock:
            _run_job_store.pop(fake_job_id, None)


# ---------------------------------------------------------------------------
# /api/refresh-table-cache
# ---------------------------------------------------------------------------


def test_api_refresh_table_cache_400_for_missing_url(client):
    """refresh-table-cache returns 400 when url is empty."""
    resp = client.post(
        "/api/refresh-table-cache",
        json={"url": "", "table_no": 1},
    )
    assert resp.status_code == 400


def test_api_refresh_table_cache_400_for_invalid_json(client):
    """refresh-table-cache returns 400 when body is not valid JSON."""
    resp = client.post(
        "/api/refresh-table-cache",
        content=b"not json at all",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400
