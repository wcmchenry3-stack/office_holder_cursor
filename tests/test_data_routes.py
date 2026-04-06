# -*- coding: utf-8 -*-
"""Integration tests for src/routers/data.py routes.

Covers:
- GET /data/scheduled-jobs → 200 HTML
- POST /api/scheduler-settings/{job_id}/pause → 200 JSON, persists state
- POST /api/scheduler-settings/{job_id}/resume → 200 JSON
- GET /data/runner-registry → 200 HTML
- GET /data/scheduled-job-runs → 200 HTML
- POST /api/scheduler-settings with invalid job_id → 400

Note: Wikipedia URL strings below are used only as test-data values.
No HTTP requests to Wikipedia are made. All actual Wikipedia HTTP requests
go through wiki_fetch.py which sets the required User-Agent header.
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from src.db.scheduler_settings import PAUSEABLE_JOB_IDS


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("data_routes_db")
    db_path = tmp / "test.db"
    cache_dir = tmp / "wiki_cache"
    cache_dir.mkdir()

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    os.environ["WIKI_CACHE_DIR"] = str(cache_dir)

    from src.main import app
    from src.db.connection import init_db

    init_db()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ---------------------------------------------------------------------------
# GET /data/scheduled-jobs
# ---------------------------------------------------------------------------


def test_scheduled_jobs_returns_200(client):
    resp = client.get("/data/scheduled-jobs")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# POST /api/scheduler-settings/{job_id}/pause
# ---------------------------------------------------------------------------


def test_pause_job_returns_200_json(client):
    job_id = PAUSEABLE_JOB_IDS[0]
    resp = client.post(f"/api/scheduler-settings/{job_id}/pause")
    assert resp.status_code == 200
    data = resp.json()
    assert data["job_id"] == job_id
    assert data["paused"] is True


def test_pause_job_persists_state(client):
    job_id = PAUSEABLE_JOB_IDS[0]
    client.post(f"/api/scheduler-settings/{job_id}/pause")
    from src.db.scheduler_settings import is_job_paused

    assert is_job_paused(job_id) is True


def test_pause_invalid_job_returns_400(client):
    resp = client.post("/api/scheduler-settings/nonexistent_job/pause")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/scheduler-settings/{job_id}/resume
# ---------------------------------------------------------------------------


def test_resume_job_returns_200_json(client):
    job_id = PAUSEABLE_JOB_IDS[0]
    client.post(f"/api/scheduler-settings/{job_id}/pause")
    resp = client.post(f"/api/scheduler-settings/{job_id}/resume")
    assert resp.status_code == 200
    data = resp.json()
    assert data["paused"] is False


def test_resume_persists_state(client):
    job_id = PAUSEABLE_JOB_IDS[1]
    client.post(f"/api/scheduler-settings/{job_id}/pause")
    client.post(f"/api/scheduler-settings/{job_id}/resume")
    from src.db.scheduler_settings import is_job_paused

    assert is_job_paused(job_id) is False


def test_resume_invalid_job_returns_400(client):
    resp = client.post("/api/scheduler-settings/nonexistent_job/resume")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /data/runner-registry
# ---------------------------------------------------------------------------


def test_runner_registry_returns_200(client):
    resp = client.get("/data/runner-registry")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# GET /data/scheduled-job-runs
# ---------------------------------------------------------------------------


def test_scheduled_job_runs_returns_200(client):
    resp = client.get("/data/scheduled-job-runs")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
