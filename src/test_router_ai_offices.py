"""Tests for src/routers/ai_offices.py.

Tests all HTTP routes: GET /ai-offices, POST /api/ai-offices/batch,
GET /api/ai-offices/batch/{job_id}/status, POST /api/ai-offices/batch/{job_id}/cancel.

Uses FastAPI TestClient with SQLite in-memory DB.
OPENAI_API_KEY is set to a fake value in the module fixture; individual tests
that need to test the missing-key path use patch.dict to temporarily remove it.

Run: pytest src/test_router_ai_offices.py -v
"""

from __future__ import annotations

import importlib
import os
import time
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from src.db.connection import init_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("ai_offices_db")
    path = tmp / "ai_offices_test.db"
    init_db(path=path)
    return path


@pytest.fixture(scope="module")
def client(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    os.environ["OPENAI_API_KEY"] = "sk-test-fake-key-for-testing"
    import src.main as main_mod

    importlib.reload(main_mod)
    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c
    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)
    os.environ.pop("OPENAI_API_KEY", None)


_VALID_DEFAULTS = {
    "country_id": 1,
    "level_id": 1,
    "branch_id": 1,
}

_VALID_BODY = {
    "urls": ["https://en.wikipedia.org/wiki/Test"],
    "defaults": _VALID_DEFAULTS,
}


# ---------------------------------------------------------------------------
# GET /ai-offices — page
# ---------------------------------------------------------------------------


def test_ai_offices_page_returns_200(client):
    resp = client.get("/ai-offices")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_ai_offices_page_no_key_still_returns_200(client):
    """GET /ai-offices is always 200; api_key_set flag changes but page still renders."""
    with patch.dict(os.environ):
        os.environ.pop("OPENAI_API_KEY", None)
        resp = client.get("/ai-offices")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /api/ai-offices/batch — validation
# ---------------------------------------------------------------------------


def test_batch_start_no_api_key_returns_503(client):
    with patch.dict(os.environ):
        os.environ.pop("OPENAI_API_KEY", None)
        resp = client.post("/api/ai-offices/batch", json=_VALID_BODY)
    assert resp.status_code == 503


def test_batch_start_empty_urls_returns_400(client):
    body = {"urls": [], "defaults": _VALID_DEFAULTS}
    resp = client.post("/api/ai-offices/batch", json=body)
    assert resp.status_code == 400


def test_batch_start_blank_urls_returns_400(client):
    body = {"urls": ["  ", ""], "defaults": _VALID_DEFAULTS}
    resp = client.post("/api/ai-offices/batch", json=body)
    assert resp.status_code == 400


def test_batch_start_missing_country_id_returns_400(client):
    body = {
        "urls": ["https://en.wikipedia.org/wiki/Test"],
        "defaults": {"country_id": 0, "level_id": 1, "branch_id": 1},
    }
    resp = client.post("/api/ai-offices/batch", json=body)
    assert resp.status_code == 400


def test_batch_start_missing_level_id_returns_400(client):
    body = {
        "urls": ["https://en.wikipedia.org/wiki/Test"],
        "defaults": {"country_id": 1, "level_id": 0, "branch_id": 1},
    }
    resp = client.post("/api/ai-offices/batch", json=body)
    assert resp.status_code == 400


def test_batch_start_missing_branch_id_returns_400(client):
    body = {
        "urls": ["https://en.wikipedia.org/wiki/Test"],
        "defaults": {"country_id": 1, "level_id": 1, "branch_id": 0},
    }
    resp = client.post("/api/ai-offices/batch", json=body)
    assert resp.status_code == 400


def test_batch_start_valid_returns_202_with_job_id(client):
    resp = client.post("/api/ai-offices/batch", json=_VALID_BODY)
    assert resp.status_code == 202
    data = resp.json()
    assert "job_id" in data
    assert data["status"] == "running"


# ---------------------------------------------------------------------------
# GET /api/ai-offices/batch/{job_id}/status
# ---------------------------------------------------------------------------


def test_batch_status_unknown_job_returns_404(client):
    resp = client.get("/api/ai-offices/batch/nonexistent-job-id/status")
    assert resp.status_code == 404


def test_batch_status_known_job_returns_200(client):
    resp = client.post("/api/ai-offices/batch", json=_VALID_BODY)
    job_id = resp.json()["job_id"]

    resp2 = client.get(f"/api/ai-offices/batch/{job_id}/status")
    assert resp2.status_code == 200
    data = resp2.json()
    assert "status" in data
    assert "results" in data
    assert "total_urls" in data


def test_batch_status_contains_url_results(client):
    resp = client.post("/api/ai-offices/batch", json=_VALID_BODY)
    job_id = resp.json()["job_id"]

    resp2 = client.get(f"/api/ai-offices/batch/{job_id}/status")
    data = resp2.json()
    assert len(data["results"]) == 1
    assert data["results"][0]["url"] == "https://en.wikipedia.org/wiki/Test"


# ---------------------------------------------------------------------------
# POST /api/ai-offices/batch/{job_id}/cancel
# ---------------------------------------------------------------------------


def test_batch_cancel_unknown_job_returns_404(client):
    resp = client.post("/api/ai-offices/batch/nonexistent-job-id/cancel")
    assert resp.status_code == 404


def test_batch_cancel_running_job_returns_200_or_409(client):
    resp = client.post("/api/ai-offices/batch", json=_VALID_BODY)
    job_id = resp.json()["job_id"]

    # Either 200 (cancelled running job) or 409 (worker finished before cancel)
    resp2 = client.post(f"/api/ai-offices/batch/{job_id}/cancel")
    assert resp2.status_code in (200, 409)


def test_batch_cancel_already_complete_job_returns_409(client):
    """Inject a complete job into the store and verify cancel returns 409."""
    from src.routers.ai_offices import _batch_job_lock, _batch_job_store

    fake_id = "test-complete-job-cancel-check"
    with _batch_job_lock:
        _batch_job_store[fake_id] = {
            "status": "complete",
            "_created_at": time.monotonic(),
            "cancelled": False,
            "current_url_index": 0,
            "total_urls": 1,
            "results": [],
        }

    resp = client.post(f"/api/ai-offices/batch/{fake_id}/cancel")
    assert resp.status_code == 409

    with _batch_job_lock:
        _batch_job_store.pop(fake_id, None)
