"""Tests for data-view routes: /data/individuals, /data/office-terms, /report/milestones.

Validates that each page renders 200 on an empty DB and that pagination
query params are accepted without error.

Run: pytest src/test_data_routes.py -v
"""

from __future__ import annotations

import os

import pytest
from starlette.testclient import TestClient

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """TestClient with a temp DB and Datasette suppressed."""
    tmp = tmp_path_factory.mktemp("data_routes_db")
    db_path = tmp / "data_routes_test.db"

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
# Data view tests
# ---------------------------------------------------------------------------


def test_data_individuals_renders_200(client):
    """GET /data/individuals returns 200 on an empty DB."""
    resp = client.get("/data/individuals")
    assert resp.status_code == 200


def test_data_office_terms_renders_200(client):
    """GET /data/office-terms returns 200 on an empty DB."""
    resp = client.get("/data/office-terms")
    assert resp.status_code == 200


def test_data_office_terms_pagination_params_accepted(client):
    """GET /data/office-terms with limit/offset params returns 200."""
    resp = client.get("/data/office-terms?limit=10&offset=5")
    assert resp.status_code == 200


def test_report_milestones_renders_200(client):
    """GET /report/milestones returns 200 — validates date-window SQL on empty DB."""
    resp = client.get("/report/milestones")
    assert resp.status_code == 200
