"""Tests for the reference-data dropdown API endpoints.

Uses FastAPI TestClient (no live server). The seeded DB always contains
"United States of America" + states, Federal/State/Local levels, and
Executive/Legislative/Judicial branches.

Run: pytest src/test_router_refs_api.py -v
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
    """TestClient with a seeded temp DB and Datasette suppressed."""
    tmp = tmp_path_factory.mktemp("refs_api_db")
    db_path = tmp / "refs_api_test.db"

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
# /api/countries
# ---------------------------------------------------------------------------


def test_api_countries_returns_list(client):
    """GET /api/countries returns a JSON list with id and name keys."""
    resp = client.get("/api/countries")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) > 0
    assert "id" in body[0]
    assert "name" in body[0]
    names = [r["name"] for r in body]
    assert "United States of America" in names


# ---------------------------------------------------------------------------
# /api/states
# ---------------------------------------------------------------------------


def test_api_states_with_valid_country_id_returns_list(client):
    """GET /api/states?country_id=<us_id> returns US states."""
    countries = client.get("/api/countries").json()
    us = next(r for r in countries if r["name"] == "United States of America")

    resp = client.get(f"/api/states?country_id={us['id']}")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) > 0
    state_names = [r["name"] for r in body]
    assert "California" in state_names


def test_api_states_with_zero_country_id_returns_empty(client):
    """GET /api/states?country_id=0 returns an empty list."""
    resp = client.get("/api/states?country_id=0")
    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# /api/levels and /api/branches
# ---------------------------------------------------------------------------


def test_api_levels_returns_list(client):
    """GET /api/levels returns seeded levels."""
    resp = client.get("/api/levels")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) > 0
    names = [r["name"] for r in body]
    assert "Federal" in names


def test_api_branches_returns_list(client):
    """GET /api/branches returns seeded branches."""
    resp = client.get("/api/branches")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) > 0
    names = [r["name"] for r in body]
    assert "Executive" in names
