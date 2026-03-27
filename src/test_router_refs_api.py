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

    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c

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


# ---------------------------------------------------------------------------
# /refs/countries — full CRUD
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def crud_client(tmp_path_factory):
    """Separate client with its own DB for CRUD mutation tests."""
    import importlib

    tmp = tmp_path_factory.mktemp("refs_crud_db")
    db_path = tmp / "refs_crud_test.db"

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    import src.main as main_mod

    importlib.reload(main_mod)

    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c

    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)


def test_create_country_redirects_on_success(crud_client):
    resp = crud_client.post(
        "/refs/countries/new", data={"name": "Testlandia"}, follow_redirects=False
    )
    assert resp.status_code == 302
    assert "countries" in resp.headers["location"]


def test_create_country_duplicate_name_returns_form_with_error(crud_client):
    crud_client.post("/refs/countries/new", data={"name": "Dupeland"})
    resp = crud_client.post(
        "/refs/countries/new", data={"name": "Dupeland"}, follow_redirects=False
    )
    assert resp.status_code == 200
    assert "already exists" in resp.text.lower() or "error" in resp.text.lower()


def test_get_country_edit_form_returns_200(crud_client):
    crud_client.post("/refs/countries/new", data={"name": "EditableCountry"})
    countries = crud_client.get("/api/countries").json()
    target = next((c for c in countries if c["name"] == "EditableCountry"), None)
    assert target is not None
    resp = crud_client.get(f"/refs/countries/{target['id']}")
    assert resp.status_code == 200
    assert "EditableCountry" in resp.text


def test_get_country_edit_form_404_for_unknown_id(crud_client):
    resp = crud_client.get("/refs/countries/999999")
    assert resp.status_code == 404


def test_update_country_redirects_on_success(crud_client):
    crud_client.post("/refs/countries/new", data={"name": "OldName"})
    countries = crud_client.get("/api/countries").json()
    target = next((c for c in countries if c["name"] == "OldName"), None)
    assert target is not None
    resp = crud_client.post(
        f"/refs/countries/{target['id']}", data={"name": "NewName"}, follow_redirects=False
    )
    assert resp.status_code == 302


def test_delete_country_redirects_on_success(crud_client):
    crud_client.post("/refs/countries/new", data={"name": "ToDelete"})
    countries = crud_client.get("/api/countries").json()
    target = next((c for c in countries if c["name"] == "ToDelete"), None)
    assert target is not None
    resp = crud_client.post(f"/refs/countries/{target['id']}/delete", follow_redirects=False)
    assert resp.status_code == 302
    countries_after = crud_client.get("/api/countries").json()
    assert not any(c["name"] == "ToDelete" for c in countries_after)


# ---------------------------------------------------------------------------
# /refs/levels — CRUD
# ---------------------------------------------------------------------------


def test_create_level_redirects_on_success(crud_client):
    resp = crud_client.post("/refs/levels/new", data={"name": "Municipal"}, follow_redirects=False)
    assert resp.status_code == 302


def test_create_level_duplicate_returns_error(crud_client):
    crud_client.post("/refs/levels/new", data={"name": "DupLevel"})
    resp = crud_client.post("/refs/levels/new", data={"name": "DupLevel"}, follow_redirects=False)
    assert resp.status_code == 200
    assert "error" in resp.text.lower() or "already" in resp.text.lower()


def test_update_level_redirects_on_success(crud_client):
    crud_client.post("/refs/levels/new", data={"name": "OldLevel"})
    levels = crud_client.get("/api/levels").json()
    target = next((lv for lv in levels if lv["name"] == "OldLevel"), None)
    assert target is not None
    resp = crud_client.post(
        f"/refs/levels/{target['id']}", data={"name": "UpdatedLevel"}, follow_redirects=False
    )
    assert resp.status_code == 302


def test_delete_level_redirects_on_success(crud_client):
    crud_client.post("/refs/levels/new", data={"name": "LevelToDelete"})
    levels = crud_client.get("/api/levels").json()
    target = next((lv for lv in levels if lv["name"] == "LevelToDelete"), None)
    assert target is not None
    resp = crud_client.post(f"/refs/levels/{target['id']}/delete", follow_redirects=False)
    assert resp.status_code == 302
    levels_after = crud_client.get("/api/levels").json()
    assert not any(lv["name"] == "LevelToDelete" for lv in levels_after)


# ---------------------------------------------------------------------------
# /refs/branches — CRUD
# ---------------------------------------------------------------------------


def test_create_branch_redirects_on_success(crud_client):
    resp = crud_client.post(
        "/refs/branches/new", data={"name": "Electoral"}, follow_redirects=False
    )
    assert resp.status_code == 302


def test_create_branch_duplicate_returns_error(crud_client):
    crud_client.post("/refs/branches/new", data={"name": "DupBranch"})
    resp = crud_client.post(
        "/refs/branches/new", data={"name": "DupBranch"}, follow_redirects=False
    )
    assert resp.status_code == 200
    assert "error" in resp.text.lower() or "already" in resp.text.lower()


def test_update_branch_redirects_on_success(crud_client):
    crud_client.post("/refs/branches/new", data={"name": "OldBranch"})
    branches = crud_client.get("/api/branches").json()
    target = next((b for b in branches if b["name"] == "OldBranch"), None)
    assert target is not None
    resp = crud_client.post(
        f"/refs/branches/{target['id']}", data={"name": "UpdatedBranch"}, follow_redirects=False
    )
    assert resp.status_code == 302


def test_delete_branch_redirects_on_success(crud_client):
    crud_client.post("/refs/branches/new", data={"name": "BranchToDelete"})
    branches = crud_client.get("/api/branches").json()
    target = next((b for b in branches if b["name"] == "BranchToDelete"), None)
    assert target is not None
    resp = crud_client.post(f"/refs/branches/{target['id']}/delete", follow_redirects=False)
    assert resp.status_code == 302
    branches_after = crud_client.get("/api/branches").json()
    assert not any(b["name"] == "BranchToDelete" for b in branches_after)


# ---------------------------------------------------------------------------
# /refs/states — CRUD
# ---------------------------------------------------------------------------


def test_create_state_redirects_on_success(crud_client):
    countries = crud_client.get("/api/countries").json()
    us = next(c for c in countries if c["name"] == "United States of America")
    resp = crud_client.post(
        "/refs/states/new",
        data={"name": "New Test State", "country_id": us["id"]},
        follow_redirects=False,
    )
    assert resp.status_code == 302


def test_update_state_redirects_on_success(crud_client):
    countries = crud_client.get("/api/countries").json()
    us = next(c for c in countries if c["name"] == "United States of America")
    crud_client.post("/refs/states/new", data={"name": "StateToUpdate", "country_id": us["id"]})
    states = crud_client.get(f"/api/states?country_id={us['id']}").json()
    target = next((s for s in states if s["name"] == "StateToUpdate"), None)
    assert target is not None
    resp = crud_client.post(
        f"/refs/states/{target['id']}",
        data={"name": "StateUpdated", "country_id": us["id"]},
        follow_redirects=False,
    )
    assert resp.status_code == 302


def test_delete_state_redirects_on_success(crud_client):
    countries = crud_client.get("/api/countries").json()
    us = next(c for c in countries if c["name"] == "United States of America")
    crud_client.post("/refs/states/new", data={"name": "StateToDelete", "country_id": us["id"]})
    states = crud_client.get(f"/api/states?country_id={us['id']}").json()
    target = next((s for s in states if s["name"] == "StateToDelete"), None)
    assert target is not None
    resp = crud_client.post(f"/refs/states/{target['id']}/delete", follow_redirects=False)
    assert resp.status_code == 302
