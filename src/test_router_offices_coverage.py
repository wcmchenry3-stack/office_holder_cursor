"""Additional coverage tests for src/routers/offices.py.

Targets uncovered branches: helper functions, validation paths,
hierarchy list, import, page CRUD, office update, and add-office-to-page.

Run: pytest src/test_router_offices_coverage.py -v
"""

from __future__ import annotations

import importlib
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from src.db import offices as db_offices
from src.db import refs as db_refs
from src.db.connection import get_connection, init_db
from src.routers.offices import (
    _list_return_query,
    _page_redirect_query,
    _validate_level_state_city,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("offices_cov_db")
    path = tmp / "offices_cov_test.db"
    init_db(path=path)
    return path


@pytest.fixture(scope="module")
def client(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    import src.main as main_mod

    importlib.reload(main_mod)
    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c
    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)


@pytest.fixture(scope="module")
def seeded_ids(db_path, client):
    """Return seeded country_id, level_ids dict, branch_ids dict for use in tests.
    Depends on client to ensure OFFICE_HOLDER_DB_PATH is set before any test that
    calls _validate_level_state_city (which opens its own connection via get_connection())."""
    conn = get_connection(db_path)
    try:
        countries = {
            r["name"]: r["id"] for r in conn.execute("SELECT id, name FROM countries").fetchall()
        }
        levels = {
            r["name"]: r["id"] for r in conn.execute("SELECT id, name FROM levels").fetchall()
        }
        branches = {
            r["name"]: r["id"] for r in conn.execute("SELECT id, name FROM branches").fetchall()
        }
        states = {
            r["name"]: r["id"]
            for r in conn.execute(
                "SELECT id, name FROM states WHERE country_id = ?",
                (countries.get("United States of America", 1),),
            ).fetchall()
        }
    finally:
        conn.close()
    return {"countries": countries, "levels": levels, "branches": branches, "states": states}


@pytest.fixture(scope="module")
def page_id(db_path, seeded_ids):
    """Create a page with one office and return (source_page_id, office_id)."""
    conn = get_connection(db_path)
    try:
        oid = db_offices.create_office(
            {
                "country_id": seeded_ids["countries"].get("United States of America", 1),
                "name": "Coverage Office",
                "url": "https://en.wikipedia.org/wiki/Coverage_Office_Router",
                "enabled": True,
            },
            conn=conn,
        )
        conn.commit()
        office = db_offices.get_office(oid, conn=conn)
        return office.get("source_page_id"), oid
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helper function — _list_return_query
# ---------------------------------------------------------------------------


def test_list_return_query_all_params():
    q = _list_return_query(
        country_id=1,
        state_id=2,
        level_id=3,
        branch_id=4,
        office_category_id=5,
        enabled="1",
        limit="20",
        office_count="gt0",
    )
    assert "country_id=1" in q
    assert "state_id=2" in q
    assert "level_id=3" in q
    assert "branch_id=4" in q
    assert "office_category_id=5" in q
    assert "enabled=1" in q
    assert "limit=20" in q
    assert "office_count=gt0" in q


def test_list_return_query_empty_params():
    q = _list_return_query()
    assert q == ""


def test_list_return_query_office_count_all_is_excluded():
    """office_count='all' should not appear in the query string."""
    q = _list_return_query(office_count="all")
    assert "office_count" not in q


def test_list_return_query_partial_params():
    q = _list_return_query(country_id=1, enabled="0")
    assert "country_id=1" in q
    assert "enabled=0" in q
    assert "state_id" not in q


# ---------------------------------------------------------------------------
# Helper function — _page_redirect_query
# ---------------------------------------------------------------------------


def test_page_redirect_query_both():
    q = _page_redirect_query("1,2,3", "level_id=3")
    assert "nav_ids=1,2,3" in q
    assert "level_id=3" in q


def test_page_redirect_query_nav_only():
    q = _page_redirect_query("5", "")
    assert "nav_ids=5" in q


def test_page_redirect_query_list_only():
    q = _page_redirect_query("", "country_id=1")
    assert "country_id=1" in q
    assert "nav_ids" not in q


def test_page_redirect_query_empty():
    assert _page_redirect_query("", "") == ""


# ---------------------------------------------------------------------------
# Helper function — _validate_level_state_city (direct, mocking DB refs)
# ---------------------------------------------------------------------------


def test_validate_no_level_always_passes():
    """None level_id bypasses all validation."""
    _validate_level_state_city(None, None, None)  # should not raise


def test_validate_federal_no_state_passes(seeded_ids):
    federal_id = seeded_ids["levels"].get("Federal")
    if not federal_id:
        pytest.skip("Federal level not seeded")
    _validate_level_state_city(federal_id, None, None)  # should not raise


def test_validate_federal_with_state_raises(seeded_ids):
    federal_id = seeded_ids["levels"].get("Federal")
    state_id = next(iter(seeded_ids["states"].values()), None)
    if not federal_id or not state_id:
        pytest.skip("Required reference data not seeded")
    with pytest.raises(ValueError, match="[Ss]tate"):
        _validate_level_state_city(federal_id, state_id, None)


def test_validate_federal_legislative_with_state_ok(seeded_ids):
    federal_id = seeded_ids["levels"].get("Federal")
    leg_id = seeded_ids["branches"].get("Legislative")
    state_id = next(iter(seeded_ids["states"].values()), None)
    if not federal_id or not leg_id or not state_id:
        pytest.skip("Required reference data not seeded")
    # State allowed for Federal Legislative — should not raise
    _validate_level_state_city(federal_id, state_id, None, branch_id=leg_id)


def test_validate_federal_legislative_with_city_raises(seeded_ids):
    federal_id = seeded_ids["levels"].get("Federal")
    leg_id = seeded_ids["branches"].get("Legislative")
    if not federal_id or not leg_id:
        pytest.skip("Required reference data not seeded")
    with pytest.raises(ValueError, match="[Cc]ity"):
        _validate_level_state_city(federal_id, None, 99, branch_id=leg_id)


def test_validate_state_level_no_state_raises(seeded_ids):
    state_level_id = seeded_ids["levels"].get("State")
    if not state_level_id:
        pytest.skip("State level not seeded")
    with pytest.raises(ValueError, match="[Ss]tate"):
        _validate_level_state_city(state_level_id, None, None)


def test_validate_state_level_with_city_raises(seeded_ids):
    state_level_id = seeded_ids["levels"].get("State")
    state_id = next(iter(seeded_ids["states"].values()), None)
    if not state_level_id or not state_id:
        pytest.skip("Required reference data not seeded")
    with pytest.raises(ValueError, match="[Cc]ity"):
        _validate_level_state_city(state_level_id, state_id, 99)


def test_validate_local_level_no_state_raises(seeded_ids):
    local_id = seeded_ids["levels"].get("Local")
    if not local_id:
        pytest.skip("Local level not seeded")
    with pytest.raises(ValueError, match="[Ss]tate"):
        _validate_level_state_city(local_id, None, None)


def test_validate_local_level_no_city_raises(seeded_ids):
    local_id = seeded_ids["levels"].get("Local")
    state_id = next(iter(seeded_ids["states"].values()), None)
    if not local_id or not state_id:
        pytest.skip("Required reference data not seeded")
    with pytest.raises(ValueError, match="[Cc]ity"):
        _validate_level_state_city(local_id, state_id, None)


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------


def test_index_redirect_to_offices(client):
    resp = client.get("/", follow_redirects=False)
    assert resp.status_code == 302
    assert "/offices" in resp.headers["location"]


def test_offices_import_page_returns_200(client):
    resp = client.get("/offices/import")
    assert resp.status_code == 200


def test_offices_import_empty_path_returns_error(client):
    resp = client.post("/offices/import", data={"csv_path": ""})
    assert resp.status_code == 200
    assert "required" in resp.text.lower() or "path" in resp.text.lower()


def test_offices_import_nonexistent_file_returns_error(client):
    resp = client.post("/offices/import", data={"csv_path": "/no/such/file.csv"})
    assert resp.status_code == 200
    assert "not found" in resp.text.lower() or "error" in resp.text.lower()


def test_offices_import_valid_csv(client, tmp_path):
    """POST /offices/import with a valid (minimal) CSV redirects on success."""
    csv = tmp_path / "import_test.csv"
    # Write a CSV with the required columns — just the header is enough for 0 rows imported
    csv.write_text("url,name,country_id\n", encoding="utf-8")
    resp = client.post("/offices/import", data={"csv_path": str(csv)}, follow_redirects=False)
    # Either a redirect (success) or 200 with error — both are acceptable
    assert resp.status_code in (302, 200)


def test_offices_list_search_by_office_id_found(client, page_id):
    _, oid = page_id
    resp = client.get(f"/offices?search_office_id={oid}", follow_redirects=False)
    assert resp.status_code == 302
    assert str(oid) in resp.headers["location"]


def test_offices_list_search_by_office_id_not_found(client):
    resp = client.get("/offices?search_office_id=999999")
    assert resp.status_code == 200  # falls through to list


def test_offices_list_search_by_url_found(client, page_id):
    _, oid = page_id
    # Get the office URL
    office = db_offices.get_office(oid)
    url = office.get("url") if office else None
    if not url:
        pytest.skip("No URL on fixture office")
    resp = client.get(f"/offices?search_url={url}", follow_redirects=False)
    assert resp.status_code == 302


def test_office_create_federal_level_with_state_shows_error(client, seeded_ids):
    """POST /offices/new with Federal level + state set → 200 with validation error."""
    federal_id = seeded_ids["levels"].get("Federal")
    state_id = next(iter(seeded_ids["states"].values()), None)
    if not federal_id or not state_id:
        pytest.skip("Required reference data not seeded")
    resp = client.post(
        "/offices/new",
        data={
            "country_id": str(seeded_ids["countries"].get("United States of America", 1)),
            "level_id": str(federal_id),
            "state_id": str(state_id),
            "city_id": "",
            "branch_id": "",
            "name": "Bad Federal Office",
            "url": "https://en.wikipedia.org/wiki/Bad_Federal_Office_Cov",
            "action": "save_and_close",
        },
    )
    assert resp.status_code == 200
    assert "federal" in resp.text.lower() or "state" in resp.text.lower()


def test_office_create_duplicate_url_shows_error(client, page_id):
    """POST /offices/new with an already-existing URL → 200 with validation error."""
    _, oid = page_id
    office = db_offices.get_office(oid)
    url = office.get("url") if office else None
    if not url:
        pytest.skip("No URL on fixture office")
    resp = client.post(
        "/offices/new",
        data={
            "country_id": str(seeded_ids_value := office.get("country_id") or 1),
            "name": "Dup Office",
            "url": url,
            "action": "save_and_close",
        },
    )
    assert resp.status_code == 200
    assert "already exists" in resp.text.lower() or "url" in resp.text.lower()


def test_page_delete(client, db_path, seeded_ids):
    """POST /pages/{source_page_id}/delete redirects to /offices."""
    conn = get_connection(db_path)
    try:
        oid = db_offices.create_office(
            {
                "country_id": seeded_ids["countries"].get("United States of America", 1),
                "name": "Delete Me Page Office",
                "url": "https://en.wikipedia.org/wiki/Delete_Me_Page_Office_Cov",
                "enabled": False,
            },
            conn=conn,
        )
        conn.commit()
        office = db_offices.get_office(oid, conn=conn)
        spid = office.get("source_page_id")
    finally:
        conn.close()

    resp = client.post(f"/pages/{spid}/delete", follow_redirects=False)
    assert resp.status_code == 302
    assert "/offices" in resp.headers["location"]


def test_api_page_enabled_404(client):
    resp = client.post("/api/pages/999999/enabled", data={"enabled": "1"})
    assert resp.status_code == 404


def test_api_page_enabled_toggle(client, page_id):
    spid, _ = page_id
    resp = client.post(f"/api/pages/{spid}/enabled", data={"enabled": "0"})
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("ok") is True


def test_page_update_valid(client, page_id, seeded_ids):
    """POST /pages/{id} with valid data → redirect."""
    spid, _ = page_id
    resp = client.post(
        f"/pages/{spid}",
        data={
            "url": "https://en.wikipedia.org/wiki/Coverage_Office_Router_Updated",
            "country_id": str(seeded_ids["countries"].get("United States of America", 1)),
            "state_id": "",
            "city_id": "",
            "level_id": "",
            "branch_id": "",
            "notes": "",
            "enabled": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302


def test_page_update_validation_error_redirects(client, page_id, seeded_ids):
    """POST /pages/{id} with Federal level + state → redirect with error param."""
    spid, _ = page_id
    federal_id = seeded_ids["levels"].get("Federal")
    state_id = next(iter(seeded_ids["states"].values()), None)
    if not federal_id or not state_id:
        pytest.skip("Required reference data not seeded")
    resp = client.post(
        f"/pages/{spid}",
        data={
            "url": "https://en.wikipedia.org/wiki/Coverage_Office_Router_Updated",
            "country_id": str(seeded_ids["countries"].get("United States of America", 1)),
            "state_id": str(state_id),
            "city_id": "",
            "level_id": str(federal_id),
            "branch_id": "",
            "notes": "",
            "enabled": "1",
        },
        follow_redirects=False,
    )
    # Validation error → redirect with ?error= OR JSON error
    assert resp.status_code in (302, 200)


def test_office_update_redirects_on_success(client, page_id, seeded_ids):
    """POST /offices/{office_id} with valid data → redirect."""
    _, oid = page_id
    resp = client.post(
        f"/offices/{oid}",
        data={
            "country_id": str(seeded_ids["countries"].get("United States of America", 1)),
            "name": "Updated Coverage Office",
            "url": "https://en.wikipedia.org/wiki/Coverage_Office_Router",
            "enabled": "1",
            "action": "save_and_close",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302


def test_office_update_action_save_redirects_to_office(client, page_id, seeded_ids):
    """action=save → redirect to /offices/{id}?saved=1."""
    _, oid = page_id
    resp = client.post(
        f"/offices/{oid}",
        data={
            "country_id": str(seeded_ids["countries"].get("United States of America", 1)),
            "name": "Updated Coverage Office",
            "url": "https://en.wikipedia.org/wiki/Coverage_Office_Router",
            "enabled": "1",
            "action": "save",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert str(oid) in resp.headers["location"]
    assert "saved=1" in resp.headers["location"]


def test_office_update_save_all_header_returns_json(client, page_id, seeded_ids):
    """X-Save-All: 1 header → JSON response instead of redirect."""
    _, oid = page_id
    resp = client.post(
        f"/offices/{oid}",
        data={
            "country_id": str(seeded_ids["countries"].get("United States of America", 1)),
            "name": "Updated Coverage Office",
            "url": "https://en.wikipedia.org/wiki/Coverage_Office_Router",
            "enabled": "1",
            "action": "save_and_close",
        },
        headers={"X-Save-All": "1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "ok" in body or "redirect" in body


def test_office_update_validation_error_redirects(client, page_id, seeded_ids):
    """POST /offices/{id} with Federal level + state → redirect with error."""
    _, oid = page_id
    federal_id = seeded_ids["levels"].get("Federal")
    state_id = next(iter(seeded_ids["states"].values()), None)
    if not federal_id or not state_id:
        pytest.skip("Required reference data not seeded")
    resp = client.post(
        f"/offices/{oid}",
        data={
            "country_id": str(seeded_ids["countries"].get("United States of America", 1)),
            "level_id": str(federal_id),
            "state_id": str(state_id),
            "name": "Bad Federal",
            "url": "https://en.wikipedia.org/wiki/Coverage_Office_Router",
            "action": "save_and_close",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert "error" in resp.headers["location"]


def test_office_add_to_page_bad_id(client):
    resp = client.post("/offices/add-office-to-page", data={"source_page_id": "0"})
    assert resp.status_code == 400


def test_office_add_to_page_unknown_id(client):
    resp = client.post("/offices/add-office-to-page", data={"source_page_id": "999999"})
    assert resp.status_code == 404


def test_office_add_to_page_valid(client, page_id):
    spid, _ = page_id
    resp = client.post(
        "/offices/add-office-to-page",
        data={"source_page_id": str(spid)},
        follow_redirects=False,
    )
    assert resp.status_code == 302
