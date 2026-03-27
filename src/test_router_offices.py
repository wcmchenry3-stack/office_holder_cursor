"""Tests for src/routers/offices.py.

Uses FastAPI TestClient (no live server). A seeded temp DB is shared across
the module; individual tests that mutate state create their own office(s)
via the API or directly through db_offices to avoid cross-test coupling.

Covers: list, new-form, create, edit-form, update, delete, duplicate,
table-config delete, enabled toggles, table-configs JSON, export-config,
populate-terms launch, and test-config draft endpoints.

Run: pytest src/test_router_offices.py -v
"""

from __future__ import annotations

import importlib
import os

import pytest
from starlette.testclient import TestClient

from src.db import offices as db_offices
from src.db.connection import get_connection, init_db

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_MINIMAL_FORM = {
    "country_id": "1",  # United States of America (seeded)
    "state_id": "",
    "city_id": "",
    "level_id": "",
    "branch_id": "",
    "department": "",
    "name": "Test Office",
    "enabled": "1",
    "notes": "",
    "url": "https://en.wikipedia.org/wiki/Test_Senate_Office",
    "table_no": "1",
    "table_rows": "4",
    "link_column": "1",
    "party_column": "0",
    "term_start_column": "4",
    "term_end_column": "5",
    "district_column": "0",
    "filter_column": "0",
    "filter_criteria": "",
    "dynamic_parse": "1",
    "action": "save_and_close",
}


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("offices_router_db")
    path = tmp / "offices_test.db"
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
def office_id(db_path):
    """Create one office in the DB and return its id for read-only tests."""
    conn = get_connection(db_path)
    try:
        oid = db_offices.create_office(
            {
                "country_id": 1,
                "name": "Fixture Office",
                "url": "https://en.wikipedia.org/wiki/Fixture_Office",
                "enabled": True,
                "table_configs": [
                    {
                        "table_no": 1,
                        "table_rows": 4,
                        "link_column": 1,
                        "party_column": 0,
                        "term_start_column": 4,
                        "term_end_column": 5,
                        "district_column": 0,
                        "enabled": 1,
                    }
                ],
            },
            conn=conn,
        )
        conn.commit()
        return oid
    finally:
        conn.close()


@pytest.fixture(scope="module")
def table_config_id(db_path, office_id):
    """Return the table_config id for the fixture office."""
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            "SELECT id FROM office_table_config WHERE office_details_id = %s LIMIT 1",
            (office_id,),
        )
        row = cur.fetchone()
        return row["id"]
    finally:
        conn.close()


@pytest.fixture(scope="module")
def source_page_id(db_path, office_id):
    """Return the source_page_id for the fixture office."""
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            "SELECT source_page_id FROM office_details WHERE id = %s",
            (office_id,),
        )
        row = cur.fetchone()
        return row["source_page_id"]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /offices — list
# ---------------------------------------------------------------------------


def test_offices_list_returns_200(client):
    resp = client.get("/offices")
    assert resp.status_code == 200


def test_offices_list_accepts_filter_params(client):
    resp = client.get("/offices?country_id=1&enabled=1&limit=20")
    assert resp.status_code == 200


def test_offices_list_search_url_invalid_redirects_or_renders(client):
    """Search by URL that doesn't exist renders list (no crash)."""
    resp = client.get("/offices?search_url=https://en.wikipedia.org/wiki/Nonexistent_XYZZY")
    assert resp.status_code in (200, 302)


# ---------------------------------------------------------------------------
# GET /offices/new
# ---------------------------------------------------------------------------


def test_office_new_form_returns_200(client):
    resp = client.get("/offices/new")
    assert resp.status_code == 200


def test_office_new_form_contains_country_select(client):
    resp = client.get("/offices/new")
    assert "United States" in resp.text


# ---------------------------------------------------------------------------
# POST /offices/new — create
# ---------------------------------------------------------------------------


def test_office_create_redirects_on_success(client):
    # action="save" redirects to /offices/{new_id}?saved=1; "save_and_close" → /offices?saved=1
    form = {
        **_MINIMAL_FORM,
        "url": "https://en.wikipedia.org/wiki/NewOffice_Create_Test",
        "action": "save",
    }
    resp = client.post("/offices/new", data=form, follow_redirects=False)
    assert resp.status_code == 302
    assert "/offices/" in resp.headers["location"]


def test_office_create_invalid_country_zero_re_renders_form(client):
    form = {**_MINIMAL_FORM, "country_id": "0", "url": "https://en.wikipedia.org/wiki/BadCountry"}
    resp = client.post("/offices/new", data=form, follow_redirects=False)
    # Invalid country → re-renders form with error (200) or redirects back
    assert resp.status_code in (200, 302)
    if resp.status_code == 200:
        assert (
            "error" in resp.text.lower()
            or "required" in resp.text.lower()
            or "country" in resp.text.lower()
        )


def test_office_create_duplicate_url_re_renders_with_error(client):
    """Creating a second office at the same URL renders an error/existing-office notice."""
    url = "https://en.wikipedia.org/wiki/DupURL_OfficeTest"
    form = {**_MINIMAL_FORM, "url": url}
    client.post("/offices/new", data=form)  # create first
    resp = client.post("/offices/new", data=form, follow_redirects=False)
    # Should re-render the form noting the URL already exists
    assert resp.status_code == 200
    assert (
        "already" in resp.text.lower()
        or "exists" in resp.text.lower()
        or "edit" in resp.text.lower()
    )


def test_office_create_federal_with_state_validation_error(client):
    """Federal non-legislative office with a state set should fail validation."""
    form = {
        **_MINIMAL_FORM,
        "url": "https://en.wikipedia.org/wiki/FederalWithState_Test",
        "level_id": "1",  # Federal
        "branch_id": "1",  # Executive
        "state_id": "2",  # California — not allowed for Federal Executive
    }
    resp = client.post("/offices/new", data=form, follow_redirects=False)
    assert resp.status_code == 200
    assert (
        "state" in resp.text.lower()
        or "federal" in resp.text.lower()
        or "error" in resp.text.lower()
    )


# ---------------------------------------------------------------------------
# GET /offices/{office_id} — edit form
# ---------------------------------------------------------------------------


def test_office_edit_form_returns_200(client, office_id):
    resp = client.get(f"/offices/{office_id}")
    assert resp.status_code == 200


def test_office_edit_form_contains_office_name(client, office_id):
    resp = client.get(f"/offices/{office_id}")
    assert "Fixture Office" in resp.text


def test_office_edit_form_404_for_unknown_id(client):
    resp = client.get("/offices/999999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /offices/{office_id}/delete
# ---------------------------------------------------------------------------


def test_office_delete_redirects_on_success(client, db_path):
    conn = get_connection(db_path)
    try:
        oid = db_offices.create_office(
            {
                "country_id": 1,
                "name": "Office To Delete",
                "url": "https://en.wikipedia.org/wiki/OfficeToDelete_Test",
                "enabled": True,
                "table_configs": [
                    {
                        "table_no": 1,
                        "table_rows": 4,
                        "link_column": 1,
                        "party_column": 0,
                        "term_start_column": 4,
                        "term_end_column": 5,
                        "district_column": 0,
                        "enabled": 1,
                    }
                ],
            },
            conn=conn,
        )
        conn.commit()
    finally:
        conn.close()

    resp = client.post(f"/offices/{oid}/delete", follow_redirects=False)
    assert resp.status_code == 302
    assert "offices" in resp.headers["location"]


# ---------------------------------------------------------------------------
# POST /offices/{office_id}/duplicate
# ---------------------------------------------------------------------------


def test_office_duplicate_redirects_to_new_copy(client, office_id):
    resp = client.post(f"/offices/{office_id}/duplicate", follow_redirects=False)
    assert resp.status_code == 302
    location = resp.headers["location"]
    assert "/offices/" in location


def test_office_duplicate_creates_copy_with_prefix(client, office_id, db_path):
    client.post(f"/offices/{office_id}/duplicate")
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            "SELECT name FROM office_details WHERE name LIKE 'Copy of%' ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["name"].startswith("Copy of")


# ---------------------------------------------------------------------------
# POST /offices/{office_id}/table/{tc_id}/delete
# ---------------------------------------------------------------------------


def test_table_delete_on_office_with_single_table_redirects(client, db_path):
    """Deleting the only table config on an office redirects (may delete office too)."""
    conn = get_connection(db_path)
    try:
        oid = db_offices.create_office(
            {
                "country_id": 1,
                "name": "Table Delete Test",
                "url": "https://en.wikipedia.org/wiki/TableDeleteTest_XYZ",
                "enabled": True,
                "table_configs": [
                    {
                        "table_no": 1,
                        "table_rows": 4,
                        "link_column": 1,
                        "party_column": 0,
                        "term_start_column": 4,
                        "term_end_column": 5,
                        "district_column": 0,
                        "enabled": 1,
                    }
                ],
            },
            conn=conn,
        )
        conn.commit()
        cur = conn.execute(
            "SELECT id FROM office_table_config WHERE office_details_id = %s LIMIT 1",
            (oid,),
        )
        tc_id = cur.fetchone()["id"]
    finally:
        conn.close()

    resp = client.post(f"/offices/{oid}/table/{tc_id}/delete", follow_redirects=False)
    assert resp.status_code == 302


# ---------------------------------------------------------------------------
# POST /api/offices/{office_id}/enabled
# ---------------------------------------------------------------------------


def test_api_office_enabled_returns_ok_json(client, office_id):
    resp = client.post(f"/api/offices/{office_id}/enabled", data={"enabled": "0"})
    assert resp.status_code == 200
    assert resp.json().get("ok") is True
    # Restore
    client.post(f"/api/offices/{office_id}/enabled", data={"enabled": "1"})


def test_api_office_enabled_unknown_id_returns_ok(client):
    # Handler is fire-and-forget — no 404 for unknown IDs by design
    resp = client.post("/api/offices/999999/enabled", data={"enabled": "1"})
    assert resp.status_code == 200
    assert resp.json().get("ok") is True


# ---------------------------------------------------------------------------
# POST /api/offices/enabled-all
# ---------------------------------------------------------------------------


def test_api_offices_enabled_all_returns_ok(client):
    resp = client.post("/api/offices/enabled-all", data={"enabled": "1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("ok") is True
    assert "enabled" in body


# ---------------------------------------------------------------------------
# GET /api/offices/{office_id}/table-configs
# ---------------------------------------------------------------------------


def test_api_table_configs_returns_ok_json(client, office_id):
    resp = client.get(f"/api/offices/{office_id}/table-configs")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("ok") is True
    assert "table_configs" in body
    assert len(body["table_configs"]) >= 1


def test_api_table_configs_404_for_unknown(client):
    resp = client.get("/api/offices/999999/table-configs")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/table-configs/{table_config_id}
# ---------------------------------------------------------------------------


def test_api_table_config_get_returns_ok(client, table_config_id):
    resp = client.get(f"/api/table-configs/{table_config_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("ok") is True


def test_api_table_config_get_404_for_unknown(client):
    resp = client.get("/api/table-configs/999999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/pages/{source_page_id}/enabled
# ---------------------------------------------------------------------------


def test_api_page_enabled_returns_ok(client, source_page_id):
    resp = client.post(f"/api/pages/{source_page_id}/enabled", data={"enabled": "1"})
    assert resp.status_code == 200
    assert resp.json().get("ok") is True


def test_api_page_enabled_404_for_unknown(client):
    resp = client.post("/api/pages/999999/enabled", data={"enabled": "1"})
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/export-config
# ---------------------------------------------------------------------------


def test_api_export_config_returns_json_file(client):
    resp = client.get("/api/export-config")
    assert resp.status_code == 200
    assert "attachment" in resp.headers.get("content-disposition", "")


# ---------------------------------------------------------------------------
# GET /api/pages/{source_page_id}/export-config
# ---------------------------------------------------------------------------


def test_api_page_export_config_returns_json_file(client, source_page_id):
    resp = client.get(f"/api/pages/{source_page_id}/export-config")
    assert resp.status_code == 200
    assert "attachment" in resp.headers.get("content-disposition", "")


def test_api_page_export_config_404_for_unknown(client):
    resp = client.get("/api/pages/999999/export-config")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/offices/{office_id}/populate-terms
# ---------------------------------------------------------------------------


def test_api_populate_terms_returns_202_with_job_id(client, office_id):
    resp = client.post(f"/api/offices/{office_id}/populate-terms")
    assert resp.status_code == 202
    body = resp.json()
    assert "job_id" in body


def test_api_populate_terms_status_returns_known_job(client, office_id):
    launch = client.post(f"/api/offices/{office_id}/populate-terms")
    job_id = launch.json()["job_id"]
    resp = client.get(f"/api/offices/{office_id}/populate-terms/status/{job_id}")
    assert resp.status_code == 200
    assert resp.json().get("status") in ("running", "complete", "error", "cancelled")


def test_api_populate_terms_status_404_for_unknown_job(client, office_id):
    resp = client.get(f"/api/offices/{office_id}/populate-terms/status/nonexistent-job-id")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/offices/test-config (draft validation)
# ---------------------------------------------------------------------------


def test_api_test_config_draft_returns_ok_or_error_json(client):
    resp = client.post(
        "/api/offices/test-config",
        json={
            "url": "https://en.wikipedia.org/wiki/United_States_Senate",
            "table_no": 1,
            "table_rows": 4,
            "link_column": 1,
            "party_column": 0,
            "term_start_column": 4,
            "term_end_column": 5,
            "district_column": 0,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "ok" in body
    assert "message" in body


def test_api_test_config_draft_missing_url_returns_error(client):
    resp = client.post("/api/offices/test-config", json={})
    assert resp.status_code in (200, 400, 422)
    if resp.status_code == 200:
        assert resp.json().get("ok") is False or "error" in resp.json().get("message", "").lower()


# ---------------------------------------------------------------------------
# Deprecated endpoints return 410
# ---------------------------------------------------------------------------


def test_deprecated_set_infobox_role_key_returns_410(client, office_id):
    resp = client.post(f"/api/offices/{office_id}/set-infobox-role-key", json={})
    assert resp.status_code == 410


def test_deprecated_table_config_set_infobox_role_key_returns_410(client, table_config_id):
    resp = client.post(f"/api/table-configs/{table_config_id}/set-infobox-role-key", json={})
    assert resp.status_code == 410
