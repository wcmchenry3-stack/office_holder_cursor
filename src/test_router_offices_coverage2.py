"""Extended coverage tests for src/routers/offices.py — round 2.

Targets: table_delete, table_move, deprecated endpoints, table-config
endpoints, set-infobox-role-key-filter, test-config, populate-terms, and
find-matching-table routes.

Run: pytest src/test_router_offices_coverage2.py -v
"""
from __future__ import annotations

import importlib
import os
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from src.db import offices as db_offices
from src.db.connection import get_connection, init_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("offices_cov2_db")
    path = tmp / "offices_cov2_test.db"
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
def office_with_tc(db_path):
    """Create an office with an explicit table config. Returns (office_id, tc_id)."""
    conn = get_connection(db_path)
    try:
        country_id = conn.execute("SELECT id FROM countries LIMIT 1").fetchone()[0]
        oid = db_offices.create_office(
            {
                "country_id": country_id,
                "name": "TC Office",
                "url": "https://en.wikipedia.org/wiki/TC_Office_Cov2",
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
                        "enabled": True,
                    }
                ],
            },
            conn=conn,
        )
        conn.commit()
        office = db_offices.get_office(oid, conn=conn)
        tcs = office.get("table_configs") or []
        tc_id = tcs[0]["id"] if tcs else None
        return oid, tc_id
    finally:
        conn.close()


@pytest.fixture(scope="module")
def two_offices_same_page(db_path, client, office_with_tc):
    """
    Return (oid_a, tc_a, oid_b, tc_b) — two offices on the same source page,
    each with exactly one table config.
    """
    oid_a, tc_a = office_with_tc
    office_a = db_offices.get_office(oid_a)
    spid = office_a.get("source_page_id")

    # Add a second office to the same page via the API
    resp = client.post(
        "/offices/add-office-to-page",
        data={"source_page_id": str(spid)},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    location = resp.headers["location"]
    oid_b = int(location.split("/offices/")[1].split("?")[0].split("#")[0])

    office_b = db_offices.get_office(oid_b)
    tcs_b = office_b.get("table_configs") or []
    tc_b = tcs_b[0]["id"] if tcs_b else None
    return oid_a, tc_a, oid_b, tc_b


# ---------------------------------------------------------------------------
# Hierarchy list — limit/enabled params (lines 268-282)
# ---------------------------------------------------------------------------


def test_offices_list_hierarchy_with_limit_and_enabled(client):
    """GET /offices?limit=20&enabled=1 covers limit+enabled parsing in hierarchy branch."""
    resp = client.get("/offices?limit=20&enabled=1")
    assert resp.status_code == 200


def test_offices_list_hierarchy_with_invalid_limit(client):
    """Non-integer limit falls through gracefully (line 270)."""
    resp = client.get("/offices?limit=notanint")
    assert resp.status_code == 200


def test_offices_list_hierarchy_with_office_count(client):
    """office_count=gt0 covers line 282 normalisation."""
    resp = client.get("/offices?office_count=gt0")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# table_delete (lines 982-994)
# ---------------------------------------------------------------------------


def test_table_delete_success(client, office_with_tc):
    """POST /offices/{id}/table/{tc_id}/delete → 302 with saved=1 in redirect.
    Mock delete_table so we don't actually remove the tc."""
    oid, tc_id = office_with_tc
    if tc_id is None:
        pytest.skip("No tc_id available")
    with patch("src.routers.offices.db_offices.delete_table"):
        resp = client.post(f"/offices/{oid}/table/{tc_id}/delete", follow_redirects=False)
    assert resp.status_code == 302
    assert "saved=1" in resp.headers["location"]


def test_table_delete_with_return_query(client, office_with_tc):
    """return_query param is appended to the redirect URL (lines 987-993)."""
    oid, tc_id = office_with_tc
    if tc_id is None:
        pytest.skip("No tc_id available")
    # Mock delete so we don't actually remove the tc (other tests depend on it)
    with patch("src.routers.offices.db_offices.delete_table"):
        resp = client.post(
            f"/offices/{oid}/table/{tc_id}/delete",
            data={"return_query": "level_id=1"},
            follow_redirects=False,
        )
    assert resp.status_code == 302


# ---------------------------------------------------------------------------
# table_move (lines 1006-1026)
# ---------------------------------------------------------------------------


def test_table_move_would_be_empty_returns_409(client, two_offices_same_page):
    """Moving the only tc from an office → 409 requires_confirm (line 1011-1016)."""
    oid_a, tc_a, oid_b, tc_b = two_offices_same_page
    if tc_a is None or tc_b is None:
        pytest.skip("tc_a or tc_b not available")
    # Mock move_table to raise OFFICE_WOULD_BE_EMPTY so the test is deterministic
    with patch(
        "src.routers.offices.db_offices.move_table",
        side_effect=ValueError("OFFICE_WOULD_BE_EMPTY: TC Office"),
    ):
        resp = client.post(
            f"/offices/{oid_a}/table/{tc_a}/move",
            data={"to_office_id": str(oid_b), "delete_source_office_if_empty": ""},
        )
    assert resp.status_code == 409
    assert resp.json().get("requires_confirm") is True


def test_table_move_with_delete_flag(client, two_offices_same_page):
    """Moving with delete_source_office_if_empty=1 → JSON redirect (lines 1018-1026).
    Mock move_table so we avoid cross-page / table-number conflict issues."""
    oid_a, tc_a, oid_b, tc_b = two_offices_same_page
    if tc_a is None:
        pytest.skip("tc_a not available")
    with patch("src.routers.offices.db_offices.move_table"):
        resp = client.post(
            f"/offices/{oid_a}/table/{tc_a}/move",
            data={
                "to_office_id": str(oid_b),
                "delete_source_office_if_empty": "1",
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "redirect" in body


# ---------------------------------------------------------------------------
# office_duplicate — 404 (line 1034)
# ---------------------------------------------------------------------------


def test_office_duplicate_not_found(client):
    resp = client.post("/offices/999999/duplicate", follow_redirects=False)
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# api_office_table_configs — table_no filter (line 1120)
# ---------------------------------------------------------------------------


def test_api_office_table_configs_with_table_no_filter(client, office_with_tc):
    oid, _ = office_with_tc
    resp = client.get(f"/api/offices/{oid}/table-configs?table_no=1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    for tc in body["table_configs"]:
        assert int(tc["table_no"]) == 1


def test_api_office_table_configs_404(client):
    resp = client.get("/api/offices/999999/table-configs")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# api_office_set_infobox_role_key — deprecated 410 (line 1111)
# ---------------------------------------------------------------------------


def test_api_office_set_infobox_role_key_deprecated(client, office_with_tc):
    oid, _ = office_with_tc
    resp = client.post(f"/api/offices/{oid}/set-infobox-role-key", json={})
    assert resp.status_code == 410
    assert resp.json().get("deprecated") is True


# ---------------------------------------------------------------------------
# api_office_set_infobox_role_key_filter (lines 1156-1210)
# ---------------------------------------------------------------------------


def test_api_office_set_infobox_role_key_filter_office_not_found(client):
    resp = client.post(
        "/api/offices/999999/set-infobox-role-key-filter",
        json={"table_config_id": 1, "infobox_role_key_filter_id": None},
    )
    assert resp.status_code == 404


def test_api_office_set_infobox_role_key_filter_missing_tc_id(client, office_with_tc):
    oid, _ = office_with_tc
    resp = client.post(
        f"/api/offices/{oid}/set-infobox-role-key-filter",
        json={"infobox_role_key_filter_id": None},
    )
    assert resp.status_code == 400
    assert "table_config_id" in resp.json()["detail"]


def test_api_office_set_infobox_role_key_filter_bad_tc_id(client, office_with_tc):
    oid, _ = office_with_tc
    resp = client.post(
        f"/api/offices/{oid}/set-infobox-role-key-filter",
        json={"table_config_id": "not-int"},
    )
    assert resp.status_code == 400


def test_api_office_set_infobox_role_key_filter_tc_not_found(client, office_with_tc):
    oid, _ = office_with_tc
    resp = client.post(
        f"/api/offices/{oid}/set-infobox-role-key-filter",
        json={"table_config_id": 999999, "infobox_role_key_filter_id": None},
    )
    assert resp.status_code == 404


def test_api_office_set_infobox_role_key_filter_success(client, office_with_tc):
    oid, tc_id = office_with_tc
    if tc_id is None:
        pytest.skip("No tc_id available")
    resp = client.post(
        f"/api/offices/{oid}/set-infobox-role-key-filter",
        json={"table_config_id": tc_id, "infobox_role_key_filter_id": None},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True


# ---------------------------------------------------------------------------
# api_table_config_get (lines 1250-1270)
# ---------------------------------------------------------------------------


def test_api_table_config_get_not_found(client):
    resp = client.get("/api/table-configs/999999")
    assert resp.status_code == 404


def test_api_table_config_get_success(client, office_with_tc):
    _, tc_id = office_with_tc
    if tc_id is None:
        pytest.skip("No tc_id available")
    resp = client.get(f"/api/table-configs/{tc_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert "table_config" in body


# ---------------------------------------------------------------------------
# api_table_config_set_infobox_role_key — deprecated 410 (lines 1288-1292)
# ---------------------------------------------------------------------------


def test_api_table_config_set_infobox_role_key_deprecated(client, office_with_tc):
    _, tc_id = office_with_tc
    if tc_id is None:
        pytest.skip("No tc_id available")
    resp = client.post(f"/api/table-configs/{tc_id}/set-infobox-role-key", json={})
    assert resp.status_code == 410
    assert resp.json().get("deprecated") is True


# ---------------------------------------------------------------------------
# api_table_config_set_infobox_role_key_filter (lines 1315-1319)
# ---------------------------------------------------------------------------


def test_api_table_config_set_infobox_role_key_filter_not_found(client):
    resp = client.post(
        "/api/table-configs/999999/set-infobox-role-key-filter",
        json={"infobox_role_key_filter_id": None},
    )
    assert resp.status_code == 404


def test_api_table_config_set_infobox_role_key_filter_success(client, office_with_tc):
    _, tc_id = office_with_tc
    if tc_id is None:
        pytest.skip("No tc_id available")
    resp = client.post(
        f"/api/table-configs/{tc_id}/set-infobox-role-key-filter",
        json={"infobox_role_key_filter_id": None},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True


# ---------------------------------------------------------------------------
# api_office_test_config — GET saved config (lines 1286-1292)
# ---------------------------------------------------------------------------


def test_api_office_test_config_saved_not_found(client):
    resp = client.get("/api/offices/999999/test-config")
    assert resp.status_code == 404


def test_api_office_test_config_saved(client, office_with_tc):
    oid, _ = office_with_tc
    with patch("src.routers.offices.test_office_config", return_value=(True, "OK")):
        resp = client.get(f"/api/offices/{oid}/test-config")
    assert resp.status_code == 200
    body = resp.json()
    assert "ok" in body
    assert "message" in body


# ---------------------------------------------------------------------------
# office_update — tc_id/tc_table_no form fields (lines 869-870)
# ---------------------------------------------------------------------------


def test_office_update_with_tc_fields(client, office_with_tc):
    """POST /offices/{id} with tc_id/tc_table_no triggers _form_to_table_config."""
    oid, tc_id = office_with_tc
    if tc_id is None:
        pytest.skip("No tc_id available")
    office = db_offices.get_office(oid)
    resp = client.post(
        f"/offices/{oid}",
        data={
            "country_id": str(office["country_id"]),
            "name": office["name"],
            "url": office.get("url") or "",
            "enabled": "1",
            "action": "save_and_close",
            "tc_id": str(tc_id),
            "tc_table_no": "1",
            f"tc_enabled_{tc_id}": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302


def test_office_update_action_save_with_list_return(client, office_with_tc):
    """action=save + list_return_query → redirect includes list params (lines 946-950).
    On error the list_return_query still appears; check that regardless of save outcome."""
    oid, _ = office_with_tc
    office = db_offices.get_office(oid)
    resp = client.post(
        f"/offices/{oid}",
        data={
            "country_id": str(office["country_id"]),
            "name": office["name"],
            "url": office.get("url") or "",
            "enabled": "1",
            "action": "save",
            "list_return_query": "level_id=1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert "level_id=1" in resp.headers["location"]


# ---------------------------------------------------------------------------
# api_office_populate_terms — force_override JSON body (lines 1400-1405)
# ---------------------------------------------------------------------------


def test_api_office_populate_terms_with_force_override(client, office_with_tc):
    """JSON body with force_override=true is parsed (lines 1400-1405)."""
    oid, _ = office_with_tc
    with patch("src.routers.offices.run_with_db", return_value={"terms_parsed": 0, "office_count": 0}):
        resp = client.post(
            f"/api/offices/{oid}/populate-terms",
            json={"force_override": True},
            headers={"Content-Type": "application/json"},
        )
    assert resp.status_code == 202
    assert "job_id" in resp.json()


def test_api_office_populate_terms_not_found(client):
    resp = client.post("/api/offices/999999/populate-terms")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# api_office_populate_terms_status — office_id mismatch (line 1433)
# ---------------------------------------------------------------------------


def test_api_populate_terms_status_office_id_mismatch(client, office_with_tc):
    oid, _ = office_with_tc
    with patch("src.routers.offices.run_with_db", return_value={"terms_parsed": 0, "office_count": 0}):
        start = client.post(f"/api/offices/{oid}/populate-terms")
    job_id = start.json()["job_id"]
    # Poll with wrong office_id → 404 (line 1433)
    resp = client.get(f"/api/offices/999999/populate-terms/status/{job_id}")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# api_office_populate_terms_cancel (lines 1521-1530)
# ---------------------------------------------------------------------------


def test_api_populate_terms_cancel_not_found(client, office_with_tc):
    oid, _ = office_with_tc
    resp = client.post(f"/api/offices/{oid}/populate-terms/cancel/nonexistent-job")
    assert resp.status_code == 404


def test_api_populate_terms_cancel_running(client, office_with_tc):
    oid, _ = office_with_tc
    with patch("src.routers.offices.run_with_db", return_value={"terms_parsed": 0, "office_count": 0}):
        start = client.post(f"/api/offices/{oid}/populate-terms")
    job_id = start.json()["job_id"]
    resp = client.post(f"/api/offices/{oid}/populate-terms/cancel/{job_id}")
    assert resp.status_code in (200, 409)  # 409 if already done


# ---------------------------------------------------------------------------
# api_office_find_matching_table (lines 1453-1515)
# ---------------------------------------------------------------------------


def test_api_office_find_matching_table_not_found(client):
    resp = client.post("/api/offices/999999/find-matching-table", json={})
    assert resp.status_code == 404


def test_api_office_find_matching_table_no_existing_terms(client, office_with_tc):
    """With no existing terms, returns ok=False (line 1473)."""
    oid, _ = office_with_tc
    resp = client.post(f"/api/offices/{oid}/find-matching-table", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert "No existing terms" in body.get("message", "")


def test_api_office_find_matching_table_no_better_match(client, office_with_tc):
    """Mock returns no found_table_no → ok=False (lines 1477-1484)."""
    oid, _ = office_with_tc
    with patch("src.db.office_terms.get_existing_terms_for_office", return_value=[{"key": "t1"}]):
        with patch(
            "src.routers.offices.find_best_matching_table_for_existing_terms",
            return_value={"found_table_no": None, "missing_before": []},
        ):
            resp = client.post(f"/api/offices/{oid}/find-matching-table", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False


def test_api_office_find_matching_table_found(client, office_with_tc):
    """Mock returns found_table_no=2, confirm=false → ok=True updated=False (lines 1505-1514)."""
    oid, _ = office_with_tc
    with patch("src.db.office_terms.get_existing_terms_for_office", return_value=[{"key": "t1"}]):
        with patch(
            "src.routers.offices.find_best_matching_table_for_existing_terms",
            return_value={"found_table_no": 2, "missing_before": [], "missing_after": []},
        ):
            resp = client.post(f"/api/offices/{oid}/find-matching-table", json={"confirm": False})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["updated"] is False
    assert body["table_no"] == 2
