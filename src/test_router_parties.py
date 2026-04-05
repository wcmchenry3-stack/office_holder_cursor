"""Tests for src/routers/parties.py and src/db/parties.py.

Uses FastAPI TestClient with SQLite in-memory DB via init_db().

Run: pytest src/test_router_parties.py -v
"""

from __future__ import annotations

import importlib
import os

import pytest
from starlette.testclient import TestClient

from src.db import parties as db_parties
from src.db.connection import get_connection, init_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("parties_db")
    path = tmp / "parties_test.db"
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
def party_id(db_path):
    """Create a party for read/update/delete tests."""
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    conn = get_connection()
    try:
        pid = db_parties.create_party(
            {"country_id": 1, "party_name": "Fixture Party", "party_link": "/wiki/Fixture_Party"}
        )
        return pid
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /parties — list
# ---------------------------------------------------------------------------


def test_parties_list_returns_200(client):
    resp = client.get("/refs/parties")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_parties_list_with_saved_flag(client):
    resp = client.get("/refs/parties?saved=1")
    assert resp.status_code == 200


def test_parties_list_with_imported_flag(client):
    resp = client.get("/refs/parties?imported=1&count=3&errors=0")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /parties/import — import form
# ---------------------------------------------------------------------------


def test_parties_import_page_returns_200(client):
    resp = client.get("/refs/parties/import")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# POST /parties/import — file validation
# ---------------------------------------------------------------------------


def test_parties_import_no_file_returns_form_with_error(client):
    resp = client.post("/refs/parties/import", data={"mode": "append"})
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_parties_import_non_csv_returns_form_with_error(client):
    from io import BytesIO

    resp = client.post(
        "/refs/parties/import",
        data={"mode": "append"},
        files={"csv_file": ("parties.txt", BytesIO(b"Country,Party name\n"), "text/plain")},
    )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /parties/new — new party form
# ---------------------------------------------------------------------------


def test_party_new_form_returns_200(client):
    resp = client.get("/refs/parties/new")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# POST /parties/new — create party
# ---------------------------------------------------------------------------


def test_party_create_redirects_on_success(client):
    resp = client.post(
        "/refs/parties/new",
        data={
            "country_id": "1",
            "party_name": "New Test Party",
            "party_link": "/wiki/New_Test_Party",
        },
    )
    assert resp.status_code in (302, 200)


# ---------------------------------------------------------------------------
# GET /parties/{party_id} — edit form
# ---------------------------------------------------------------------------


def test_party_edit_page_returns_200(client, party_id):
    resp = client.get(f"/refs/parties/{party_id}")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_party_edit_page_404_for_unknown(client):
    resp = client.get("/refs/parties/999999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /parties/{party_id} — update party
# ---------------------------------------------------------------------------


def test_party_update_redirects_on_success(client, party_id):
    resp = client.post(
        f"/refs/parties/{party_id}",
        data={
            "country_id": "1",
            "party_name": "Updated Party",
            "party_link": "/wiki/Updated_Party",
        },
    )
    assert resp.status_code in (302, 200)


# ---------------------------------------------------------------------------
# POST /parties/{party_id}/delete — delete party
# ---------------------------------------------------------------------------


def test_party_delete_redirects(client, db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    pid = db_parties.create_party(
        {"country_id": 1, "party_name": "Delete Me", "party_link": "/wiki/Delete_Me"}
    )
    resp = client.post(f"/refs/parties/{pid}/delete")
    assert resp.status_code in (302, 200)


# ---------------------------------------------------------------------------
# db/parties.py unit tests
# ---------------------------------------------------------------------------


def test_db_list_parties_returns_list(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    result = db_parties.list_parties()
    assert isinstance(result, list)


def test_db_get_party_returns_dict(db_path, party_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    party = db_parties.get_party(party_id)
    assert party is not None
    assert party["id"] == party_id


def test_db_get_party_not_found_returns_none(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    assert db_parties.get_party(999999) is None


def test_db_create_party_no_country_raises(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    with pytest.raises(ValueError, match="country_id"):
        db_parties.create_party({"country_id": 0, "party_name": "x", "party_link": "/x"})


def test_db_update_party_no_country_raises(db_path, party_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    with pytest.raises(ValueError, match="country_id"):
        db_parties.update_party(party_id, {"country_id": 0, "party_name": "x"})


def test_db_update_party_returns_true(db_path, party_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    result = db_parties.update_party(
        party_id, {"country_id": 1, "party_name": "Updated", "party_link": "/wiki/Updated"}
    )
    assert result is True


def test_db_delete_party_returns_true(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    pid = db_parties.create_party(
        {"country_id": 1, "party_name": "To Delete", "party_link": "/wiki/To_Delete"}
    )
    result = db_parties.delete_party(pid)
    assert result is True


def test_db_delete_party_not_found_returns_false(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    result = db_parties.delete_party(999999)
    assert result is False


def test_db_get_party_list_for_scraper(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    result = db_parties.get_party_list_for_scraper()
    assert isinstance(result, dict)


def test_db_resolve_party_id_by_country_empty_name_returns_none(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    assert db_parties.resolve_party_id_by_country(1, "") is None


def test_db_resolve_party_id_by_country_zero_country_returns_none(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    assert db_parties.resolve_party_id_by_country(0, "Democratic") is None


def test_db_resolve_party_id_by_country_match(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    pid = db_parties.create_party(
        {"country_id": 1, "party_name": "Resolve Party", "party_link": "/wiki/Resolve"}
    )
    result = db_parties.resolve_party_id_by_country(1, "Resolve Party")
    assert result == pid


def test_db_resolve_party_id_no_match_returns_none(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    assert db_parties.resolve_party_id_by_country(1, "Nonexistent Party XYZ") is None
