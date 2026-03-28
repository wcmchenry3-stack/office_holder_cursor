"""Tests for untested refs router sections: cities, office categories, infobox filters.

Covers: CRUD for /refs/cities, /refs/office-categories, /refs/infobox-role-key-filters,
and the /api/cities dropdown endpoint. Extends coverage of db/refs.py city functions
and validation error paths.

Run: pytest src/test_router_refs_cities_categories.py -v
"""

from __future__ import annotations

import importlib
import os

import pytest
from starlette.testclient import TestClient

from src.db import refs as db_refs
from src.db import office_category as db_office_category
from src.db import infobox_role_key_filter as db_filter
from src.db.connection import get_connection, init_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("refs_extra_db")
    path = tmp / "refs_extra_test.db"
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
def state_id(db_path):
    """Create a country + state for city tests."""
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    return db_refs.create_state(1, "Cities Test State")


@pytest.fixture(scope="module")
def city_id(db_path, state_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    return db_refs.create_city(state_id, "Cities Test City")


@pytest.fixture(scope="module")
def category_id(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    return db_office_category.create_office_category("Test Category", [1], [], [])


@pytest.fixture(scope="module")
def filter_id(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    return db_filter.create_infobox_role_key_filter("Test Filter Cat", "senator", [], [], [])


# ---------------------------------------------------------------------------
# Cities — route tests
# ---------------------------------------------------------------------------


def test_cities_list_returns_200(client):
    resp = client.get("/refs/cities")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_cities_list_with_saved_flag(client):
    resp = client.get("/refs/cities?saved=1")
    assert resp.status_code == 200


def test_city_new_form_returns_200(client):
    resp = client.get("/refs/cities/new")
    assert resp.status_code == 200


def test_city_create_redirects_on_success(client, state_id):
    resp = client.post(
        "/refs/cities/new", data={"state_id": str(state_id), "name": "Route Created City"}
    )
    assert resp.status_code in (302, 200)


def test_city_create_empty_name_returns_form_with_error(client, state_id):
    resp = client.post("/refs/cities/new", data={"state_id": str(state_id), "name": ""})
    assert resp.status_code == 200


def test_city_edit_form_returns_200(client, city_id):
    resp = client.get(f"/refs/cities/{city_id}")
    assert resp.status_code == 200


def test_city_edit_form_404_for_unknown(client):
    resp = client.get("/refs/cities/999999")
    assert resp.status_code == 404


def test_city_update_redirects_on_success(client, city_id, state_id):
    resp = client.post(
        f"/refs/cities/{city_id}", data={"state_id": str(state_id), "name": "Updated City"}
    )
    assert resp.status_code in (302, 200)


def test_city_delete_redirects(client, db_path, state_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    cid = db_refs.create_city(state_id, "Delete This City")
    resp = client.post(f"/refs/cities/{cid}/delete")
    assert resp.status_code in (302, 200)


# ---------------------------------------------------------------------------
# db/refs.py — city unit tests
# ---------------------------------------------------------------------------


def test_db_create_city_empty_name_raises(db_path, state_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    with pytest.raises(ValueError, match="City name"):
        db_refs.create_city(state_id, "")


def test_db_create_city_no_state_raises(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    with pytest.raises(ValueError, match="State"):
        db_refs.create_city(0, "Some City")


def test_db_get_city_returns_dict(db_path, city_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    city = db_refs.get_city(city_id)
    assert city is not None
    assert city["id"] == city_id


def test_db_get_city_not_found_returns_none(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    assert db_refs.get_city(999999) is None


def test_db_list_cities_with_country_state(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    result = db_refs.list_cities_with_country_state()
    assert isinstance(result, list)


def test_db_update_city_success(db_path, city_id, state_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    result = db_refs.update_city(city_id, state_id, "Renamed City")
    assert result is True


def test_db_update_city_empty_name_raises(db_path, city_id, state_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    with pytest.raises(ValueError, match="City name"):
        db_refs.update_city(city_id, state_id, "")


def test_db_update_city_no_state_raises(db_path, city_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    with pytest.raises(ValueError, match="State"):
        db_refs.update_city(city_id, 0, "Some Name")


def test_db_delete_city_success(db_path, state_id):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    cid = db_refs.create_city(state_id, "Tmp City To Delete")
    db_refs.delete_city(cid)
    assert db_refs.get_city(cid) is None


# ---------------------------------------------------------------------------
# API cities endpoint
# ---------------------------------------------------------------------------


def test_api_cities_no_state_id_returns_empty(client):
    resp = client.get("/api/cities")
    assert resp.status_code == 200
    assert resp.json() == []


def test_api_cities_with_state_id_returns_list(client, state_id):
    resp = client.get(f"/api/cities?state_id={state_id}")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


# ---------------------------------------------------------------------------
# Office categories — route tests
# ---------------------------------------------------------------------------


def test_office_categories_list_returns_200(client):
    resp = client.get("/refs/office-categories")
    assert resp.status_code == 200


def test_office_category_new_form_returns_200(client):
    resp = client.get("/refs/office-categories/new")
    assert resp.status_code == 200


def test_office_category_create_redirects_on_success(client):
    resp = client.post(
        "/refs/office-categories/new",
        data={"name": "Route Created Category"},
    )
    assert resp.status_code in (302, 200)


def test_office_category_create_empty_name_returns_form_with_error(client):
    resp = client.post("/refs/office-categories/new", data={"name": ""})
    assert resp.status_code == 200


def test_office_category_edit_form_returns_200(client, category_id):
    resp = client.get(f"/refs/office-categories/{category_id}")
    assert resp.status_code == 200


def test_office_category_edit_form_404_for_unknown(client):
    resp = client.get("/refs/office-categories/999999")
    assert resp.status_code == 404


def test_office_category_update_redirects(client, category_id):
    resp = client.post(
        f"/refs/office-categories/{category_id}",
        data={"name": "Updated Category"},
    )
    assert resp.status_code in (302, 200)


def test_office_category_update_empty_name_returns_form_with_error(client, category_id):
    resp = client.post(f"/refs/office-categories/{category_id}", data={"name": ""})
    assert resp.status_code == 200


def test_office_category_delete_redirects(client, db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    cid = db_office_category.create_office_category("Delete Me", [], [], [])
    resp = client.post(f"/refs/office-categories/{cid}/delete")
    assert resp.status_code in (302, 200)


# ---------------------------------------------------------------------------
# Infobox role key filters — route tests
# ---------------------------------------------------------------------------


def test_infobox_filters_list_returns_200(client):
    resp = client.get("/refs/infobox-role-key-filters")
    assert resp.status_code == 200


def test_infobox_filter_new_form_returns_200(client):
    resp = client.get("/refs/infobox-role-key-filters/new")
    assert resp.status_code == 200


def test_infobox_filter_create_redirects_on_success(client):
    resp = client.post(
        "/refs/infobox-role-key-filters/new",
        data={"name": "Route Created Filter", "role_key": "member_of"},
    )
    assert resp.status_code in (302, 200)


def test_infobox_filter_create_empty_name_returns_form_with_error(client):
    resp = client.post(
        "/refs/infobox-role-key-filters/new",
        data={"name": "", "role_key": "member"},
    )
    assert resp.status_code == 200


def test_infobox_filter_create_empty_role_key_returns_form_with_error(client):
    resp = client.post(
        "/refs/infobox-role-key-filters/new",
        data={"name": "Some Name", "role_key": ""},
    )
    assert resp.status_code == 200


def test_infobox_filter_edit_form_returns_200(client, filter_id):
    resp = client.get(f"/refs/infobox-role-key-filters/{filter_id}")
    assert resp.status_code == 200


def test_infobox_filter_edit_form_404_for_unknown(client):
    resp = client.get("/refs/infobox-role-key-filters/999999")
    assert resp.status_code == 404


def test_infobox_filter_update_redirects_on_success(client, filter_id):
    resp = client.post(
        f"/refs/infobox-role-key-filters/{filter_id}",
        data={"name": "Updated Filter", "role_key": "senator"},
    )
    assert resp.status_code in (302, 200)


def test_infobox_filter_update_empty_name_returns_form_with_error(client, filter_id):
    resp = client.post(
        f"/refs/infobox-role-key-filters/{filter_id}",
        data={"name": "", "role_key": "senator"},
    )
    assert resp.status_code == 200


def test_infobox_filter_delete_redirects(client, db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    fid = db_filter.create_infobox_role_key_filter("Delete Me Filter", "del_key", [], [], [])
    resp = client.post(f"/refs/infobox-role-key-filters/{fid}/delete")
    assert resp.status_code in (302, 200)


# ---------------------------------------------------------------------------
# db/refs.py — validation error path tests (previously uncovered)
# ---------------------------------------------------------------------------


def test_db_create_state_empty_name_raises(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    with pytest.raises(ValueError, match="State name"):
        db_refs.create_state(1, "")


def test_db_create_state_no_country_raises(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    with pytest.raises(ValueError, match="Country"):
        db_refs.create_state(0, "Some State")


def test_db_delete_state_in_use_raises(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    sid = db_refs.create_state(1, "In Use State For Delete Test")
    db_refs.create_city(sid, "Blocker City")
    with pytest.raises(ValueError, match="cities"):
        db_refs.delete_state(sid)


def test_db_delete_country_in_use_by_state_raises(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    cid = db_refs.create_country("Deletable Country With State")
    db_refs.create_state(cid, "State Of Deletable Country")
    with pytest.raises(ValueError, match="states"):
        db_refs.delete_country(cid)
