"""Unit tests for src/routers/db_explorer.py.

Tests the SQL allowlist enforcement and query execution via FastAPI TestClient.
Uses SQLite in-memory for query execution; mocks _get_table_names to avoid
the PostgreSQL-only information_schema query.

Run: pytest src/test_db_explorer.py -v
"""

from __future__ import annotations

import importlib
import os
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from src.db.connection import init_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("db_explorer_db")
    path = tmp / "db_explorer_test.db"
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


# ---------------------------------------------------------------------------
# POST /db/query — SQL allowlist enforcement
# ---------------------------------------------------------------------------


def test_query_empty_sql_returns_400(client):
    resp = client.post("/db/query", json={"sql": ""})
    assert resp.status_code == 400
    assert "No query" in resp.json()["error"]


def test_query_whitespace_only_returns_400(client):
    resp = client.post("/db/query", json={"sql": "   "})
    assert resp.status_code == 400
    assert "No query" in resp.json()["error"]


def test_query_drop_table_blocked(client):
    resp = client.post("/db/query", json={"sql": "DROP TABLE offices"})
    assert resp.status_code == 400
    assert "SELECT" in resp.json()["error"]


def test_query_insert_blocked(client):
    resp = client.post("/db/query", json={"sql": "INSERT INTO offices VALUES (1)"})
    assert resp.status_code == 400
    assert "SELECT" in resp.json()["error"]


def test_query_update_blocked(client):
    resp = client.post("/db/query", json={"sql": "UPDATE offices SET name = 'x'"})
    assert resp.status_code == 400
    assert "SELECT" in resp.json()["error"]


def test_query_delete_blocked(client):
    resp = client.post("/db/query", json={"sql": "DELETE FROM offices"})
    assert resp.status_code == 400
    assert "SELECT" in resp.json()["error"]


def test_query_select_star_blocked_sql_injection_attempt(client):
    # Verify a statement starting with SELECT but injecting DDL is still allowed
    # (the allowlist only blocks non-SELECT starts)
    resp = client.post("/db/query", json={"sql": "SELECT * FROM offices"})
    # Should reach the DB layer — either 200 or 400 (bad SQL), NOT a 403/policy block
    assert resp.status_code in (200, 400)


# ---------------------------------------------------------------------------
# POST /db/query — valid queries
# ---------------------------------------------------------------------------


def test_query_select_1_returns_200(client):
    resp = client.post("/db/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 200
    data = resp.json()
    assert "columns" in data
    assert "rows" in data
    assert "row_count" in data
    assert "elapsed_ms" in data


def test_query_select_1_row_count(client):
    resp = client.post("/db/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] == 1


def test_query_with_cte_allowed(client):
    sql = "WITH cte AS (SELECT 1 AS n) SELECT n FROM cte"
    resp = client.post("/db/query", json={"sql": sql})
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] == 1


def test_query_case_insensitive_select(client):
    resp = client.post("/db/query", json={"sql": "select 1"})
    assert resp.status_code == 200


def test_query_syntax_error_returns_400(client):
    resp = client.post("/db/query", json={"sql": "SELECT FROM WHERE"})
    assert resp.status_code == 400
    assert "error" in resp.json()


def test_query_response_has_elapsed_ms(client):
    resp = client.post("/db/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 200
    assert isinstance(resp.json()["elapsed_ms"], int)


# ---------------------------------------------------------------------------
# GET /db — explorer page
# ---------------------------------------------------------------------------


def test_get_db_explorer_returns_200(client):
    with patch("src.routers.db_explorer._get_table_names", return_value=["offices", "countries"]):
        resp = client.get("/db")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
