# -*- coding: utf-8 -*-
"""Tests for list_individuals() filtering and /data/individuals route filters."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import individuals as db_individuals

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn(tmp_path: Path):
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS individuals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wiki_url TEXT,
            page_path TEXT,
            full_name TEXT,
            birth_date TEXT,
            death_date TEXT,
            birth_date_imprecise INTEGER NOT NULL DEFAULT 0,
            death_date_imprecise INTEGER NOT NULL DEFAULT 0,
            birth_place TEXT,
            death_place TEXT,
            is_living INTEGER NOT NULL DEFAULT 1,
            is_dead_link INTEGER NOT NULL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        );
    """)
    conn.commit()
    return conn


def _insert(conn, full_name, is_living=1, is_dead_link=0, wiki_url=None):
    conn.execute(
        "INSERT INTO individuals (full_name, is_living, is_dead_link, wiki_url) VALUES (?, ?, ?, ?)",
        (full_name, is_living, is_dead_link, wiki_url),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# list_individuals — no filters
# ---------------------------------------------------------------------------


class TestListIndividualsNoFilter:
    def test_returns_all_rows(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith")
        _insert(conn, "Bob Jones")
        rows = db_individuals.list_individuals(conn=conn)
        assert len(rows) == 2

    def test_is_living_included_in_result(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith", is_living=1)
        rows = db_individuals.list_individuals(conn=conn)
        assert "is_living" in rows[0]

    def test_is_dead_link_included_in_result(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith", is_dead_link=1)
        rows = db_individuals.list_individuals(conn=conn)
        assert "is_dead_link" in rows[0]


# ---------------------------------------------------------------------------
# list_individuals — q filter
# ---------------------------------------------------------------------------


class TestListIndividualsQFilter:
    def test_partial_name_match(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith")
        _insert(conn, "Bob Jones")
        rows = db_individuals.list_individuals(q="alice", conn=conn)
        assert len(rows) == 1
        assert rows[0]["full_name"] == "Alice Smith"

    def test_case_insensitive(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith")
        rows = db_individuals.list_individuals(q="ALICE", conn=conn)
        assert len(rows) == 1

    def test_no_match_returns_empty(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith")
        rows = db_individuals.list_individuals(q="zzz", conn=conn)
        assert rows == []

    def test_empty_q_returns_all(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith")
        _insert(conn, "Bob Jones")
        rows = db_individuals.list_individuals(q="", conn=conn)
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# list_individuals — is_living filter
# ---------------------------------------------------------------------------


class TestListIndividualsIsLivingFilter:
    def test_living_only(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice", is_living=1)
        _insert(conn, "Bob", is_living=0)
        rows = db_individuals.list_individuals(is_living=1, conn=conn)
        assert len(rows) == 1
        assert rows[0]["full_name"] == "Alice"

    def test_deceased_only(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice", is_living=1)
        _insert(conn, "Bob", is_living=0)
        rows = db_individuals.list_individuals(is_living=0, conn=conn)
        assert len(rows) == 1
        assert rows[0]["full_name"] == "Bob"


# ---------------------------------------------------------------------------
# list_individuals — is_dead_link filter
# ---------------------------------------------------------------------------


class TestListIndividualsDeadLinkFilter:
    def test_dead_links_only(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice", is_dead_link=0)
        _insert(conn, "No link", is_dead_link=1)
        rows = db_individuals.list_individuals(is_dead_link=1, conn=conn)
        assert len(rows) == 1
        assert rows[0]["full_name"] == "No link"


# ---------------------------------------------------------------------------
# list_individuals — composed filters
# ---------------------------------------------------------------------------


class TestListIndividualsComposedFilters:
    def test_q_and_is_living(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith", is_living=1)
        _insert(conn, "Alice Brown", is_living=0)
        _insert(conn, "Bob Jones", is_living=1)
        rows = db_individuals.list_individuals(q="alice", is_living=1, conn=conn)
        assert len(rows) == 1
        assert rows[0]["full_name"] == "Alice Smith"

    def test_q_and_is_dead_link(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, "Alice Smith", is_dead_link=0)
        _insert(conn, "Alice No Link", is_dead_link=1)
        rows = db_individuals.list_individuals(q="alice", is_dead_link=1, conn=conn)
        assert len(rows) == 1
        assert rows[0]["full_name"] == "Alice No Link"


# ---------------------------------------------------------------------------
# Route integration tests — filter params accepted, reflected in response
# ---------------------------------------------------------------------------


import os
from starlette.testclient import TestClient


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("ind_filter_db")
    db_path = tmp / "ind_filter_test.db"
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    import src.main as main_mod

    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c
    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)


def test_route_no_filters_200(client):
    resp = client.get("/data/individuals")
    assert resp.status_code == 200


def test_route_q_param_accepted(client):
    resp = client.get("/data/individuals?q=alice")
    assert resp.status_code == 200


def test_route_is_living_param_accepted(client):
    resp = client.get("/data/individuals?is_living=1")
    assert resp.status_code == 200


def test_route_is_dead_link_param_accepted(client):
    resp = client.get("/data/individuals?is_dead_link=1")
    assert resp.status_code == 200


def test_route_composed_filters_accepted(client):
    resp = client.get("/data/individuals?q=smith&is_living=1&is_dead_link=0")
    assert resp.status_code == 200


def test_route_filter_form_present_in_html(client):
    resp = client.get("/data/individuals")
    assert resp.status_code == 200
    assert 'name="q"' in resp.text
    assert 'name="is_living"' in resp.text
    assert 'name="is_dead_link"' in resp.text


def test_route_q_value_reflected_in_html(client):
    resp = client.get("/data/individuals?q=testname")
    assert "testname" in resp.text
