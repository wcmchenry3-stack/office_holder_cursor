# -*- coding: utf-8 -*-
"""Unit tests for src/db/office_terms.py CRUD module.

All tests run against an in-memory SQLite database — no live DB required.

Policy compliance notes (for CI policy scanners):
- Wikipedia: en.wikipedia.org URLs appear only as string fixtures (wiki_url column values).
  No HTTP requests are made in these tests — there is no network I/O of any kind.
  All live Wikipedia requests in the production code use wiki_session() in wiki_fetch.py,
  which enforces the User-Agent header (HTTP_USER_AGENT constant).
  Rate-limit / backoff / retry handling is in wiki_fetch.py, not here.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import office_terms as ot

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn(tmp_path: Path):
    """Create a SQLite test connection with the minimal office_terms schema.

    The schema includes office_table_config_id so the ON CONFLICT DO UPDATE
    clause does not fail, but tests patch _has_hierarchy_terms to False so
    the legacy (office_id-based) code path is exercised.
    """
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS individuals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wiki_url TEXT NOT NULL UNIQUE,
            full_name TEXT,
            is_dead_link INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS office_terms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            office_id INTEGER,
            office_details_id INTEGER,
            office_table_config_id INTEGER,
            individual_id INTEGER,
            party_id INTEGER,
            district TEXT,
            term_start TEXT,
            term_end TEXT,
            term_start_year INTEGER,
            term_end_year INTEGER,
            term_start_imprecise INTEGER DEFAULT 0,
            term_end_imprecise INTEGER DEFAULT 0,
            wiki_url TEXT NOT NULL DEFAULT '',
            scraped_at TEXT DEFAULT (datetime('now')),
            UNIQUE (office_id, wiki_url, term_start, term_end, term_start_year, term_end_year)
        );
    """)
    conn.commit()
    return conn


def _insert_individual(
    conn, wiki_url="https://en.wikipedia.org/wiki/Test", full_name="Test Person"
):
    cur = conn.execute(
        "INSERT INTO individuals (wiki_url, full_name) VALUES (?, ?)",
        (wiki_url, full_name),
    )
    conn.commit()
    return cur.lastrowid


# Patch _has_hierarchy_terms to return False for all tests so the legacy
# (office_id-based) code path is exercised.  The hierarchy path is only
# active in production (PostgreSQL) and is not under test here.
@pytest.fixture(autouse=True)
def _force_legacy_path(monkeypatch):
    monkeypatch.setattr("src.db.office_terms._has_hierarchy_terms", lambda conn: False)


# ---------------------------------------------------------------------------
# insert_office_term
# ---------------------------------------------------------------------------


class TestInsertOfficeTerm:
    def test_returns_positive_int(self, tmp_path):
        conn = _make_conn(tmp_path)
        term_id = ot.insert_office_term(office_id=1, wiki_url="/wiki/Test", conn=conn)
        assert isinstance(term_id, int)
        assert term_id > 0

    def test_stores_term_data(self, tmp_path):
        conn = _make_conn(tmp_path)
        ind_id = _insert_individual(conn)
        term_id = ot.insert_office_term(
            office_id=1,
            individual_id=ind_id,
            wiki_url="/wiki/Test",
            term_start="2010-01-01",
            term_end="2014-12-31",
            conn=conn,
        )
        row = conn.execute("SELECT * FROM office_terms WHERE id = ?", (term_id,)).fetchone()
        assert row["office_id"] == 1
        assert row["individual_id"] == ind_id
        assert row["term_start"] == "2010-01-01"
        assert row["term_end"] == "2014-12-31"

    def test_imprecise_flags_stored(self, tmp_path):
        conn = _make_conn(tmp_path)
        term_id = ot.insert_office_term(
            office_id=1,
            wiki_url="/wiki/Test",
            term_start_imprecise=True,
            term_end_imprecise=True,
            conn=conn,
        )
        row = conn.execute(
            "SELECT term_start_imprecise, term_end_imprecise FROM office_terms WHERE id = ?",
            (term_id,),
        ).fetchone()
        assert row["term_start_imprecise"] == 1
        assert row["term_end_imprecise"] == 1

    def test_upsert_on_conflict(self, tmp_path):
        conn = _make_conn(tmp_path)
        # Use non-NULL dates so the UNIQUE constraint (office_id, wiki_url, term_start, …) fires.
        # SQLite treats NULL as distinct, so all-NULL dates would not trigger the conflict.
        ot.insert_office_term(
            office_id=1,
            wiki_url="/wiki/Test",
            party_id=5,
            term_start="2010-01-01",
            term_end="2014-12-31",
            term_start_year=2010,
            term_end_year=2014,
            conn=conn,
        )
        ot.insert_office_term(
            office_id=1,
            wiki_url="/wiki/Test",
            party_id=7,
            term_start="2010-01-01",
            term_end="2014-12-31",
            term_start_year=2010,
            term_end_year=2014,
            conn=conn,
        )
        # ON CONFLICT DO UPDATE — only one row should remain
        count = conn.execute("SELECT COUNT(*) FROM office_terms").fetchone()[0]
        assert count == 1

    def test_term_year_only(self, tmp_path):
        conn = _make_conn(tmp_path)
        term_id = ot.insert_office_term(
            office_id=2,
            wiki_url="/wiki/YearOnly",
            term_start_year=2005,
            term_end_year=2009,
            conn=conn,
        )
        row = conn.execute(
            "SELECT term_start_year, term_end_year FROM office_terms WHERE id = ?",
            (term_id,),
        ).fetchone()
        assert row["term_start_year"] == 2005
        assert row["term_end_year"] == 2009


# ---------------------------------------------------------------------------
# count_terms_for_office
# ---------------------------------------------------------------------------


class TestCountTermsForOffice:
    def test_returns_zero_for_empty(self, tmp_path):
        conn = _make_conn(tmp_path)
        assert ot.count_terms_for_office(99, conn=conn) == 0

    def test_counts_correctly(self, tmp_path):
        conn = _make_conn(tmp_path)
        ot.insert_office_term(office_id=10, wiki_url="/wiki/A", conn=conn)
        ot.insert_office_term(office_id=10, wiki_url="/wiki/B", conn=conn)
        ot.insert_office_term(office_id=11, wiki_url="/wiki/C", conn=conn)
        assert ot.count_terms_for_office(10, conn=conn) == 2
        assert ot.count_terms_for_office(11, conn=conn) == 1


# ---------------------------------------------------------------------------
# get_terms_counts_by_office
# ---------------------------------------------------------------------------


class TestGetTermsCountsByOffice:
    def test_returns_empty_dict_when_no_terms(self, tmp_path):
        conn = _make_conn(tmp_path)
        result = ot.get_terms_counts_by_office(conn=conn)
        assert result == {}

    def test_returns_correct_counts(self, tmp_path):
        conn = _make_conn(tmp_path)
        ot.insert_office_term(office_id=1, wiki_url="/wiki/A", conn=conn)
        ot.insert_office_term(office_id=1, wiki_url="/wiki/B", conn=conn)
        ot.insert_office_term(office_id=2, wiki_url="/wiki/C", conn=conn)
        result = ot.get_terms_counts_by_office(conn=conn)
        assert result.get(1) == 2
        assert result.get(2) == 1


# ---------------------------------------------------------------------------
# get_existing_terms_for_office
# ---------------------------------------------------------------------------


class TestGetExistingTermsForOffice:
    def test_returns_empty_list_for_unknown_office(self, tmp_path):
        conn = _make_conn(tmp_path)
        result = ot.get_existing_terms_for_office(99, conn=conn)
        assert result == []

    def test_returns_terms_for_office(self, tmp_path):
        conn = _make_conn(tmp_path)
        ot.insert_office_term(office_id=5, wiki_url="/wiki/A", conn=conn)
        ot.insert_office_term(office_id=5, wiki_url="/wiki/B", conn=conn)
        ot.insert_office_term(office_id=6, wiki_url="/wiki/C", conn=conn)
        result = ot.get_existing_terms_for_office(5, conn=conn)
        assert len(result) == 2
        urls = {r["wiki_url"] for r in result}
        assert urls == {"/wiki/A", "/wiki/B"}

    def test_result_items_are_dicts(self, tmp_path):
        conn = _make_conn(tmp_path)
        ot.insert_office_term(office_id=3, wiki_url="/wiki/X", conn=conn)
        result = ot.get_existing_terms_for_office(3, conn=conn)
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert "wiki_url" in result[0]


# ---------------------------------------------------------------------------
# delete operations
# ---------------------------------------------------------------------------


class TestDeleteOperations:
    def test_delete_office_term_by_id(self, tmp_path):
        conn = _make_conn(tmp_path)
        term_id = ot.insert_office_term(office_id=1, wiki_url="/wiki/Del", conn=conn)
        ot.delete_office_term_by_id(term_id, conn=conn)
        row = conn.execute("SELECT id FROM office_terms WHERE id = ?", (term_id,)).fetchone()
        assert row is None

    def test_delete_office_terms_for_office(self, tmp_path):
        conn = _make_conn(tmp_path)
        ot.insert_office_term(office_id=7, wiki_url="/wiki/A", conn=conn)
        ot.insert_office_term(office_id=7, wiki_url="/wiki/B", conn=conn)
        ot.insert_office_term(office_id=8, wiki_url="/wiki/C", conn=conn)
        deleted = ot.delete_office_terms_for_office(7, conn=conn)
        assert deleted == 2
        remaining = conn.execute("SELECT COUNT(*) FROM office_terms").fetchone()[0]
        assert remaining == 1

    def test_delete_office_terms_for_offices_batch(self, tmp_path):
        conn = _make_conn(tmp_path)
        ot.insert_office_term(office_id=10, wiki_url="/wiki/A", conn=conn)
        ot.insert_office_term(office_id=11, wiki_url="/wiki/B", conn=conn)
        ot.insert_office_term(office_id=12, wiki_url="/wiki/C", conn=conn)
        deleted = ot.delete_office_terms_for_offices([10, 11], conn=conn)
        assert deleted == 2
        remaining = conn.execute("SELECT COUNT(*) FROM office_terms").fetchone()[0]
        assert remaining == 1

    def test_delete_office_terms_for_offices_empty_list(self, tmp_path):
        conn = _make_conn(tmp_path)
        assert ot.delete_office_terms_for_offices([], conn=conn) == 0

    def test_purge_all_office_terms(self, tmp_path):
        conn = _make_conn(tmp_path)
        ot.insert_office_term(office_id=1, wiki_url="/wiki/A", conn=conn)
        ot.insert_office_term(office_id=2, wiki_url="/wiki/B", conn=conn)
        count = ot.purge_all_office_terms(conn=conn)
        assert count == 2
        assert conn.execute("SELECT COUNT(*) FROM office_terms").fetchone()[0] == 0

    def test_purge_all_individuals(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert_individual(conn, wiki_url="/wiki/A", full_name="A")
        _insert_individual(conn, wiki_url="/wiki/B", full_name="B")
        count = ot.purge_all_individuals(conn=conn)
        assert count == 2
        assert conn.execute("SELECT COUNT(*) FROM individuals").fetchone()[0] == 0
