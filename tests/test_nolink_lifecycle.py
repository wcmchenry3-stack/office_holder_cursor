# -*- coding: utf-8 -*-
"""Unit tests for no-link placeholder lifecycle (#214).

Tests cover:
- find_nolink_by_name_and_office: found, not found, case/whitespace normalization
- mark_superseded: office_terms reassigned, placeholder retired, log row written
- nolink_supersede_log CRUD: insert and list_recent
- _maybe_supersede_nolink: happy path, no-op when no placeholder found,
  error swallowed gracefully
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import individuals as db_individuals
from src.db import nolink_supersede_log as db_log

# ---------------------------------------------------------------------------
# SQLite fixture (no real DB / network required)
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS individuals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wiki_url TEXT NOT NULL UNIQUE,
    page_path TEXT,
    full_name TEXT,
    birth_date TEXT,
    death_date TEXT,
    birth_date_imprecise INTEGER NOT NULL DEFAULT 0,
    death_date_imprecise INTEGER NOT NULL DEFAULT 0,
    birth_place TEXT,
    death_place TEXT,
    is_dead_link INTEGER NOT NULL DEFAULT 0,
    is_living INTEGER NOT NULL DEFAULT 1,
    bio_batch INTEGER NOT NULL DEFAULT 0,
    bio_refreshed_at TEXT,
    insufficient_vitals_checked_at TEXT,
    gemini_research_checked_at TEXT,
    superseded_by_individual_id INTEGER REFERENCES individuals(id),
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS office_terms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    individual_id INTEGER REFERENCES individuals(id),
    office_id INTEGER,
    wiki_url TEXT,
    term_start_year INTEGER,
    term_end_year INTEGER
);

CREATE TABLE IF NOT EXISTS nolink_supersede_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    old_individual_id INTEGER NOT NULL REFERENCES individuals(id),
    new_individual_id INTEGER NOT NULL REFERENCES individuals(id),
    office_id INTEGER NOT NULL,
    old_wiki_url TEXT NOT NULL,
    new_wiki_url TEXT NOT NULL,
    office_terms_reassigned INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def _conn(tmp_path: Path):
    raw = sqlite3.connect(str(tmp_path / "test.db"))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _insert_individual(
    conn, wiki_url: str, full_name: str | None = None, is_dead_link: int = 0
) -> int:
    cur = conn.execute(
        "INSERT INTO individuals (wiki_url, full_name, is_dead_link) VALUES (%s, %s, %s) RETURNING id",
        (wiki_url, full_name, is_dead_link),
    )
    conn.commit()
    return cur.fetchone()[0]


def _insert_office_term(conn, individual_id: int, office_id: int = 1) -> int:
    cur = conn.execute(
        "INSERT INTO office_terms (individual_id, office_id) VALUES (%s, %s) RETURNING id",
        (individual_id, office_id),
    )
    conn.commit()
    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# find_nolink_by_name_and_office
# ---------------------------------------------------------------------------


class TestFindNolinkByNameAndOffice:
    def test_finds_exact_match(self, tmp_path):
        conn = _conn(tmp_path)
        _insert_individual(
            conn, "No link:94:William E. Woodruff, Jr.", "William E. Woodruff, Jr.", is_dead_link=1
        )
        result = db_individuals.find_nolink_by_name_and_office(
            94, "William E. Woodruff, Jr.", conn=conn
        )
        assert result is not None
        assert result["wiki_url"] == "No link:94:William E. Woodruff, Jr."

    def test_case_insensitive_match(self, tmp_path):
        conn = _conn(tmp_path)
        _insert_individual(conn, "No link:94:John Smith", "John Smith", is_dead_link=1)
        result = db_individuals.find_nolink_by_name_and_office(94, "JOHN SMITH", conn=conn)
        assert result is not None

    def test_whitespace_normalized_match(self, tmp_path):
        conn = _conn(tmp_path)
        _insert_individual(conn, "No link:94:John  Smith", "John  Smith", is_dead_link=1)
        result = db_individuals.find_nolink_by_name_and_office(94, "John Smith", conn=conn)
        assert result is not None

    def test_wrong_office_id_returns_none(self, tmp_path):
        conn = _conn(tmp_path)
        _insert_individual(conn, "No link:94:John Smith", "John Smith", is_dead_link=1)
        result = db_individuals.find_nolink_by_name_and_office(99, "John Smith", conn=conn)
        assert result is None

    def test_no_match_returns_none(self, tmp_path):
        conn = _conn(tmp_path)
        result = db_individuals.find_nolink_by_name_and_office(94, "Nobody Here", conn=conn)
        assert result is None

    def test_different_name_not_matched(self, tmp_path):
        conn = _conn(tmp_path)
        _insert_individual(conn, "No link:94:Jane Doe", "Jane Doe", is_dead_link=1)
        result = db_individuals.find_nolink_by_name_and_office(94, "John Doe", conn=conn)
        assert result is None


# ---------------------------------------------------------------------------
# mark_superseded
# ---------------------------------------------------------------------------


class TestMarkSuperseded:
    def test_reassigns_office_terms(self, tmp_path):
        conn = _conn(tmp_path)
        old_id = _insert_individual(conn, "No link:94:Jane Doe", "Jane Doe", is_dead_link=1)
        new_id = _insert_individual(conn, "https://en.wikipedia.org/wiki/Jane_Doe", "Jane Doe")
        _insert_office_term(conn, old_id, office_id=94)
        _insert_office_term(conn, old_id, office_id=94)

        reassigned = db_individuals.mark_superseded(old_id, new_id, conn=conn)

        assert reassigned == 2
        cur = conn.execute(
            "SELECT individual_id FROM office_terms WHERE individual_id = %s", (new_id,)
        )
        assert len(cur.fetchall()) == 2

    def test_marks_placeholder_retired(self, tmp_path):
        conn = _conn(tmp_path)
        old_id = _insert_individual(conn, "No link:94:Jane Doe", "Jane Doe", is_dead_link=1)
        new_id = _insert_individual(conn, "https://en.wikipedia.org/wiki/Jane_Doe", "Jane Doe")

        db_individuals.mark_superseded(old_id, new_id, conn=conn)

        cur = conn.execute(
            "SELECT is_dead_link, superseded_by_individual_id FROM individuals WHERE id = %s",
            (old_id,),
        )
        row = cur.fetchone()
        assert row[0] == 1
        assert row[1] == new_id

    def test_returns_zero_when_no_terms(self, tmp_path):
        conn = _conn(tmp_path)
        old_id = _insert_individual(conn, "No link:94:Jane Doe", "Jane Doe", is_dead_link=1)
        new_id = _insert_individual(conn, "https://en.wikipedia.org/wiki/Jane_Doe", "Jane Doe")
        reassigned = db_individuals.mark_superseded(old_id, new_id, conn=conn)
        assert reassigned == 0


# ---------------------------------------------------------------------------
# nolink_supersede_log CRUD
# ---------------------------------------------------------------------------


class TestNolinkSupersedeLog:
    def test_insert_and_list_recent(self, tmp_path):
        conn = _conn(tmp_path)
        old_id = _insert_individual(conn, "No link:94:Jane Doe", "Jane Doe", is_dead_link=1)
        new_id = _insert_individual(conn, "https://en.wikipedia.org/wiki/Jane_Doe", "Jane Doe")

        db_log.insert_log(
            old_individual_id=old_id,
            new_individual_id=new_id,
            office_id=94,
            old_wiki_url="No link:94:Jane Doe",
            new_wiki_url="https://en.wikipedia.org/wiki/Jane_Doe",
            office_terms_reassigned=2,
            conn=conn,
        )

        rows = db_log.list_recent(conn=conn)
        assert len(rows) == 1
        assert rows[0]["old_individual_id"] == old_id
        assert rows[0]["new_individual_id"] == new_id
        assert rows[0]["office_id"] == 94
        assert rows[0]["office_terms_reassigned"] == 2

    def test_list_recent_returns_newest_first(self, tmp_path):
        conn = _conn(tmp_path)
        old1 = _insert_individual(conn, "No link:94:Alice", "Alice", is_dead_link=1)
        old2 = _insert_individual(conn, "No link:94:Bob", "Bob", is_dead_link=1)
        new1 = _insert_individual(conn, "https://en.wikipedia.org/wiki/Alice", "Alice")
        new2 = _insert_individual(conn, "https://en.wikipedia.org/wiki/Bob", "Bob")

        db_log.insert_log(
            old1, new1, 94, "No link:94:Alice", "https://en.wikipedia.org/wiki/Alice", 1, conn=conn
        )
        db_log.insert_log(
            old2, new2, 94, "No link:94:Bob", "https://en.wikipedia.org/wiki/Bob", 1, conn=conn
        )

        rows = db_log.list_recent(conn=conn)
        # list_recent orders by id DESC — Bob was inserted last so it appears first
        assert rows[0]["old_wiki_url"] == "No link:94:Bob"
        assert rows[1]["old_wiki_url"] == "No link:94:Alice"

    def test_list_recent_respects_limit(self, tmp_path):
        conn = _conn(tmp_path)
        for i in range(5):
            old = _insert_individual(conn, f"No link:94:Person {i}", f"Person {i}", is_dead_link=1)
            new = _insert_individual(
                conn, f"https://en.wikipedia.org/wiki/Person_{i}", f"Person {i}"
            )
            db_log.insert_log(
                old,
                new,
                94,
                f"No link:94:Person {i}",
                f"https://en.wikipedia.org/wiki/Person_{i}",
                0,
                conn=conn,
            )

        rows = db_log.list_recent(limit=3, conn=conn)
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# _maybe_supersede_nolink (runner helper)
# ---------------------------------------------------------------------------


class TestMaybeSupersedNolink:
    def test_happy_path_supersedes_placeholder(self, tmp_path):
        from src.scraper.runner import _maybe_supersede_nolink

        conn = _conn(tmp_path)
        old_id = _insert_individual(
            conn, "No link:94:William Woodruff", "William Woodruff", is_dead_link=1
        )
        new_id = _insert_individual(
            conn, "https://en.wikipedia.org/wiki/William_Woodruff", "William Woodruff"
        )
        _insert_office_term(conn, old_id, office_id=94)

        _maybe_supersede_nolink(
            office_id=94,
            name="William Woodruff",
            new_individual_id=new_id,
            new_wiki_url="https://en.wikipedia.org/wiki/William_Woodruff",
            conn=conn,
        )

        # office_term reassigned
        cur = conn.execute("SELECT individual_id FROM office_terms")
        assert cur.fetchone()[0] == new_id

        # log written
        rows = db_log.list_recent(conn=conn)
        assert len(rows) == 1
        assert rows[0]["old_individual_id"] == old_id

    def test_no_op_when_no_placeholder(self, tmp_path):
        from src.scraper.runner import _maybe_supersede_nolink

        conn = _conn(tmp_path)
        new_id = _insert_individual(conn, "https://en.wikipedia.org/wiki/Nobody", "Nobody")

        # Should not raise
        _maybe_supersede_nolink(
            office_id=94,
            name="Nobody",
            new_individual_id=new_id,
            new_wiki_url="https://en.wikipedia.org/wiki/Nobody",
            conn=conn,
        )

        rows = db_log.list_recent(conn=conn)
        assert len(rows) == 0

    def test_exception_is_swallowed(self, tmp_path):
        from src.scraper.runner import _maybe_supersede_nolink

        conn = _conn(tmp_path)
        new_id = _insert_individual(conn, "https://en.wikipedia.org/wiki/Test", "Test")

        with patch.object(
            db_individuals, "find_nolink_by_name_and_office", side_effect=RuntimeError("db error")
        ):
            # Should not raise — errors are caught and logged
            _maybe_supersede_nolink(
                office_id=94,
                name="Test",
                new_individual_id=new_id,
                new_wiki_url="https://en.wikipedia.org/wiki/Test",
                conn=conn,
            )
