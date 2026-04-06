# -*- coding: utf-8 -*-
"""Unit tests for src/db/parse_errors.py CRUD functions."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import parse_errors as db_pe


def _make_conn(tmp_path: Path):
    raw = sqlite3.connect(str(tmp_path / "test.db"))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS parse_error_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT NOT NULL UNIQUE,
            function_name TEXT NOT NULL,
            error_type TEXT NOT NULL,
            wiki_url TEXT,
            office_name TEXT,
            github_issue_url TEXT,
            github_issue_number INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# find_by_fingerprint
# ---------------------------------------------------------------------------


def test_find_by_fingerprint_returns_none_when_missing(tmp_path):
    conn = _make_conn(tmp_path)
    assert db_pe.find_by_fingerprint("pe-nonexistent", conn=conn) is None


def test_find_by_fingerprint_returns_dict_after_insert(tmp_path):
    conn = _make_conn(tmp_path)
    db_pe.insert_report(
        fingerprint="pe-abc123",
        function_name="parse_table",
        error_type="KeyError",
        wiki_url="https://en.wikipedia.org/wiki/Test",
        office_name="Mayor",
        github_issue_url=None,
        github_issue_number=None,
        conn=conn,
    )
    row = db_pe.find_by_fingerprint("pe-abc123", conn=conn)
    assert row is not None
    assert row["fingerprint"] == "pe-abc123"
    assert row["function_name"] == "parse_table"
    assert row["error_type"] == "KeyError"
    assert row["wiki_url"] == "https://en.wikipedia.org/wiki/Test"
    assert row["office_name"] == "Mayor"


# ---------------------------------------------------------------------------
# insert_report
# ---------------------------------------------------------------------------


def test_insert_report_roundtrip(tmp_path):
    conn = _make_conn(tmp_path)
    db_pe.insert_report(
        fingerprint="pe-xyz",
        function_name="parse_infobox",
        error_type="AttributeError",
        wiki_url=None,
        office_name="Governor",
        github_issue_url="https://github.com/org/repo/issues/10",
        github_issue_number=10,
        conn=conn,
    )
    row = db_pe.find_by_fingerprint("pe-xyz", conn=conn)
    assert row["github_issue_url"] == "https://github.com/org/repo/issues/10"
    assert row["github_issue_number"] == 10


def test_insert_report_duplicate_is_silently_ignored(tmp_path):
    conn = _make_conn(tmp_path)
    db_pe.insert_report(
        fingerprint="pe-dup",
        function_name="f",
        error_type="ValueError",
        wiki_url=None,
        office_name=None,
        github_issue_url=None,
        github_issue_number=None,
        conn=conn,
    )
    # Second insert with same fingerprint — should not raise
    db_pe.insert_report(
        fingerprint="pe-dup",
        function_name="f2",
        error_type="RuntimeError",
        wiki_url=None,
        office_name=None,
        github_issue_url=None,
        github_issue_number=None,
        conn=conn,
    )
    # Should still have original data
    row = db_pe.find_by_fingerprint("pe-dup", conn=conn)
    assert row["function_name"] == "f"


# ---------------------------------------------------------------------------
# list_recent_reports
# ---------------------------------------------------------------------------


def test_list_recent_reports_empty(tmp_path):
    conn = _make_conn(tmp_path)
    assert db_pe.list_recent_reports(conn=conn) == []


def test_list_recent_reports_returns_all_inserted(tmp_path):
    conn = _make_conn(tmp_path)
    for i in range(3):
        db_pe.insert_report(
            fingerprint=f"pe-{i}",
            function_name="fn",
            error_type="E",
            wiki_url=None,
            office_name=None,
            github_issue_url=None,
            github_issue_number=None,
            conn=conn,
        )
    rows = db_pe.list_recent_reports(conn=conn)
    assert len(rows) == 3
    assert all("fingerprint" in r for r in rows)


def test_list_recent_reports_limit(tmp_path):
    conn = _make_conn(tmp_path)
    for i in range(5):
        db_pe.insert_report(
            fingerprint=f"pe-lim-{i}",
            function_name="fn",
            error_type="E",
            wiki_url=None,
            office_name=None,
            github_issue_url=None,
            github_issue_number=None,
            conn=conn,
        )
    rows = db_pe.list_recent_reports(limit=2, conn=conn)
    assert len(rows) == 2
