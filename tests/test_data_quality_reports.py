# -*- coding: utf-8 -*-
"""
Unit tests for data_quality_reports CRUD module.

Tests cover:
- Insert and find by fingerprint (round-trip)
- Duplicate fingerprint rejected (UNIQUE constraint)
- List recent reports (ordered, limited)
- Count by check type (aggregation)
- Fingerprint generation
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import data_quality_reports as dqr

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn(tmp_path: Path):
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS data_quality_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT NOT NULL UNIQUE,
            record_type TEXT NOT NULL,
            record_id INTEGER NOT NULL,
            check_type TEXT NOT NULL,
            flagged_by TEXT NOT NULL,
            concern_details TEXT,
            github_issue_url TEXT,
            github_issue_number INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Fingerprint generation
# ---------------------------------------------------------------------------


class TestMakeFingerprint:
    def test_format(self):
        fp = dqr.make_fingerprint("individual", 42, "bad_dates")
        assert fp.startswith("dq-")
        assert len(fp) == 19  # "dq-" + 16 hex chars

    def test_deterministic(self):
        fp1 = dqr.make_fingerprint("individual", 42, "bad_dates")
        fp2 = dqr.make_fingerprint("individual", 42, "bad_dates")
        assert fp1 == fp2

    def test_different_inputs_different_fingerprints(self):
        fp1 = dqr.make_fingerprint("individual", 42, "bad_dates")
        fp2 = dqr.make_fingerprint("individual", 43, "bad_dates")
        fp3 = dqr.make_fingerprint("office_term", 42, "bad_dates")
        assert fp1 != fp2
        assert fp1 != fp3


# ---------------------------------------------------------------------------
# CRUD: insert and find
# ---------------------------------------------------------------------------


class TestInsertAndFind:
    def test_insert_and_find_by_fingerprint(self, tmp_path):
        conn = _make_conn(tmp_path)
        fp = dqr.make_fingerprint("individual", 1, "bad_dates")
        row_id = dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=1,
            check_type="bad_dates",
            flagged_by="claude",
            concern_details="Birth date after death date",
            conn=conn,
        )
        assert row_id >= 1

        found = dqr.find_by_fingerprint(fp, conn=conn)
        assert found is not None
        assert found["fingerprint"] == fp
        assert found["record_type"] == "individual"
        assert found["record_id"] == 1
        assert found["check_type"] == "bad_dates"
        assert found["flagged_by"] == "claude"
        assert found["concern_details"] == "Birth date after death date"

    def test_find_missing_returns_none(self, tmp_path):
        conn = _make_conn(tmp_path)
        assert dqr.find_by_fingerprint("dq-nonexistent000000", conn=conn) is None

    def test_duplicate_fingerprint_rejected(self, tmp_path):
        conn = _make_conn(tmp_path)
        fp = dqr.make_fingerprint("individual", 1, "bad_dates")
        id1 = dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=1,
            check_type="bad_dates",
            flagged_by="claude",
            conn=conn,
        )
        assert id1 >= 1

        # Second insert with same fingerprint should be silently ignored
        id2 = dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=1,
            check_type="bad_dates",
            flagged_by="gemini",
            conn=conn,
        )
        assert id2 == 0  # ON CONFLICT DO NOTHING → no RETURNING row

    def test_insert_with_github_issue(self, tmp_path):
        conn = _make_conn(tmp_path)
        fp = dqr.make_fingerprint("individual", 5, "missing_wiki_url")
        row_id = dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=5,
            check_type="missing_wiki_url",
            flagged_by="openai",
            github_issue_url="https://github.com/org/repo/issues/42",
            github_issue_number=42,
            conn=conn,
        )
        found = dqr.find_by_fingerprint(fp, conn=conn)
        assert found["github_issue_url"] == "https://github.com/org/repo/issues/42"
        assert found["github_issue_number"] == 42


# ---------------------------------------------------------------------------
# List recent reports
# ---------------------------------------------------------------------------


class TestListRecentReports:
    def test_returns_ordered_results(self, tmp_path):
        conn = _make_conn(tmp_path)
        for i in range(5):
            fp = dqr.make_fingerprint("individual", i, "bad_dates")
            dqr.insert_report(
                fingerprint=fp,
                record_type="individual",
                record_id=i,
                check_type="bad_dates",
                flagged_by="claude",
                conn=conn,
            )

        reports = dqr.list_recent_reports(limit=3, conn=conn)
        assert len(reports) == 3

    def test_empty_table_returns_empty(self, tmp_path):
        conn = _make_conn(tmp_path)
        assert dqr.list_recent_reports(conn=conn) == []


# ---------------------------------------------------------------------------
# Count by check type
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# update_github_issue
# ---------------------------------------------------------------------------


class TestUpdateGithubIssue:
    def test_update_sets_url_and_number(self, tmp_path):
        conn = _make_conn(tmp_path)
        fp = dqr.make_fingerprint("individual", 7, "missing_name")
        dqr.insert_report(
            fingerprint=fp,
            record_type="individual",
            record_id=7,
            check_type="missing_name",
            flagged_by="openai",
            conn=conn,
        )
        updated = dqr.update_github_issue(
            fp,
            "https://github.com/org/repo/issues/77",
            77,
            conn=conn,
        )
        assert updated is True
        found = dqr.find_by_fingerprint(fp, conn=conn)
        assert found["github_issue_url"] == "https://github.com/org/repo/issues/77"
        assert found["github_issue_number"] == 77

    def test_update_missing_fingerprint_returns_false(self, tmp_path):
        conn = _make_conn(tmp_path)
        result = dqr.update_github_issue(
            "dq-nonexistent000000",
            "https://github.com/org/repo/issues/1",
            1,
            conn=conn,
        )
        assert result is False


class TestCountByCheckType:
    def test_aggregation(self, tmp_path):
        conn = _make_conn(tmp_path)
        for i, check in enumerate(["bad_dates", "bad_dates", "missing_wiki_url", "incomplete"]):
            fp = dqr.make_fingerprint("individual", i, check)
            dqr.insert_report(
                fingerprint=fp,
                record_type="individual",
                record_id=i,
                check_type=check,
                flagged_by="claude",
                conn=conn,
            )

        counts = dqr.count_by_check_type(conn=conn)
        assert counts["bad_dates"] == 2
        assert counts["missing_wiki_url"] == 1
        assert counts["incomplete"] == 1

    def test_empty_table_returns_empty_dict(self, tmp_path):
        conn = _make_conn(tmp_path)
        assert dqr.count_by_check_type(conn=conn) == {}
