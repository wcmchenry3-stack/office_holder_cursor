"""Unit tests for src/db/scraper_jobs.py.

Uses SQLite in-memory via init_db() — no PostgreSQL required.

Run: pytest src/db/test_scraper_jobs.py -v
"""

from __future__ import annotations

import time

import pytest

from src.db.connection import get_connection, init_db
from src.db import scraper_jobs as db_jobs


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("scraper_jobs_db")
    path = tmp / "scraper_jobs_test.db"
    init_db(path=path)
    return path


@pytest.fixture()
def conn(db_path):
    """Fresh connection per test; caller is responsible for commit/close."""
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    c = get_connection()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# create_job
# ---------------------------------------------------------------------------


def test_create_job_inserts_record(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-create-1", "scrape")

    c = get_connection()
    row = c.execute(
        "SELECT id, type, status FROM scraper_jobs WHERE id = %s", ("job-create-1",)
    ).fetchone()
    c.close()

    assert row is not None
    assert row[0] == "job-create-1"
    assert row[1] == "scrape"
    assert row[2] == "running"


def test_create_job_sets_running_status(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-create-status", "preview")

    c = get_connection()
    row = c.execute(
        "SELECT status FROM scraper_jobs WHERE id = %s", ("job-create-status",)
    ).fetchone()
    c.close()

    assert row[0] == "running"


def test_create_job_sets_timestamps(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-create-ts", "scrape")

    c = get_connection()
    row = c.execute(
        "SELECT created_at, updated_at FROM scraper_jobs WHERE id = %s", ("job-create-ts",)
    ).fetchone()
    c.close()

    assert row[0] is not None
    assert row[1] is not None


# ---------------------------------------------------------------------------
# update_job
# ---------------------------------------------------------------------------


def test_update_job_status(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-update-1", "scrape")
    db_jobs.update_job("job-update-1", "complete")

    c = get_connection()
    row = c.execute(
        "SELECT status FROM scraper_jobs WHERE id = %s", ("job-update-1",)
    ).fetchone()
    c.close()

    assert row[0] == "complete"


def test_update_job_with_result(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-update-result", "scrape")
    db_jobs.update_job("job-update-result", "complete", result={"rows": 42, "ok": True})

    c = get_connection()
    row = c.execute(
        "SELECT result_json FROM scraper_jobs WHERE id = %s", ("job-update-result",)
    ).fetchone()
    c.close()

    import json

    assert row[0] is not None
    data = json.loads(row[0])
    assert data["rows"] == 42
    assert data["ok"] is True


def test_update_job_cancelled(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-cancel-1", "scrape")
    db_jobs.update_job("job-cancel-1", "cancelled")

    c = get_connection()
    row = c.execute(
        "SELECT status FROM scraper_jobs WHERE id = %s", ("job-cancel-1",)
    ).fetchone()
    c.close()

    assert row[0] == "cancelled"


# ---------------------------------------------------------------------------
# get_job
# ---------------------------------------------------------------------------


def test_get_job_returns_dict(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-get-1", "preview")
    record = db_jobs.get_job("job-get-1")

    assert record is not None
    assert record["id"] == "job-get-1"
    assert record["type"] == "preview"
    assert record["status"] == "running"


def test_get_job_not_found_returns_none(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    assert db_jobs.get_job("nonexistent-job-id-xyz") is None


def test_get_job_deserializes_result(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-get-result", "scrape")
    db_jobs.update_job("job-get-result", "complete", result={"count": 7})

    record = db_jobs.get_job("job-get-result")

    assert record is not None
    assert "result" in record
    assert record["result"]["count"] == 7
    assert "result_json" not in record


def test_get_job_no_result_json_key(db_path):
    """result_json key must be removed from returned dict."""
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-no-resultkey", "scrape")
    record = db_jobs.get_job("job-no-resultkey")

    assert record is not None
    assert "result_json" not in record


def test_get_job_malformed_result_json(db_path):
    """Corrupt result_json must not raise — result should be None."""
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-bad-json", "scrape")

    # Inject malformed JSON directly
    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET result_json = %s WHERE id = %s",
        ("{not valid json}", "job-bad-json"),
    )
    c.commit()
    c.close()

    record = db_jobs.get_job("job-bad-json")
    assert record is not None
    assert record.get("result") is None


# ---------------------------------------------------------------------------
# list_recent_jobs
# ---------------------------------------------------------------------------


def test_list_recent_jobs_returns_list(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-list-1", "scrape")
    db_jobs.create_job("job-list-2", "preview")

    results = db_jobs.list_recent_jobs()
    assert isinstance(results, list)
    assert len(results) >= 2


def test_list_recent_jobs_respects_limit(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    # Ensure at least 3 jobs exist
    for i in range(3):
        db_jobs.create_job(f"job-limit-{i}", "scrape")

    results = db_jobs.list_recent_jobs(limit=2)
    assert len(results) <= 2


def test_list_recent_jobs_contains_expected_keys(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-list-keys", "scrape")

    results = db_jobs.list_recent_jobs(limit=1)
    assert results
    record = results[0]
    for key in ("id", "type", "status", "created_at", "updated_at"):
        assert key in record


# ---------------------------------------------------------------------------
# delete_jobs_older_than
# ---------------------------------------------------------------------------


def test_delete_jobs_older_than_removes_completed(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-delete-complete", "scrape")
    db_jobs.update_job("job-delete-complete", "complete")

    # Set created_at to 3 days ago
    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET created_at = %s WHERE id = %s",
        ("2000-01-01T00:00:00Z", "job-delete-complete"),
    )
    c.commit()
    c.close()

    deleted = db_jobs.delete_jobs_older_than(hours=1)
    assert deleted >= 1

    assert db_jobs.get_job("job-delete-complete") is None


def test_delete_jobs_older_than_preserves_running(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-delete-running", "scrape")

    # Set created_at to far in the past but leave status=running
    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET created_at = %s WHERE id = %s",
        ("2000-01-01T00:00:00Z", "job-delete-running"),
    )
    c.commit()
    c.close()

    db_jobs.delete_jobs_older_than(hours=1)

    # Running job must survive
    assert db_jobs.get_job("job-delete-running") is not None


def test_delete_jobs_older_than_returns_count(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-count-del", "scrape")
    db_jobs.update_job("job-count-del", "error")

    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET created_at = %s WHERE id = %s",
        ("2000-01-01T00:00:00Z", "job-count-del"),
    )
    c.commit()
    c.close()

    deleted = db_jobs.delete_jobs_older_than(hours=1)
    assert isinstance(deleted, int)
    assert deleted >= 1
