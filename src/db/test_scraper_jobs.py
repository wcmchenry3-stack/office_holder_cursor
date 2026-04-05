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
    row = c.execute("SELECT status FROM scraper_jobs WHERE id = %s", ("job-update-1",)).fetchone()
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
    row = c.execute("SELECT status FROM scraper_jobs WHERE id = %s", ("job-cancel-1",)).fetchone()
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


# ---------------------------------------------------------------------------
# enqueue_job
# ---------------------------------------------------------------------------


def test_enqueue_job_inserts_queued_status(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.enqueue_job("job-enqueue-1", "scrape", '{"run_mode": "delta"}')

    record = db_jobs.get_job("job-enqueue-1")
    assert record is not None
    assert record["status"] == "queued"
    assert record["type"] == "scrape"


def test_enqueue_job_stores_params_json(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.enqueue_job("job-enqueue-params", "scrape", '{"run_mode": "full"}')

    c = get_connection()
    row = c.execute(
        "SELECT job_params_json FROM scraper_jobs WHERE id = %s", ("job-enqueue-params",)
    ).fetchone()
    c.close()

    assert row is not None
    import json

    params = json.loads(row[0])
    assert params["run_mode"] == "full"


def test_enqueue_job_sets_queued_at(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.enqueue_job("job-enqueue-ts", "scrape", "{}")

    c = get_connection()
    row = c.execute(
        "SELECT queued_at FROM scraper_jobs WHERE id = %s", ("job-enqueue-ts",)
    ).fetchone()
    c.close()

    assert row is not None
    assert row[0] is not None


# ---------------------------------------------------------------------------
# pop_next_queued_job
# ---------------------------------------------------------------------------


def test_pop_next_queued_job_claims_oldest_job(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    # Seed two queued jobs with distinct queued_at times
    db_jobs.enqueue_job("job-pop-first", "scrape", '{"run_mode": "delta"}')
    db_jobs.enqueue_job("job-pop-second", "scrape", '{"run_mode": "full"}')

    # Set first job as older
    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET queued_at = %s WHERE id = %s",
        ("2000-01-01T00:00:00Z", "job-pop-first"),
    )
    c.commit()
    c.close()

    result = db_jobs.pop_next_queued_job()
    assert result is not None
    assert result["id"] == "job-pop-first"


def test_pop_next_queued_job_sets_status_running(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    # Clear any leftover queued jobs so ours is the only candidate
    c = get_connection()
    c.execute("UPDATE scraper_jobs SET status = %s WHERE status = %s", ("error", "queued"))
    c.commit()
    c.close()

    db_jobs.enqueue_job("job-pop-running", "scrape", "{}")
    db_jobs.pop_next_queued_job()

    record = db_jobs.get_job("job-pop-running")
    assert record is not None
    assert record["status"] == "running"


def test_pop_next_queued_job_returns_params_json(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    # Clear any leftover queued jobs so ours is the only candidate
    c = get_connection()
    c.execute("UPDATE scraper_jobs SET status = %s WHERE status = %s", ("error", "queued"))
    c.commit()
    c.close()

    db_jobs.enqueue_job("job-pop-params", "scrape", '{"run_mode": "bios_only"}')
    result = db_jobs.pop_next_queued_job()
    assert result is not None
    assert result["job_params_json"] == '{"run_mode": "bios_only"}'


def test_pop_next_queued_job_returns_none_when_empty(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    # Expire all queued jobs first
    c = get_connection()
    c.execute("UPDATE scraper_jobs SET status = %s WHERE status = %s", ("error", "queued"))
    c.commit()
    c.close()

    result = db_jobs.pop_next_queued_job()
    assert result is None


# ---------------------------------------------------------------------------
# count_active_jobs
# ---------------------------------------------------------------------------


def test_count_active_jobs_counts_running_and_queued(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    # Reset: mark all existing active jobs as complete
    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET status = %s WHERE status IN (%s, %s)",
        ("complete", "running", "queued"),
    )
    c.commit()
    c.close()

    db_jobs.create_job("job-active-running", "scrape")  # status=running
    db_jobs.enqueue_job("job-active-queued", "scrape", "{}")  # status=queued

    count = db_jobs.count_active_jobs()
    assert count >= 2


def test_count_active_jobs_excludes_complete(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET status = %s WHERE status IN (%s, %s)",
        ("complete", "running", "queued"),
    )
    c.commit()
    c.close()

    db_jobs.create_job("job-active-complete", "scrape")
    db_jobs.update_job("job-active-complete", "complete")

    count = db_jobs.count_active_jobs()
    assert count == 0


# ---------------------------------------------------------------------------
# count_queued_jobs
# ---------------------------------------------------------------------------


def test_count_queued_jobs_counts_only_queued(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    # Clear active jobs
    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET status = %s WHERE status IN (%s, %s)",
        ("complete", "running", "queued"),
    )
    c.commit()
    c.close()

    db_jobs.enqueue_job("job-queue-count-1", "scrape", "{}")
    db_jobs.enqueue_job("job-queue-count-2", "scrape", "{}")

    count = db_jobs.count_queued_jobs()
    assert count >= 2


def test_count_queued_jobs_excludes_running(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET status = %s WHERE status IN (%s, %s)",
        ("complete", "running", "queued"),
    )
    c.commit()
    c.close()

    db_jobs.create_job("job-queue-running-only", "scrape")  # running, not queued

    count = db_jobs.count_queued_jobs()
    assert count == 0


# ---------------------------------------------------------------------------
# expire_stale_jobs
# ---------------------------------------------------------------------------


def _age_job(db_path, job_id: str, hours_ago: float) -> None:
    """Helper: backdates a job's created_at to simulate age."""
    import os
    from datetime import timedelta

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    from datetime import datetime, timezone

    past = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    past_iso = past.strftime("%Y-%m-%dT%H:%M:%SZ")
    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET created_at = %s, queued_at = %s WHERE id = %s",
        (past_iso, past_iso, job_id),
    )
    c.commit()
    c.close()


def test_expire_stale_jobs_expires_old_running_full_job(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-expire-full", "full")
    _age_job(db_path, "job-expire-full", hours_ago=25)

    expired = db_jobs.expire_stale_jobs()

    ids = [e["id"] for e in expired]
    assert "job-expire-full" in ids
    assert db_jobs.get_job("job-expire-full")["status"] == "error"


def test_expire_stale_jobs_does_not_expire_recent_full_job(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-expire-full-recent", "full")
    _age_job(db_path, "job-expire-full-recent", hours_ago=1)

    expired = db_jobs.expire_stale_jobs()

    ids = [e["id"] for e in expired]
    assert "job-expire-full-recent" not in ids
    assert db_jobs.get_job("job-expire-full-recent")["status"] == "running"


def test_expire_stale_jobs_expires_old_running_nonfull_job(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-expire-delta", "delta")
    _age_job(db_path, "job-expire-delta", hours_ago=9)

    expired = db_jobs.expire_stale_jobs()

    ids = [e["id"] for e in expired]
    assert "job-expire-delta" in ids
    assert db_jobs.get_job("job-expire-delta")["status"] == "error"


def test_expire_stale_jobs_does_not_expire_recent_nonfull_job(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-expire-delta-recent", "delta")
    _age_job(db_path, "job-expire-delta-recent", hours_ago=1)

    expired = db_jobs.expire_stale_jobs()

    ids = [e["id"] for e in expired]
    assert "job-expire-delta-recent" not in ids
    assert db_jobs.get_job("job-expire-delta-recent")["status"] == "running"


def test_expire_stale_jobs_expires_old_queued_job(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.enqueue_job("job-expire-queued", "scrape", "{}")
    _age_job(db_path, "job-expire-queued", hours_ago=13)

    expired = db_jobs.expire_stale_jobs()

    ids = [e["id"] for e in expired]
    assert "job-expire-queued" in ids
    assert db_jobs.get_job("job-expire-queued")["status"] == "error"


def test_expire_stale_jobs_does_not_expire_recent_queued_job(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.enqueue_job("job-expire-queued-recent", "scrape", "{}")
    _age_job(db_path, "job-expire-queued-recent", hours_ago=1)

    expired = db_jobs.expire_stale_jobs()

    ids = [e["id"] for e in expired]
    assert "job-expire-queued-recent" not in ids
    assert db_jobs.get_job("job-expire-queued-recent")["status"] == "queued"


def test_expire_stale_jobs_returns_expired_list(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-expire-list", "delta")
    _age_job(db_path, "job-expire-list", hours_ago=9)

    expired = db_jobs.expire_stale_jobs()

    assert isinstance(expired, list)
    match = next((e for e in expired if e["id"] == "job-expire-list"), None)
    assert match is not None
    assert match["type"] == "delta"
    assert "reason" in match


def test_expire_stale_jobs_calls_cancel_callback(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    db_jobs.create_job("job-expire-cb", "delta")
    _age_job(db_path, "job-expire-cb", hours_ago=9)

    called_with: list[str] = []
    db_jobs.expire_stale_jobs(cancel_callback=called_with.append)

    assert "job-expire-cb" in called_with


def test_expire_stale_jobs_returns_empty_when_nothing_stale(db_path):
    import os

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    # Mark all active jobs as complete so none are stale
    c = get_connection()
    c.execute(
        "UPDATE scraper_jobs SET status = %s WHERE status IN (%s, %s)",
        ("complete", "running", "queued"),
    )
    c.commit()
    c.close()

    expired = db_jobs.expire_stale_jobs()
    assert expired == []
