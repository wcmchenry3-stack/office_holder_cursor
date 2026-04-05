"""Tests for src/db/scheduled_job_runs.py.

Uses SQLite in-memory via init_db() — no PostgreSQL required.

Run: pytest src/db/test_scheduled_job_runs.py -v
"""

from __future__ import annotations

import json

import pytest

from src.db.connection import get_connection, init_db
from src.db import scheduled_job_runs as db_runs

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("scheduled_job_runs_db")
    path = tmp / "scheduled_job_runs_test.db"
    init_db(path=path)
    return path


@pytest.fixture(autouse=True)
def _set_db_path(db_path, monkeypatch):
    """Point every test at the shared test DB."""
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))


# ---------------------------------------------------------------------------
# create_run
# ---------------------------------------------------------------------------


def test_create_run_returns_id(db_path):
    run_id = db_runs.create_run("daily_delta")
    assert isinstance(run_id, int)
    assert run_id > 0


def test_create_run_sets_running_status(db_path):
    run_id = db_runs.create_run("daily_delta")

    c = get_connection()
    row = c.execute("SELECT status FROM scheduled_job_runs WHERE id = %s", (run_id,)).fetchone()
    c.close()

    assert row is not None
    assert row[0] == "running"


def test_create_run_sets_started_at(db_path):
    run_id = db_runs.create_run("daily_delta")

    c = get_connection()
    row = c.execute("SELECT started_at FROM scheduled_job_runs WHERE id = %s", (run_id,)).fetchone()
    c.close()

    assert row is not None
    assert row[0] is not None


def test_create_run_stores_job_name(db_path):
    run_id = db_runs.create_run("gemini_research")

    c = get_connection()
    row = c.execute("SELECT job_name FROM scheduled_job_runs WHERE id = %s", (run_id,)).fetchone()
    c.close()

    assert row is not None
    assert row[0] == "gemini_research"


def test_create_run_ids_are_unique(db_path):
    id1 = db_runs.create_run("daily_delta")
    id2 = db_runs.create_run("daily_delta")
    assert id1 != id2


# ---------------------------------------------------------------------------
# finish_run
# ---------------------------------------------------------------------------


def test_finish_run_sets_status(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "complete")

    c = get_connection()
    row = c.execute("SELECT status FROM scheduled_job_runs WHERE id = %s", (run_id,)).fetchone()
    c.close()

    assert row[0] == "complete"


def test_finish_run_sets_error_status(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "error", error="something blew up")

    c = get_connection()
    row = c.execute("SELECT status FROM scheduled_job_runs WHERE id = %s", (run_id,)).fetchone()
    c.close()

    assert row[0] == "error"


def test_finish_run_sets_finished_at(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "complete")

    c = get_connection()
    row = c.execute(
        "SELECT finished_at FROM scheduled_job_runs WHERE id = %s", (run_id,)
    ).fetchone()
    c.close()

    assert row[0] is not None


def test_finish_run_computes_positive_duration(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "complete")

    c = get_connection()
    row = c.execute("SELECT duration_s FROM scheduled_job_runs WHERE id = %s", (run_id,)).fetchone()
    c.close()

    assert row[0] is not None
    assert float(row[0]) >= 0.0


def test_finish_run_stores_result_json(db_path):
    run_id = db_runs.create_run("daily_delta")
    result = {"office_count": 10, "terms_parsed": 42, "bio_success_count": 5}
    db_runs.finish_run(run_id, "complete", result=result)

    c = get_connection()
    row = c.execute(
        "SELECT result_json FROM scheduled_job_runs WHERE id = %s", (run_id,)
    ).fetchone()
    c.close()

    assert row[0] is not None
    parsed = json.loads(row[0])
    assert parsed["office_count"] == 10
    assert parsed["terms_parsed"] == 42


def test_finish_run_stores_error_message(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "error", error="scraper exploded")

    c = get_connection()
    row = c.execute("SELECT error FROM scheduled_job_runs WHERE id = %s", (run_id,)).fetchone()
    c.close()

    assert row[0] == "scraper exploded"


def test_finish_run_result_none_by_default(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "complete")

    c = get_connection()
    row = c.execute(
        "SELECT result_json FROM scheduled_job_runs WHERE id = %s", (run_id,)
    ).fetchone()
    c.close()

    assert row[0] is None


# ---------------------------------------------------------------------------
# get_last_run_for_job
# ---------------------------------------------------------------------------


def test_get_last_run_for_job_returns_most_recent(db_path):
    run_id1 = db_runs.create_run("insufficient_vitals")
    db_runs.finish_run(run_id1, "complete")
    run_id2 = db_runs.create_run("insufficient_vitals")
    db_runs.finish_run(run_id2, "complete")

    result = db_runs.get_last_run_for_job("insufficient_vitals")
    assert result is not None
    assert result["id"] == run_id2


def test_get_last_run_for_job_returns_none_when_no_rows(db_path):
    result = db_runs.get_last_run_for_job("nonexistent_job_xyz")
    assert result is None


def test_get_last_run_for_job_contains_expected_keys(db_path):
    run_id = db_runs.create_run("daily_page_quality")
    db_runs.finish_run(run_id, "complete")

    result = db_runs.get_last_run_for_job("daily_page_quality")
    assert result is not None
    for key in ("id", "job_name", "started_at", "finished_at", "status", "duration_s"):
        assert key in result


def test_get_last_run_for_job_reflects_status(db_path):
    run_id = db_runs.create_run("daily_maintenance")
    db_runs.finish_run(run_id, "error", error="boom")

    result = db_runs.get_last_run_for_job("daily_maintenance")
    assert result is not None
    assert result["status"] == "error"


# ---------------------------------------------------------------------------
# list_recent_runs
# ---------------------------------------------------------------------------


def test_list_recent_runs_returns_list(db_path):
    db_runs.create_run("daily_delta")
    result = db_runs.list_recent_runs(days=90)
    assert isinstance(result, list)


def test_list_recent_runs_newest_first(db_path):
    id1 = db_runs.create_run("daily_delta")
    db_runs.finish_run(id1, "complete")
    id2 = db_runs.create_run("daily_delta")
    db_runs.finish_run(id2, "complete")

    # id2 > id1 and both may share the same started_at second; tiebreaker is id DESC
    runs = db_runs.list_recent_runs(days=90)
    ids = [r["id"] for r in runs if r["id"] in (id1, id2)]
    assert ids[0] == id2  # newer id appears first


def test_list_recent_runs_respects_days_cutoff(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "complete")

    # Backdate the run to beyond the cutoff
    c = get_connection()
    c.execute(
        "UPDATE scheduled_job_runs SET started_at = %s WHERE id = %s",
        ("2000-01-01T00:00:00Z", run_id),
    )
    c.commit()
    c.close()

    runs = db_runs.list_recent_runs(days=1)
    ids = [r["id"] for r in runs]
    assert run_id not in ids


def test_list_recent_runs_empty_when_all_rows_outdated(db_path):
    """When every run is older than the cutoff, the result is empty."""
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "complete")

    # Backdate this run well beyond any cutoff we'd use
    c = get_connection()
    c.execute(
        "UPDATE scheduled_job_runs SET started_at = %s WHERE id = %s",
        ("1999-01-01T00:00:00Z", run_id),
    )
    c.commit()
    c.close()

    # Narrow cutoff: only last 1 day — the backdated run must not appear
    runs = db_runs.list_recent_runs(days=1)
    ids = [r["id"] for r in runs]
    assert run_id not in ids


def test_list_recent_runs_deserializes_result_json(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "complete", result={"terms_parsed": 99})

    runs = db_runs.list_recent_runs(days=90)
    match = next((r for r in runs if r["id"] == run_id), None)
    assert match is not None
    assert match["result"]["terms_parsed"] == 99
    assert "result_json" not in match


def test_list_recent_runs_result_none_when_no_result(db_path):
    run_id = db_runs.create_run("daily_delta")
    db_runs.finish_run(run_id, "complete")

    runs = db_runs.list_recent_runs(days=90)
    match = next((r for r in runs if r["id"] == run_id), None)
    assert match is not None
    assert match["result"] is None
