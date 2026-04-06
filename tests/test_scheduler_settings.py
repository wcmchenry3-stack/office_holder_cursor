# -*- coding: utf-8 -*-
"""Unit tests for src/db/scheduler_settings.py."""

from __future__ import annotations

import pytest

from src.db.connection import init_db
from src.db.scheduler_settings import (
    PAUSEABLE_JOB_IDS,
    is_job_paused,
    list_all_settings,
    seed_scheduler_settings,
    set_job_paused,
)


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "sched_test.db"
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))
    init_db(path=db_path)
    return db_path


# ---------------------------------------------------------------------------
# seed_scheduler_settings
# ---------------------------------------------------------------------------


def test_seed_creates_all_pauseable_jobs(tmp_db, monkeypatch):
    settings = list_all_settings()
    job_ids = {s["job_id"] for s in settings}
    for jid in PAUSEABLE_JOB_IDS:
        assert jid in job_ids


def test_seed_is_idempotent(tmp_db, monkeypatch):
    # init_db already called seed; calling again must not raise or duplicate
    seed_scheduler_settings()
    settings = list_all_settings()
    job_ids = [s["job_id"] for s in settings]
    # No duplicates
    assert len(job_ids) == len(set(job_ids))


def test_seed_default_paused_is_false(tmp_db, monkeypatch):
    for s in list_all_settings():
        assert s["paused"] is False


# ---------------------------------------------------------------------------
# is_job_paused
# ---------------------------------------------------------------------------


def test_is_job_paused_returns_false_for_unknown_id(tmp_db, monkeypatch):
    assert is_job_paused("nonexistent_job_xyz") is False


def test_is_job_paused_returns_false_by_default(tmp_db, monkeypatch):
    assert is_job_paused(PAUSEABLE_JOB_IDS[0]) is False


# ---------------------------------------------------------------------------
# set_job_paused / read-back
# ---------------------------------------------------------------------------


def test_set_job_paused_true_and_readback(tmp_db, monkeypatch):
    job_id = PAUSEABLE_JOB_IDS[0]
    set_job_paused(job_id, True)
    assert is_job_paused(job_id) is True


def test_set_job_paused_false_after_true(tmp_db, monkeypatch):
    job_id = PAUSEABLE_JOB_IDS[1]
    set_job_paused(job_id, True)
    assert is_job_paused(job_id) is True
    set_job_paused(job_id, False)
    assert is_job_paused(job_id) is False


# ---------------------------------------------------------------------------
# list_all_settings
# ---------------------------------------------------------------------------


def test_list_all_settings_returns_list_of_dicts(tmp_db, monkeypatch):
    settings = list_all_settings()
    assert isinstance(settings, list)
    assert len(settings) >= len(PAUSEABLE_JOB_IDS)
    for s in settings:
        assert "job_id" in s
        assert "paused" in s
        assert "updated_at" in s


def test_list_all_settings_ordered_by_job_id(tmp_db, monkeypatch):
    settings = list_all_settings()
    job_ids = [s["job_id"] for s in settings]
    assert job_ids == sorted(job_ids)


def test_list_all_settings_paused_is_bool(tmp_db, monkeypatch):
    for s in list_all_settings():
        assert isinstance(s["paused"], bool)
