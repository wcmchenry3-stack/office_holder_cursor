# -*- coding: utf-8 -*-
"""Unit tests for pure helper functions in src/routers/preview.py.

Tests cover:
- _sanitize_debug_filename: safe filename generation
- _config_bool_export: truthy coercion for config values
- _col_1_to_0_export: 1-based → 0-based column index conversion
- _evict_old_jobs: stale job eviction from in-memory store

No HTTP requests or DB access — all tests run locally.
"""

from __future__ import annotations

import time

import pytest

from src.routers.preview import (
    _sanitize_debug_filename,
    _config_bool_export,
    _col_1_to_0_export,
    _evict_old_jobs,
    _preview_job_store,
    _export_job_store,
    _JOB_MAX_AGE_SECONDS,
)

# ---------------------------------------------------------------------------
# _sanitize_debug_filename
# ---------------------------------------------------------------------------


class TestSanitizeDebugFilename:
    def test_simple_name_unchanged(self):
        assert _sanitize_debug_filename("myoffice") == "myoffice"

    def test_spaces_replaced_with_underscore(self):
        assert _sanitize_debug_filename("my office") == "my_office"

    def test_special_chars_replaced(self):
        result = _sanitize_debug_filename("office<>:/\\|?*name")
        assert "<" not in result
        assert ">" not in result
        assert "/" not in result
        assert "?" not in result

    def test_empty_string_returns_office(self):
        assert _sanitize_debug_filename("") == "office"

    def test_none_returns_office(self):
        assert _sanitize_debug_filename(None) == "office"

    def test_truncated_to_max_len(self):
        long_name = "a" * 200
        result = _sanitize_debug_filename(long_name, max_len=80)
        assert len(result) <= 80

    def test_strips_leading_trailing_underscores(self):
        result = _sanitize_debug_filename("  spaces  ")
        assert not result.startswith("_")
        assert not result.endswith("_")

    def test_default_max_len_is_80(self):
        long_name = "x" * 100
        result = _sanitize_debug_filename(long_name)
        assert len(result) <= 80


# ---------------------------------------------------------------------------
# _config_bool_export
# ---------------------------------------------------------------------------


class TestConfigBoolExport:
    def test_true_string(self):
        assert _config_bool_export("true") is True

    def test_one_string(self):
        assert _config_bool_export("1") is True

    def test_yes_string(self):
        assert _config_bool_export("yes") is True

    def test_false_string(self):
        assert _config_bool_export("false") is False

    def test_zero_string(self):
        assert _config_bool_export("0") is False

    def test_none_returns_false(self):
        assert _config_bool_export(None) is False

    def test_empty_string_returns_false(self):
        assert _config_bool_export("") is False

    def test_bool_true(self):
        assert _config_bool_export(True) is True

    def test_bool_false(self):
        assert _config_bool_export(False) is False


# ---------------------------------------------------------------------------
# _col_1_to_0_export
# ---------------------------------------------------------------------------


class TestCol1To0Export:
    def test_one_becomes_zero(self):
        assert _col_1_to_0_export("1") == 0

    def test_three_becomes_two(self):
        assert _col_1_to_0_export("3") == 2

    def test_zero_string_returns_neg_one(self):
        assert _col_1_to_0_export("0") == -1

    def test_none_returns_neg_one(self):
        assert _col_1_to_0_export(None) == -1

    def test_empty_string_returns_neg_one(self):
        assert _col_1_to_0_export("") == -1

    def test_integer_input(self):
        assert _col_1_to_0_export(2) == 1


# ---------------------------------------------------------------------------
# _evict_old_jobs
# ---------------------------------------------------------------------------


class TestEvictOldJobs:
    def setup_method(self):
        """Clear job stores before each test."""
        _preview_job_store.clear()
        _export_job_store.clear()

    def teardown_method(self):
        """Clean up after each test."""
        _preview_job_store.clear()
        _export_job_store.clear()

    def test_running_jobs_not_evicted(self):
        _preview_job_store["job1"] = {
            "status": "running",
            "_created_at": time.monotonic() - _JOB_MAX_AGE_SECONDS - 10,
        }
        _evict_old_jobs()
        assert "job1" in _preview_job_store

    def test_old_finished_jobs_evicted(self):
        _preview_job_store["job2"] = {
            "status": "complete",
            "_created_at": time.monotonic() - _JOB_MAX_AGE_SECONDS - 10,
        }
        _evict_old_jobs()
        assert "job2" not in _preview_job_store

    def test_recent_finished_jobs_kept(self):
        _preview_job_store["job3"] = {
            "status": "complete",
            "_created_at": time.monotonic() - 10,  # 10 seconds old, well within limit
        }
        _evict_old_jobs()
        assert "job3" in _preview_job_store

    def test_evicts_from_export_store_too(self):
        _export_job_store["exp1"] = {
            "status": "error",
            "_created_at": time.monotonic() - _JOB_MAX_AGE_SECONDS - 10,
        }
        _evict_old_jobs()
        assert "exp1" not in _export_job_store

    def test_no_created_at_not_evicted(self):
        # Jobs without _created_at default to 0 which is always old — should be evicted
        _preview_job_store["job4"] = {"status": "complete"}
        _evict_old_jobs()
        assert "job4" not in _preview_job_store
