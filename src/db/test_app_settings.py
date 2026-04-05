# -*- coding: utf-8 -*-
"""Tests for src/db/app_settings.py."""

from __future__ import annotations

import os

import pytest

from src.db.connection import get_connection, init_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("app_settings_db")
    path = tmp / "app_settings_test.db"
    init_db(path=path)
    return path


@pytest.fixture()
def conn(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    c = get_connection()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _all_keys(conn) -> list[str]:
    rows = conn.execute("SELECT key FROM app_settings ORDER BY key").fetchall()
    return [r[0] for r in rows]


def _get_value(conn, key: str) -> str | None:
    row = conn.execute("SELECT value FROM app_settings WHERE key = %s", (key,)).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# seed_app_settings
# ---------------------------------------------------------------------------


class TestSeedAppSettings:
    def test_seeds_all_14_rows(self, conn):
        from src.db.app_settings import APP_SETTINGS_DEFAULTS, seed_app_settings

        # Clear then re-seed
        conn.execute("DELETE FROM app_settings")
        conn.commit()
        seed_app_settings(conn=conn)
        conn.commit()
        keys = _all_keys(conn)
        assert len(keys) == len(APP_SETTINGS_DEFAULTS)
        expected = sorted(r["key"] for r in APP_SETTINGS_DEFAULTS)
        assert keys == expected

    def test_idempotent_does_not_overwrite(self, conn):
        from src.db.app_settings import seed_app_settings

        # Ensure the row exists with a known value first
        conn.execute(
            "INSERT INTO app_settings (key, value, value_type, updated_at)"
            " VALUES ('max_queued_jobs', '999', 'int', '2024-01-01T00:00:00Z')"
            " ON CONFLICT (key) DO UPDATE SET value = '999'"
        )
        conn.commit()

        seed_app_settings(conn=conn)
        conn.commit()
        val = _get_value(conn, "max_queued_jobs")
        assert val == "999", "seed_app_settings must not overwrite existing values"


# ---------------------------------------------------------------------------
# get_setting
# ---------------------------------------------------------------------------


class TestGetSetting:
    def test_returns_int_for_known_key(self, conn):
        from src.db.app_settings import get_setting, set_setting

        set_setting("expiry_hours_queued", "12", conn=conn)
        conn.commit()
        val = get_setting("expiry_hours_queued", default=12)
        assert isinstance(val, int)
        assert val == 12

    def test_returns_default_for_unknown_key(self, conn):
        from src.db.app_settings import get_setting

        val = get_setting("nonexistent_key", default=42)
        assert val == 42

    def test_returns_default_on_db_error(self, monkeypatch):
        from src.db import app_settings as mod

        def _bad_conn():
            raise RuntimeError("DB unavailable")

        monkeypatch.setattr(mod, "get_connection", _bad_conn)
        val = mod.get_setting("expiry_hours_queued", default=99)
        assert val == 99

    def test_float_default_casts_to_float(self, conn):
        from src.db.app_settings import get_setting, set_setting

        set_setting("max_queued_jobs", "3", conn=conn)
        conn.commit()
        val = get_setting("max_queued_jobs", default=1.0)
        assert isinstance(val, float)
        assert val == 3.0

    def test_str_default_returns_str(self, conn):
        from src.db.app_settings import get_setting, set_setting

        set_setting("max_queued_jobs", "5", conn=conn)
        conn.commit()
        val = get_setting("max_queued_jobs", default="1")
        assert isinstance(val, str)
        assert val == "5"


# ---------------------------------------------------------------------------
# set_setting
# ---------------------------------------------------------------------------


class TestSetSetting:
    def test_updates_value(self, conn):
        from src.db.app_settings import get_setting, set_setting

        set_setting("expiry_hours_queued", "48", conn=conn)
        conn.commit()
        val = get_setting("expiry_hours_queued", default=12)
        assert val == 48

    def test_updates_updated_at(self, conn):
        from src.db.app_settings import set_setting

        set_setting("expiry_hours_queued", "20", conn=conn)
        conn.commit()
        after = conn.execute(
            "SELECT updated_at FROM app_settings WHERE key = 'expiry_hours_queued'"
        ).fetchone()[0]
        assert after is not None
        assert "T" in after


# ---------------------------------------------------------------------------
# list_all_settings
# ---------------------------------------------------------------------------


class TestListAllSettings:
    def test_returns_all_rows(self, conn):
        from src.db.app_settings import APP_SETTINGS_DEFAULTS, list_all_settings

        rows = list_all_settings(conn=conn)
        assert len(rows) == len(APP_SETTINGS_DEFAULTS)

    def test_row_has_expected_keys(self, conn):
        from src.db.app_settings import list_all_settings

        rows = list_all_settings(conn=conn)
        for row in rows:
            assert set(row.keys()) == {"key", "value", "value_type", "description", "updated_at"}

    def test_ordered_by_key(self, conn):
        from src.db.app_settings import list_all_settings

        rows = list_all_settings(conn=conn)
        keys = [r["key"] for r in rows]
        assert keys == sorted(keys)
