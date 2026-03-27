"""Tests for src/db/infobox_role_key_filter.py CRUD functions.

Uses the shared conftest `tmp_db` fixture (function-scoped SQLite connection
to a fully initialized DB). Each test gets its own isolated DB so there is
no cross-test state.

Run: pytest src/db/test_infobox_role_key_filter.py -v
"""

from __future__ import annotations

import pytest

from src.db import infobox_role_key_filter as db_filter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _country_id(conn) -> int:
    return conn.execute("SELECT id FROM countries ORDER BY id LIMIT 1").fetchone()[0]


def _level_id(conn) -> int:
    return conn.execute("SELECT id FROM levels ORDER BY id LIMIT 1").fetchone()[0]


def _branch_id(conn) -> int:
    return conn.execute("SELECT id FROM branches ORDER BY id LIMIT 1").fetchone()[0]


# ---------------------------------------------------------------------------
# list_infobox_role_key_filters
# ---------------------------------------------------------------------------


def test_list_filters_empty_db(tmp_db):
    result = db_filter.list_infobox_role_key_filters(conn=tmp_db)
    assert isinstance(result, list)
    assert result == []


def test_list_filters_returns_created(tmp_db):
    db_filter.create_infobox_role_key_filter("ListFilter", "president", [], [], [], conn=tmp_db)
    result = db_filter.list_infobox_role_key_filters(conn=tmp_db)
    assert any(r["name"] == "ListFilter" for r in result)


def test_list_filters_sorted_by_name(tmp_db):
    db_filter.create_infobox_role_key_filter("ZFilter", "z_role", [], [], [], conn=tmp_db)
    db_filter.create_infobox_role_key_filter("AFilter", "a_role", [], [], [], conn=tmp_db)
    names = [r["name"] for r in db_filter.list_infobox_role_key_filters(conn=tmp_db)]
    assert names == sorted(names)


# ---------------------------------------------------------------------------
# get_infobox_role_key_filter
# ---------------------------------------------------------------------------


def test_get_filter_returns_none_for_unknown(tmp_db):
    assert db_filter.get_infobox_role_key_filter(999999, conn=tmp_db) is None


def test_get_filter_basic_fields(tmp_db):
    fid = db_filter.create_infobox_role_key_filter("GetFilter", "senator", [], [], [], conn=tmp_db)
    result = db_filter.get_infobox_role_key_filter(fid, conn=tmp_db)
    assert result is not None
    assert result["name"] == "GetFilter"
    assert result["role_key"] == "senator"
    assert result["country_ids"] == []
    assert result["level_ids"] == []
    assert result["branch_ids"] == []


def test_get_filter_with_all_scopes(tmp_db):
    cid = _country_id(tmp_db)
    lid = _level_id(tmp_db)
    bid = _branch_id(tmp_db)
    fid = db_filter.create_infobox_role_key_filter(
        "ScopedFilter", "governor", [cid], [lid], [bid], conn=tmp_db
    )
    result = db_filter.get_infobox_role_key_filter(fid, conn=tmp_db)
    assert cid in result["country_ids"]
    assert lid in result["level_ids"]
    assert bid in result["branch_ids"]


# ---------------------------------------------------------------------------
# create_infobox_role_key_filter — validation
# ---------------------------------------------------------------------------


def test_create_empty_name_raises(tmp_db):
    with pytest.raises(ValueError, match="name"):
        db_filter.create_infobox_role_key_filter("", "president", [], [], [], conn=tmp_db)


def test_create_whitespace_only_name_raises(tmp_db):
    with pytest.raises(ValueError, match="name"):
        db_filter.create_infobox_role_key_filter("   ", "president", [], [], [], conn=tmp_db)


def test_create_empty_role_key_raises(tmp_db):
    with pytest.raises(ValueError, match="[Rr]ole"):
        db_filter.create_infobox_role_key_filter("ValidName", "", [], [], [], conn=tmp_db)


def test_create_duplicate_name_raises(tmp_db):
    db_filter.create_infobox_role_key_filter("DupFilter", "role_a", [], [], [], conn=tmp_db)
    with pytest.raises(ValueError, match="already exists"):
        db_filter.create_infobox_role_key_filter("DupFilter", "role_b", [], [], [], conn=tmp_db)


def test_create_returns_integer_id(tmp_db):
    fid = db_filter.create_infobox_role_key_filter("IdFilter", "my_role", [], [], [], conn=tmp_db)
    assert isinstance(fid, int)
    assert fid > 0


# ---------------------------------------------------------------------------
# update_infobox_role_key_filter
# ---------------------------------------------------------------------------


def test_update_returns_true_on_success(tmp_db):
    fid = db_filter.create_infobox_role_key_filter("ToUpdate", "old_role", [], [], [], conn=tmp_db)
    result = db_filter.update_infobox_role_key_filter(
        fid, "Updated", "new_role", [], [], [], conn=tmp_db
    )
    assert result is True


def test_update_persists_new_values(tmp_db):
    fid = db_filter.create_infobox_role_key_filter("OldName", "old_role", [], [], [], conn=tmp_db)
    db_filter.update_infobox_role_key_filter(fid, "NewName", "new_role", [], [], [], conn=tmp_db)
    fetched = db_filter.get_infobox_role_key_filter(fid, conn=tmp_db)
    assert fetched["name"] == "NewName"
    assert fetched["role_key"] == "new_role"


def test_update_unknown_id_returns_false(tmp_db):
    result = db_filter.update_infobox_role_key_filter(999999, "X", "y", [], [], [], conn=tmp_db)
    assert result is False


def test_update_empty_name_raises(tmp_db):
    fid = db_filter.create_infobox_role_key_filter("UpdateVal", "role", [], [], [], conn=tmp_db)
    with pytest.raises(ValueError, match="name"):
        db_filter.update_infobox_role_key_filter(fid, "", "role", [], [], [], conn=tmp_db)


def test_update_empty_role_key_raises(tmp_db):
    fid = db_filter.create_infobox_role_key_filter("UpdateRK", "role", [], [], [], conn=tmp_db)
    with pytest.raises(ValueError, match="[Rr]ole"):
        db_filter.update_infobox_role_key_filter(fid, "UpdateRK", "", [], [], [], conn=tmp_db)


def test_update_replaces_scope_rows(tmp_db):
    cid = _country_id(tmp_db)
    fid = db_filter.create_infobox_role_key_filter("ScopeReplace", "r", [cid], [], [], conn=tmp_db)
    # Update with empty scopes — country scope should be cleared
    db_filter.update_infobox_role_key_filter(fid, "ScopeReplace", "r", [], [], [], conn=tmp_db)
    fetched = db_filter.get_infobox_role_key_filter(fid, conn=tmp_db)
    assert fetched["country_ids"] == []


def test_update_adds_new_scopes(tmp_db):
    lid = _level_id(tmp_db)
    fid = db_filter.create_infobox_role_key_filter("AddScopes", "r", [], [], [], conn=tmp_db)
    db_filter.update_infobox_role_key_filter(fid, "AddScopes", "r", [], [lid], [], conn=tmp_db)
    fetched = db_filter.get_infobox_role_key_filter(fid, conn=tmp_db)
    assert lid in fetched["level_ids"]


# ---------------------------------------------------------------------------
# delete_infobox_role_key_filter
# ---------------------------------------------------------------------------


def test_delete_removes_filter(tmp_db):
    fid = db_filter.create_infobox_role_key_filter("ToDelete", "del_role", [], [], [], conn=tmp_db)
    db_filter.delete_infobox_role_key_filter(fid, conn=tmp_db)
    assert db_filter.get_infobox_role_key_filter(fid, conn=tmp_db) is None


def test_delete_removes_scope_rows(tmp_db):
    cid = _country_id(tmp_db)
    fid = db_filter.create_infobox_role_key_filter("ScopedDelete", "r", [cid], [], [], conn=tmp_db)
    db_filter.delete_infobox_role_key_filter(fid, conn=tmp_db)
    rows = tmp_db.execute(
        "SELECT COUNT(*) FROM infobox_role_key_filter_countries WHERE filter_id = ?", (fid,)
    ).fetchone()[0]
    assert rows == 0


# ---------------------------------------------------------------------------
# list_filters_for_context
# ---------------------------------------------------------------------------


def test_list_for_context_no_scope_matches_null_context(tmp_db):
    """Filter with no scope rows should match any context (including all-None)."""
    db_filter.create_infobox_role_key_filter("OpenFilter", "open_role", [], [], [], conn=tmp_db)
    result = db_filter.list_filters_for_context(None, None, None, conn=tmp_db)
    names = [r["name"] for r in result]
    assert "OpenFilter" in names


def test_list_for_context_country_scoped_matches_correct_country(tmp_db):
    cid = _country_id(tmp_db)
    db_filter.create_infobox_role_key_filter("CtryScoped", "c_role", [cid], [], [], conn=tmp_db)
    result = db_filter.list_filters_for_context(cid, None, None, conn=tmp_db)
    names = [r["name"] for r in result]
    assert "CtryScoped" in names


def test_list_for_context_country_scoped_excluded_when_country_null(tmp_db):
    cid = _country_id(tmp_db)
    db_filter.create_infobox_role_key_filter("ExcludedCtry", "ex_role", [cid], [], [], conn=tmp_db)
    result = db_filter.list_filters_for_context(None, None, None, conn=tmp_db)
    names = [r["name"] for r in result]
    assert "ExcludedCtry" not in names
