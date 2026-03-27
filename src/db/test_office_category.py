"""Tests for src/db/office_category.py CRUD functions.

Uses the shared conftest `tmp_db` fixture (function-scoped SQLite connection).
Each test gets an isolated DB.

Run: pytest src/db/test_office_category.py -v
"""

from __future__ import annotations

import pytest

from src.db import office_category as db_cat
from src.db import offices as db_offices

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
# list_office_categories
# ---------------------------------------------------------------------------


def test_list_categories_empty(tmp_db):
    result = db_cat.list_office_categories(conn=tmp_db)
    assert isinstance(result, list)
    assert result == []


def test_list_categories_returns_created(tmp_db):
    db_cat.create_office_category("MyCategory", [], [], [], conn=tmp_db)
    result = db_cat.list_office_categories(conn=tmp_db)
    assert any(r["name"] == "MyCategory" for r in result)


def test_list_categories_sorted_by_name(tmp_db):
    db_cat.create_office_category("ZCat", [], [], [], conn=tmp_db)
    db_cat.create_office_category("ACat", [], [], [], conn=tmp_db)
    names = [r["name"] for r in db_cat.list_office_categories(conn=tmp_db)]
    assert names == sorted(names)


# ---------------------------------------------------------------------------
# get_office_category
# ---------------------------------------------------------------------------


def test_get_category_returns_none_for_unknown(tmp_db):
    assert db_cat.get_office_category(999999, conn=tmp_db) is None


def test_get_category_basic_fields(tmp_db):
    cid = db_cat.create_office_category("GetCat", [], [], [], conn=tmp_db)
    result = db_cat.get_office_category(cid, conn=tmp_db)
    assert result is not None
    assert result["name"] == "GetCat"
    assert result["country_ids"] == []
    assert result["level_ids"] == []
    assert result["branch_ids"] == []


def test_get_category_with_all_scopes(tmp_db):
    co = _country_id(tmp_db)
    lv = _level_id(tmp_db)
    br = _branch_id(tmp_db)
    cid = db_cat.create_office_category("ScopedCat", [co], [lv], [br], conn=tmp_db)
    result = db_cat.get_office_category(cid, conn=tmp_db)
    assert co in result["country_ids"]
    assert lv in result["level_ids"]
    assert br in result["branch_ids"]


# ---------------------------------------------------------------------------
# create_office_category — validation
# ---------------------------------------------------------------------------


def test_create_empty_name_raises(tmp_db):
    with pytest.raises(ValueError, match="name"):
        db_cat.create_office_category("", [], [], [], conn=tmp_db)


def test_create_whitespace_name_raises(tmp_db):
    with pytest.raises(ValueError, match="name"):
        db_cat.create_office_category("   ", [], [], [], conn=tmp_db)


def test_create_duplicate_name_raises(tmp_db):
    db_cat.create_office_category("DupCat", [], [], [], conn=tmp_db)
    with pytest.raises(ValueError, match="already exists"):
        db_cat.create_office_category("DupCat", [], [], [], conn=tmp_db)


def test_create_returns_integer_id(tmp_db):
    cid = db_cat.create_office_category("IdCat", [], [], [], conn=tmp_db)
    assert isinstance(cid, int)
    assert cid > 0


# ---------------------------------------------------------------------------
# update_office_category
# ---------------------------------------------------------------------------


def test_update_returns_true_on_success(tmp_db):
    cid = db_cat.create_office_category("ToUpdate", [], [], [], conn=tmp_db)
    result = db_cat.update_office_category(cid, "Updated", [], [], [], conn=tmp_db)
    assert result is True


def test_update_persists_new_name(tmp_db):
    cid = db_cat.create_office_category("OldCatName", [], [], [], conn=tmp_db)
    db_cat.update_office_category(cid, "NewCatName", [], [], [], conn=tmp_db)
    fetched = db_cat.get_office_category(cid, conn=tmp_db)
    assert fetched["name"] == "NewCatName"


def test_update_unknown_id_returns_false(tmp_db):
    result = db_cat.update_office_category(999999, "X", [], [], [], conn=tmp_db)
    assert result is False


def test_update_empty_name_raises(tmp_db):
    cid = db_cat.create_office_category("ValidCat", [], [], [], conn=tmp_db)
    with pytest.raises(ValueError, match="name"):
        db_cat.update_office_category(cid, "", [], [], [], conn=tmp_db)


def test_update_replaces_scope_rows(tmp_db):
    co = _country_id(tmp_db)
    cid = db_cat.create_office_category("ScopeReplaceCat", [co], [], [], conn=tmp_db)
    db_cat.update_office_category(cid, "ScopeReplaceCat", [], [], [], conn=tmp_db)
    fetched = db_cat.get_office_category(cid, conn=tmp_db)
    assert fetched["country_ids"] == []


def test_update_adds_level_scope(tmp_db):
    lv = _level_id(tmp_db)
    cid = db_cat.create_office_category("AddLevelCat", [], [], [], conn=tmp_db)
    db_cat.update_office_category(cid, "AddLevelCat", [], [lv], [], conn=tmp_db)
    fetched = db_cat.get_office_category(cid, conn=tmp_db)
    assert lv in fetched["level_ids"]


# ---------------------------------------------------------------------------
# delete_office_category
# ---------------------------------------------------------------------------


def test_delete_removes_category(tmp_db):
    cid = db_cat.create_office_category("ToDeleteCat", [], [], [], conn=tmp_db)
    db_cat.delete_office_category(cid, conn=tmp_db)
    assert db_cat.get_office_category(cid, conn=tmp_db) is None


def test_delete_removes_junction_rows(tmp_db):
    co = _country_id(tmp_db)
    cid = db_cat.create_office_category("ScopedDeleteCat", [co], [], [], conn=tmp_db)
    db_cat.delete_office_category(cid, conn=tmp_db)
    rows = tmp_db.execute(
        "SELECT COUNT(*) FROM office_category_countries WHERE category_id = ?", (cid,)
    ).fetchone()[0]
    assert rows == 0


def test_delete_in_use_raises(tmp_db):
    """Cannot delete a category that is referenced by an office."""
    cid = db_cat.create_office_category("InUseCat", [], [], [], conn=tmp_db)
    # Create an office that references this category
    db_offices.create_office(
        {
            "country_id": _country_id(tmp_db),
            "name": "Ref Office",
            "url": "https://en.wikipedia.org/wiki/Ref_Office_Cat",
            "enabled": True,
            "office_category_id": cid,
        },
        conn=tmp_db,
    )
    tmp_db.commit()
    with pytest.raises(ValueError, match="in use"):
        db_cat.delete_office_category(cid, conn=tmp_db)


# ---------------------------------------------------------------------------
# list_categories_for_office
# ---------------------------------------------------------------------------


def test_list_for_office_no_scope_matches_any_context(tmp_db):
    db_cat.create_office_category("OpenCat", [], [], [], conn=tmp_db)
    result = db_cat.list_categories_for_office(None, None, None, conn=tmp_db)
    names = [r["name"] for r in result]
    assert "OpenCat" in names


def test_list_for_office_country_scoped_matches_correct_country(tmp_db):
    co = _country_id(tmp_db)
    db_cat.create_office_category("Ctrycat", [co], [], [], conn=tmp_db)
    result = db_cat.list_categories_for_office(co, None, None, conn=tmp_db)
    names = [r["name"] for r in result]
    assert "Ctrycat" in names


def test_list_for_office_country_scoped_excluded_when_null(tmp_db):
    co = _country_id(tmp_db)
    db_cat.create_office_category("ExcludedCat", [co], [], [], conn=tmp_db)
    result = db_cat.list_categories_for_office(None, None, None, conn=tmp_db)
    names = [r["name"] for r in result]
    assert "ExcludedCat" not in names
