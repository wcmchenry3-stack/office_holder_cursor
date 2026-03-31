"""Unit tests for src/db/individuals.py.

All tests use the `tmp_db` fixture (fully initialised, seeded SQLite DB).
No network access, no HTTP.

Run: pytest src/db/test_individuals.py -v
"""

from __future__ import annotations

from datetime import date

import pytest

from src.db import individuals as db_individuals
from src.db import offices as db_offices
from src.db import refs as db_refs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_office(conn) -> int:
    """Create a minimal office and return its office_details_id."""
    return db_offices.create_office(
        {
            "country_id": 1,  # United States of America (seeded)
            "state_id": None,
            "city_id": None,
            "level_id": None,
            "branch_id": None,
            "department": "",
            "name": "Test Office",
            "enabled": True,
            "notes": "",
            "url": "https://en.wikipedia.org/wiki/Test_Office_Ind",
            "table_configs": [
                {
                    "name": "",
                    "table_no": 1,
                    "table_rows": 1,
                    "link_column": 1,
                    "party_column": 0,
                    "term_start_column": 2,
                    "term_end_column": 3,
                    "district_column": 0,
                    "enabled": 1,
                }
            ],
        },
        conn=conn,
    )


def _insert_term(conn, individual_id: int, office_details_id: int, term_start_year: int) -> None:
    """Insert a minimal office_term with a given term_start_year."""
    conn.execute(
        """INSERT INTO office_terms
           (office_id, individual_id, wiki_url, term_start_year)
           VALUES (%s, %s, %s, %s)""",
        (
            office_details_id,
            individual_id,
            f"https://en.wikipedia.org/wiki/Person_{individual_id}",
            term_start_year,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# upsert_individual — insert
# ---------------------------------------------------------------------------


def test_upsert_individual_insert_returns_id(tmp_db):
    ind_id = db_individuals.upsert_individual(
        {"wiki_url": "/wiki/Jane_Smith", "full_name": "Jane Smith"},
        conn=tmp_db,
    )
    assert isinstance(ind_id, int)
    assert ind_id > 0


def test_upsert_individual_insert_stores_correct_fields(tmp_db):
    db_individuals.upsert_individual(
        {
            "wiki_url": "/wiki/John_Doe",
            "full_name": "John Doe",
            "birth_date": "1950-01-01",
            "page_path": "John_Doe",
        },
        conn=tmp_db,
    )
    row = db_individuals.get_individual_by_wiki_url("/wiki/John_Doe", conn=tmp_db)
    assert row is not None
    assert row["full_name"] == "John Doe"
    assert row["birth_date"] == "1950-01-01"
    assert row["page_path"] == "John_Doe"


def test_upsert_individual_update_keeps_same_id(tmp_db):
    ind_id = db_individuals.upsert_individual(
        {"wiki_url": "/wiki/UpdateMe", "full_name": "Original Name"},
        conn=tmp_db,
    )
    updated_id = db_individuals.upsert_individual(
        {"wiki_url": "/wiki/UpdateMe", "full_name": "Updated Name"},
        conn=tmp_db,
    )
    assert updated_id == ind_id


def test_upsert_individual_update_changes_fields(tmp_db):
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/FieldUpdate", "full_name": "Before"},
        conn=tmp_db,
    )
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/FieldUpdate", "full_name": "After", "birth_place": "Springfield"},
        conn=tmp_db,
    )
    row = db_individuals.get_individual_by_wiki_url("/wiki/FieldUpdate", conn=tmp_db)
    assert row["full_name"] == "After"
    assert row["birth_place"] == "Springfield"


# ---------------------------------------------------------------------------
# bio_batch
# ---------------------------------------------------------------------------


def test_upsert_individual_bio_batch_is_id_mod_7(tmp_db):
    ind_id = db_individuals.upsert_individual(
        {"wiki_url": "/wiki/BatchTest"},
        conn=tmp_db,
    )
    row = db_individuals.get_individual(ind_id, conn=tmp_db)
    assert row["bio_batch"] == ind_id % 7


# ---------------------------------------------------------------------------
# is_living inference
# ---------------------------------------------------------------------------


def test_is_living_stays_1_with_no_death_date_and_recent_terms(tmp_db):
    od_id = _make_office(tmp_db)
    ind_id = db_individuals.upsert_individual(
        {"wiki_url": "/wiki/LivingPerson"},
        conn=tmp_db,
    )
    _insert_term(tmp_db, ind_id, od_id, date.today().year - 5)
    row = db_individuals.get_individual(ind_id, conn=tmp_db)
    assert row["is_living"] == 1


def test_is_living_flips_to_0_when_death_date_set(tmp_db):
    ind_id = db_individuals.upsert_individual(
        {"wiki_url": "/wiki/DeadPerson", "death_date": "2020-01-01"},
        conn=tmp_db,
    )
    row = db_individuals.get_individual(ind_id, conn=tmp_db)
    assert row["is_living"] == 0


def test_is_living_flips_to_0_when_earliest_term_over_80_years_ago(tmp_db):
    od_id = _make_office(tmp_db)
    ind_id = db_individuals.upsert_individual(
        {"wiki_url": "/wiki/OldPerson"},
        conn=tmp_db,
    )
    _insert_term(tmp_db, ind_id, od_id, date.today().year - 90)
    # Re-upsert to trigger recompute
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/OldPerson"},
        conn=tmp_db,
    )
    row = db_individuals.get_individual(ind_id, conn=tmp_db)
    assert row["is_living"] == 0


def test_is_living_0_not_flipped_back_to_1_on_re_upsert(tmp_db):
    """Once is_living = 0, a subsequent upsert without death_date must not restore it."""
    ind_id = db_individuals.upsert_individual(
        {"wiki_url": "/wiki/OneWayGate", "death_date": "2010-06-15"},
        conn=tmp_db,
    )
    # Re-upsert without death_date
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/OneWayGate"},
        conn=tmp_db,
    )
    row = db_individuals.get_individual(ind_id, conn=tmp_db)
    assert row["is_living"] == 0


# ---------------------------------------------------------------------------
# get_living_individual_wiki_urls
# ---------------------------------------------------------------------------


def test_get_living_excludes_dead_link_individuals(tmp_db):
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/DeadLink", "is_dead_link": True},
        conn=tmp_db,
    )
    urls = db_individuals.get_living_individual_wiki_urls(conn=tmp_db)
    assert "/wiki/DeadLink" not in urls


def test_get_living_excludes_no_link_prefix(tmp_db):
    db_individuals.upsert_individual(
        {"wiki_url": "No link: Jane Doe"},
        conn=tmp_db,
    )
    urls = db_individuals.get_living_individual_wiki_urls(conn=tmp_db)
    assert "No link: Jane Doe" not in urls


def test_get_living_includes_normal_living_individual(tmp_db):
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/AliveAndWell", "full_name": "Alive Person"},
        conn=tmp_db,
    )
    urls = db_individuals.get_living_individual_wiki_urls(conn=tmp_db)
    assert "/wiki/AliveAndWell" in urls


# ---------------------------------------------------------------------------
# mark_bio_refreshed
# ---------------------------------------------------------------------------


def test_mark_bio_refreshed_stamps_bio_refreshed_at(tmp_db):
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/RefreshMe"},
        conn=tmp_db,
    )
    row_before = db_individuals.get_individual_by_wiki_url("/wiki/RefreshMe", conn=tmp_db)
    assert row_before["bio_refreshed_at"] is None

    db_individuals.mark_bio_refreshed("/wiki/RefreshMe", conn=tmp_db)

    row_after = db_individuals.get_individual_by_wiki_url("/wiki/RefreshMe", conn=tmp_db)
    assert row_after["bio_refreshed_at"] is not None


# ---------------------------------------------------------------------------
# Duplicate / race-condition protection
# ---------------------------------------------------------------------------


def test_upsert_individual_duplicate_wiki_url_does_not_create_second_row(tmp_db):
    """Calling upsert_individual twice with the same wiki_url must not produce duplicate rows."""
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/DupPerson", "full_name": "First Insert"},
        conn=tmp_db,
    )
    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/DupPerson", "full_name": "Second Insert"},
        conn=tmp_db,
    )
    cur = tmp_db.execute(
        "SELECT COUNT(*) FROM individuals WHERE wiki_url = ?", ("/wiki/DupPerson",)
    )
    assert cur.fetchone()[0] == 1, "individuals must not have duplicate wiki_url rows"


def test_upsert_individual_db_unique_constraint_enforced(tmp_db):
    """A raw INSERT of a duplicate wiki_url raises an integrity error — the constraint exists at DB level."""
    import sqlite3

    db_individuals.upsert_individual(
        {"wiki_url": "/wiki/ConstraintCheck", "full_name": "Original"},
        conn=tmp_db,
    )
    with pytest.raises(sqlite3.IntegrityError):
        tmp_db.execute(
            "INSERT INTO individuals (wiki_url) VALUES (?)", ("/wiki/ConstraintCheck",)
        )
