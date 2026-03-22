"""DB layer unit tests — refs, parties, office terms, bulk import.

All tests use the `tmp_db` fixture from conftest.py (fully initialised,
seeded SQLite DB). No network access, no HTTP.

Run: pytest src/db/test_db_layer.py -v
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.db import refs as db_refs
from src.db import parties as db_parties
from src.db import offices as db_offices
from src.db import office_terms as db_terms
from src.db.bulk_import import bulk_import_offices_from_csv


# ---------------------------------------------------------------------------
# Country ref tests
# ---------------------------------------------------------------------------

def test_create_country_raises_on_duplicate_name(tmp_db):
    """Creating a country with a duplicate name raises ValueError."""
    db_refs.create_country("Testland", conn=tmp_db)
    with pytest.raises(ValueError, match="already exists"):
        db_refs.create_country("Testland", conn=tmp_db)


def test_delete_country_raises_when_in_use_by_party(tmp_db):
    """delete_country raises ValueError when a party references the country."""
    country_id = db_refs.create_country("Partyville", conn=tmp_db)
    db_parties.create_party(
        {"country_id": country_id, "party_name": "TestParty", "party_link": "/wiki/TestParty"},
        conn=tmp_db,
    )
    with pytest.raises(ValueError, match="parties"):
        db_refs.delete_country(country_id, conn=tmp_db)


# ---------------------------------------------------------------------------
# Party resolution tests
# ---------------------------------------------------------------------------

def test_resolve_party_id_by_country_matches_name_and_link(tmp_db):
    """resolve_party_id_by_country matches by name and by link."""
    country_id = db_refs.create_country("Resolveland", conn=tmp_db)
    party_id = db_parties.create_party(
        {"country_id": country_id, "party_name": "Republican", "party_link": "/wiki/Republican_Party"},
        conn=tmp_db,
    )

    assert db_parties.resolve_party_id_by_country(country_id, "Republican", conn=tmp_db) == party_id
    assert db_parties.resolve_party_id_by_country(country_id, "/wiki/Republican_Party", conn=tmp_db) == party_id
    assert db_parties.resolve_party_id_by_country(country_id, "Unknown Party", conn=tmp_db) is None


def test_resolve_party_id_by_country_returns_none_for_empty_input(tmp_db):
    """resolve_party_id_by_country returns None for empty or None input."""
    # Use seeded country_id=1 (United States of America)
    assert db_parties.resolve_party_id_by_country(1, "", conn=tmp_db) is None
    assert db_parties.resolve_party_id_by_country(1, None, conn=tmp_db) is None


# ---------------------------------------------------------------------------
# Office terms insert / count / delete
# ---------------------------------------------------------------------------

def _make_office(conn) -> tuple[int, int]:
    """Create a minimal office, return (office_details_id, office_table_config_id)."""
    data = {
        "country_id": 1,  # United States of America (seeded)
        "state_id": None,
        "city_id": None,
        "level_id": None,
        "branch_id": None,
        "department": "",
        "name": "Test Senate",
        "enabled": True,
        "notes": "",
        "url": "https://en.wikipedia.org/wiki/Test_Senate",
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
    }
    od_id = db_offices.create_office(data, conn=conn)
    cur = conn.execute("SELECT id FROM office_table_config WHERE office_details_id = ? LIMIT 1", (od_id,))
    tc_id = cur.fetchone()["id"]
    return od_id, tc_id


def test_insert_office_term_and_count_and_delete(tmp_db):
    """Insert a term, count it, then delete it."""
    od_id, tc_id = _make_office(tmp_db)

    db_terms.insert_office_term(
        office_details_id=od_id,
        office_table_config_id=tc_id,
        wiki_url="https://en.wikipedia.org/wiki/Jane_Smith",
        conn=tmp_db,
    )

    assert db_terms.count_terms_for_office(od_id, conn=tmp_db) == 1

    deleted = db_terms.delete_office_terms_for_office(tc_id, conn=tmp_db)
    assert deleted == 1
    assert db_terms.count_terms_for_office(od_id, conn=tmp_db) == 0


# ---------------------------------------------------------------------------
# Bulk import CSV
# ---------------------------------------------------------------------------

def test_bulk_import_offices_from_csv_valid_and_invalid_rows(tmp_db, tmp_path):
    """CSV bulk import: 1 valid row + 2 invalid rows → (imported=1, errors=2)."""
    header = "Country,Level,Branch,Department,Name,State,URL,Table No,Table Rows,Link Column,Party Column,Term Start Column,Term End Column,District,Dynamic Parse,Read columns right to left,Find Date,Parse Rowspan,Consolidate Rowspan Terms,Rep Link,Party Link,Notes,Alt Link\n"
    row_valid = "United States of America,Federal,Executive,,Valid Office,,https://en.wikipedia.org/wiki/Valid,1,4,1,0,4,5,0,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,,\n"
    row_no_url = "United States of America,,,, No URL Office,,,1,4,1,0,4,5,0,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,,\n"
    row_bad_country = "Nonexistent Country,,,, Bad Country,,https://en.wikipedia.org/wiki/Bad,1,4,1,0,4,5,0,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,,\n"

    csv_file = tmp_path / "import.csv"
    csv_file.write_text(header + row_valid + row_no_url + row_bad_country, encoding="utf-8")

    imported, errors = bulk_import_offices_from_csv(csv_file, conn=tmp_db)
    assert imported == 1
    assert errors == 2
