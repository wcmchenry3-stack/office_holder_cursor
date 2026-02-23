from pathlib import Path

from src.db import offices
from src.db.connection import get_connection, init_db


def _base_data(table_configs):
    return {
        "country_id": 1,
        "state_id": None,
        "city_id": None,
        "level_id": None,
        "branch_id": None,
        "department": "",
        "name": "Office A",
        "enabled": True,
        "notes": "",
        "url": "https://en.wikipedia.org/wiki/Test",
        "table_configs": table_configs,
    }


def _tc(table_no, tc_id=None, name=""):
    row = {
        "name": name,
        "table_no": table_no,
        "table_rows": 1,
        "link_column": 1,
        "party_column": 0,
        "term_start_column": 2,
        "term_end_column": 3,
        "district_column": 0,
        "enabled": 1,
    }
    if tc_id is not None:
        row["id"] = tc_id
    return row


def test_create_office_with_row_filter_columns_persists_values(tmp_path: Path):
    db_path = tmp_path / "test_filter.db"
    init_db(db_path)
    conn = get_connection(db_path)
    try:
        data = _base_data([
            {
                **_tc(1, name="filtered"),
                "filter_column": 5,
                "filter_criteria": "Associate Justice",
            }
        ])

        office_id = offices.create_office(data, conn)

        saved = conn.execute(
            "SELECT filter_column, filter_criteria FROM office_table_config WHERE office_details_id = ?",
            (office_id,),
        ).fetchone()
        assert saved is not None
        assert int(saved["filter_column"]) == 5
        assert (saved["filter_criteria"] or "") == "Associate Justice"
    finally:
        conn.close()


def test_update_office_allows_renumbering_without_transient_unique_conflict(tmp_path: Path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = get_connection(db_path)
    try:
        office_id = offices.create_office(
            _base_data([_tc(2, name="t2"), _tc(3, name="t3")]),
            conn,
        )

        rows = conn.execute(
            "SELECT id, table_no FROM office_table_config WHERE office_details_id = ? ORDER BY table_no",
            (office_id,),
        ).fetchall()

        ok = offices.update_office(
            office_id,
            _base_data(
                [
                    _tc(3, tc_id=rows[0]["id"], name="t2"),
                    _tc(4, tc_id=rows[1]["id"], name="t3"),
                ]
            ),
            conn,
        )

        assert ok is True
        updated = conn.execute(
            "SELECT table_no FROM office_table_config WHERE office_details_id = ? ORDER BY table_no",
            (office_id,),
        ).fetchall()
        assert [r["table_no"] for r in updated] == [3, 4]
    finally:
        conn.close()
