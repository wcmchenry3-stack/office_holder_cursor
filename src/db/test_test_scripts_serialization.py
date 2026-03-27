import sqlite3

from src.db import test_scripts as db_test_scripts
from src.db.connection import _SQLiteConnWrapper


def _memory_conn() -> _SQLiteConnWrapper:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return _SQLiteConnWrapper(conn)


def test_create_test_serializes_infobox_role_key_filter_id_round_trip():
    conn = _memory_conn()

    test_id = db_test_scripts.create_test(
        {
            "name": "serialization check",
            "test_type": "table_config",
            "enabled": True,
            "html_file": "sample.html",
            "source_url": "https://en.wikipedia.org/wiki/Sample",
            "config_json": {
                "table_no": 1,
                "infobox_role_key": '"senior judge"',
                "infobox_role_key_filter_id": 7,
            },
            "expected_json": [],
        },
        conn=conn,
    )

    row = db_test_scripts.get_test(test_id, conn=conn)
    assert row is not None
    assert row["config_json"]["infobox_role_key"] == '"senior judge"'
    assert row["config_json"]["infobox_role_key_filter_id"] == 7


def test_update_test_serializes_infobox_role_key_filter_id_round_trip():
    conn = _memory_conn()

    test_id = db_test_scripts.create_test(
        {
            "name": "serialization check update",
            "test_type": "table_config",
            "enabled": True,
            "html_file": "sample.html",
            "source_url": "https://en.wikipedia.org/wiki/Sample",
            "config_json": {"table_no": 1},
            "expected_json": [],
        },
        conn=conn,
    )

    db_test_scripts.update_test(
        test_id,
        {
            "name": "serialization check update",
            "test_type": "table_config",
            "enabled": True,
            "html_file": "sample.html",
            "source_url": "https://en.wikipedia.org/wiki/Sample",
            "config_json": {
                "table_no": 1,
                "infobox_role_key": '"associate justice"',
                "infobox_role_key_filter_id": 9,
            },
            "expected_json": [],
        },
        conn=conn,
    )

    row = db_test_scripts.get_test(test_id, conn=conn)
    assert row is not None
    assert row["config_json"]["infobox_role_key"] == '"associate justice"'
    assert row["config_json"]["infobox_role_key_filter_id"] == 9
