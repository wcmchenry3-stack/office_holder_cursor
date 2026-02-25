import json
import sqlite3
from pathlib import Path

from src.db import test_scripts as db_test_scripts


FIXTURE_REL_PATH = "test_scripts/fixtures/sample_parser_fixture.html"


def _memory_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_import_manifest_to_db_inserts_rows(tmp_path: Path):
    manifest = tmp_path / "parser_tests.json"
    manifest.write_text(
        json.dumps(
            [
                {
                    "name": "manifest-one",
                    "test_type": "table_config",
                    "html_file": FIXTURE_REL_PATH,
                    "source_url": "https://example.test",
                    "config_json": {"table_no": 1},
                    "expected_json": [{"name": "Jane"}],
                    "enabled": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    conn = _memory_conn()
    try:
        result = db_test_scripts.import_manifest_to_db(manifest_path=manifest, conn=conn)
        assert result == {"imported": 1, "updated": 0, "skipped": 0}
        rows = db_test_scripts.list_tests(conn=conn)
        assert len(rows) == 1
        assert rows[0]["name"] == "manifest-one"
    finally:
        conn.close()


def test_seed_db_from_manifest_if_empty_only_seeds_once(tmp_path: Path):
    manifest = tmp_path / "parser_tests.json"
    manifest.write_text(
        json.dumps(
            [
                {
                    "name": "seeded-test",
                    "test_type": "table_config",
                    "html_file": FIXTURE_REL_PATH,
                    "source_url": "https://example.test",
                    "config_json": {"table_no": 1},
                    "expected_json": [],
                    "enabled": False,
                }
            ]
        ),
        encoding="utf-8",
    )

    conn = _memory_conn()
    try:
        seeded = db_test_scripts.seed_db_from_manifest_if_empty(manifest_path=manifest, conn=conn)
        assert seeded == 1
        seeded_again = db_test_scripts.seed_db_from_manifest_if_empty(manifest_path=manifest, conn=conn)
        assert seeded_again == 0
        assert len(db_test_scripts.list_tests(conn=conn)) == 1
    finally:
        conn.close()


def test_export_manifest_from_db_round_trip(tmp_path: Path):
    conn = _memory_conn()
    try:
        db_test_scripts.create_test(
            {
                "name": "exported",
                "test_type": "table_config",
                "html_file": FIXTURE_REL_PATH,
                "source_url": "https://example.test",
                "config_json": {"table_no": 1},
                "expected_json": [{"name": "Jane Doe"}],
                "enabled": True,
            },
            conn=conn,
        )
        out_manifest = tmp_path / "out.json"
        count = db_test_scripts.export_manifest_from_db(manifest_path=out_manifest, conn=conn)
        assert count == 1
        payload = json.loads(out_manifest.read_text(encoding="utf-8"))
        assert payload[0]["name"] == "exported"
        assert payload[0]["html_file"] == FIXTURE_REL_PATH
    finally:
        conn.close()
