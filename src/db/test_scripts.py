"""CRUD for parser test scripts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TEST_SCRIPTS_DIR = PROJECT_ROOT / "test_scripts"
MANIFEST_PATH = TEST_SCRIPTS_DIR / "manifest" / "parser_tests.json"


def ensure_test_scripts_dir() -> Path:
    TEST_SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    return TEST_SCRIPTS_DIR


def _manifest_item_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": row.get("name") or "",
        "test_type": row.get("test_type") or "table_config",
        "html_file": row.get("html_file") or "",
        "source_url": row.get("source_url") or "",
        "config_json": row.get("config_json") or {},
        "expected_json": row.get("expected_json"),
        "enabled": bool(row.get("enabled", True)),
    }


def _normalize_html_file_path(path_value: str) -> str:
    rel = (path_value or "").strip().replace("\\", "/")
    if not rel:
        raise ValueError("Manifest entry html_file is required")
    rel_project = rel
    if rel_project.startswith("fixtures/"):
        rel_project = f"test_scripts/{rel_project}"
    rel_path = Path(rel_project)
    if rel_path.is_absolute():
        raise ValueError(f"html_file must be relative to project root: {rel}")
    target = (PROJECT_ROOT / rel_path).resolve()
    if not target.exists() or not target.is_file():
        raise ValueError(f"html_file path does not exist: {rel}")
    return rel_path.as_posix()


def load_manifest(manifest_path: Path = MANIFEST_PATH) -> list[dict[str, Any]]:
    if not manifest_path.exists():
        return []
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Parser test manifest must be a JSON array")
    normalized: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("Each parser test manifest entry must be a JSON object")
        normalized.append(
            {
                "name": (item.get("name") or "").strip(),
                "test_type": (item.get("test_type") or "table_config").strip(),
                "html_file": _normalize_html_file_path(item.get("html_file") or ""),
                "source_url": (item.get("source_url") or "").strip(),
                "config_json": (
                    item.get("config_json") if isinstance(item.get("config_json"), dict) else {}
                ),
                "expected_json": item.get("expected_json"),
                "enabled": bool(item.get("enabled", True)),
            }
        )
    return normalized


def export_manifest_from_db(manifest_path: Path = MANIFEST_PATH, conn=None) -> int:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        rows = list_tests(conn=conn)
        payload = [
            _manifest_item_from_row(r) for r in sorted(rows, key=lambda x: x.get("name") or "")
        ]
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return len(payload)
    finally:
        if own:
            conn.close()


def import_manifest_to_db(
    manifest_path: Path = MANIFEST_PATH,
    conn=None,
    *,
    overwrite_existing: bool = False,
) -> dict[str, int]:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        imported = 0
        skipped = 0
        updated = 0
        for item in load_manifest(manifest_path=manifest_path):
            name = (item.get("name") or "").strip()
            if not name:
                raise ValueError("Manifest entry name is required")
            existing = conn.execute(
                "SELECT id FROM parser_test_scripts WHERE name = %s", (name,)
            ).fetchone()
            if existing:
                if overwrite_existing:
                    update_test(int(existing["id"]), item, conn=conn)
                    updated += 1
                else:
                    skipped += 1
                continue
            create_test(item, conn=conn)
            imported += 1
        return {"imported": imported, "updated": updated, "skipped": skipped}
    finally:
        if own:
            conn.close()


def seed_db_from_manifest_if_empty(manifest_path: Path = MANIFEST_PATH, conn=None) -> int:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        count = int(conn.execute("SELECT COUNT(*) FROM parser_test_scripts").fetchone()[0])
        if count > 0:
            return 0
        result = import_manifest_to_db(
            manifest_path=manifest_path, conn=conn, overwrite_existing=False
        )
        return int(result.get("imported", 0))
    finally:
        if own:
            conn.close()


def _ensure_table(conn) -> None:
    """Create parser_test_scripts if it doesn't exist.

    On initialised connections (via init_db) the table already exists and this
    is a no-op. On bare in-memory connections used in unit tests this creates
    the table so tests don't need a full init_db() call.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS parser_test_scripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            test_type TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            html_file TEXT NOT NULL,
            source_url TEXT,
            config_json TEXT NOT NULL,
            expected_json TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_parser_test_scripts_enabled ON parser_test_scripts(enabled)"
    )
    conn.commit()


def _parse_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ("config_json", "expected_json"):
        try:
            out[key] = json.loads(out[key]) if out.get(key) else None
        except Exception:
            out[key] = None
    out["enabled"] = bool(out.get("enabled"))
    return out


def list_tests(conn=None) -> list[dict[str, Any]]:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        cur = conn.execute("SELECT * FROM parser_test_scripts ORDER BY id DESC")
        return [_parse_row(_row_to_dict(r)) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def get_test(test_id: int, conn=None) -> dict[str, Any] | None:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        row = conn.execute("SELECT * FROM parser_test_scripts WHERE id = %s", (test_id,)).fetchone()
        return _parse_row(_row_to_dict(row)) if row else None
    finally:
        if own:
            conn.close()


def create_test(data: dict[str, Any], conn=None) -> int:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        cur = conn.execute(
            """
            INSERT INTO parser_test_scripts(name, test_type, enabled, html_file, source_url, config_json, expected_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                (data.get("name") or "").strip(),
                (data.get("test_type") or "table_config").strip(),
                1 if data.get("enabled", True) else 0,
                (data.get("html_file") or "").strip(),
                (data.get("source_url") or "").strip() or None,
                json.dumps(data.get("config_json") or {}, ensure_ascii=False),
                (
                    json.dumps(data.get("expected_json"), ensure_ascii=False)
                    if data.get("expected_json") is not None
                    else None
                ),
            ),
        )
        conn.commit()
        return int(cur.fetchone()["id"])
    finally:
        if own:
            conn.close()


def update_test_enabled(test_id: int, enabled: bool, conn=None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        conn.execute(
            "UPDATE parser_test_scripts SET enabled = %s, updated_at = NOW() WHERE id = %s",
            (1 if enabled else 0, test_id),
        )
        conn.commit()
    finally:
        if own:
            conn.close()


def delete_test(test_id: int, conn=None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        conn.execute("DELETE FROM parser_test_scripts WHERE id = %s", (test_id,))
        conn.commit()
    finally:
        if own:
            conn.close()


def update_test(test_id: int, data: dict[str, Any], conn=None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        conn.execute(
            """
            UPDATE parser_test_scripts
               SET name = %s,
                   test_type = %s,
                   enabled = %s,
                   html_file = %s,
                   source_url = %s,
                   config_json = %s,
                   expected_json = %s,
                   updated_at = NOW()
             WHERE id = %s
            """,
            (
                (data.get("name") or "").strip(),
                (data.get("test_type") or "table_config").strip(),
                1 if data.get("enabled", True) else 0,
                (data.get("html_file") or "").strip(),
                (data.get("source_url") or "").strip() or None,
                json.dumps(data.get("config_json") or {}, ensure_ascii=False),
                (
                    json.dumps(data.get("expected_json"), ensure_ascii=False)
                    if data.get("expected_json") is not None
                    else None
                ),
                test_id,
            ),
        )
        conn.commit()
    finally:
        if own:
            conn.close()
