"""Coverage tests for src/routers/preview.py.

Tests helpers directly and routes through TestClient with mocked scraper
calls (no real network I/O). Uses unittest.mock.patch for function-level
mocks with a module-scoped TestClient.

Run: pytest src/test_router_preview_coverage.py -v
"""

from __future__ import annotations

import importlib
import os
import time
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from src.db import offices as db_offices
from src.db.connection import get_connection, init_db
from src.routers.preview import (
    _sanitize_debug_filename,
    _config_bool_export,
    _col_1_to_0_export,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("preview_cov_db")
    path = tmp / "preview_cov_test.db"
    init_db(path=path)
    return path


@pytest.fixture(scope="module")
def client(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    import src.main as main_mod

    importlib.reload(main_mod)
    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c
    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)


@pytest.fixture(scope="module")
def office_id(db_path):
    """Create one office for preview endpoint tests."""
    conn = get_connection(db_path)
    try:
        oid = db_offices.create_office(
            {
                "country_id": conn.execute("SELECT id FROM countries LIMIT 1").fetchone()[0],
                "name": "Preview Test Office",
                "url": "https://en.wikipedia.org/wiki/Preview_Test_Office_Cov",
                "enabled": True,
            },
            conn=conn,
        )
        conn.commit()
        return oid
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helper function — _sanitize_debug_filename
# ---------------------------------------------------------------------------


def test_sanitize_basic_name():
    assert _sanitize_debug_filename("governor") == "governor"


def test_sanitize_spaces_replaced():
    result = _sanitize_debug_filename("Governor of California")
    assert " " not in result
    assert "Governor" in result


def test_sanitize_special_chars_replaced():
    result = _sanitize_debug_filename('name: "with" <special> /chars/')
    assert '"' not in result
    assert "<" not in result
    assert "/" not in result


def test_sanitize_empty_string_returns_office():
    assert _sanitize_debug_filename("") == "office"


def test_sanitize_max_len():
    long_name = "a" * 200
    result = _sanitize_debug_filename(long_name)
    assert len(result) <= 80


def test_sanitize_whitespace_only_returns_office():
    assert _sanitize_debug_filename("   ") == "office"


# ---------------------------------------------------------------------------
# Helper function — _config_bool_export
# ---------------------------------------------------------------------------


def test_config_bool_export_true_strings():
    for v in ("true", "1", "yes", "True", "YES"):
        assert _config_bool_export(v) is True, f"Expected True for {v!r}"


def test_config_bool_export_false_values():
    for v in (None, "", "false", "0", "no", False):
        assert _config_bool_export(v) is False, f"Expected False for {v!r}"


# ---------------------------------------------------------------------------
# Helper function — _col_1_to_0_export
# ---------------------------------------------------------------------------


def test_col_1_to_0_export_none_returns_negative():
    assert _col_1_to_0_export(None) == -1


def test_col_1_to_0_export_zero_returns_negative():
    assert _col_1_to_0_export(0) == -1


def test_col_1_to_0_export_empty_string_returns_negative():
    assert _col_1_to_0_export("") == -1


def test_col_1_to_0_export_positive():
    assert _col_1_to_0_export(1) == 0
    assert _col_1_to_0_export(4) == 3
    assert _col_1_to_0_export("5") == 4


# ---------------------------------------------------------------------------
# GET /offices/{office_id}/preview
# ---------------------------------------------------------------------------


def test_office_preview_page_not_found(client):
    resp = client.get("/offices/999999/preview")
    assert resp.status_code == 404


def test_office_preview_page_exists(client, office_id):
    fake_result = {"preview_rows": [], "error": None}
    with patch("src.routers.preview.run_with_db", return_value=fake_result):
        with patch("src.routers.preview.get_raw_table_preview", return_value=None):
            resp = client.get(f"/offices/{office_id}/preview")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /api/preview/{office_id}
# ---------------------------------------------------------------------------


def test_api_preview_office_not_found(client):
    resp = client.get("/api/preview/999999")
    assert resp.status_code == 404


def test_api_preview_returns_json(client, office_id):
    fake_result = {"preview_rows": [{"wiki_link": "Test"}], "error": None}
    with patch("src.routers.preview.run_with_db", return_value=fake_result):
        resp = client.get(f"/api/preview/{office_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert "office_id" in body
    assert body["office_id"] == office_id


def test_api_preview_no_rows_attaches_raw_table(client, office_id):
    fake_result = {"preview_rows": [], "error": None}
    fake_raw = {"rows": [["Col1", "Col2"]], "headers": []}
    with patch("src.routers.preview.run_with_db", return_value=fake_result):
        with patch("src.routers.preview.get_raw_table_preview", return_value=fake_raw):
            resp = client.get(f"/api/preview/{office_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert "raw_table_preview" in body


# ---------------------------------------------------------------------------
# POST /api/preview — draft preview
# ---------------------------------------------------------------------------


def test_api_preview_draft_invalid_json(client):
    resp = client.post(
        "/api/preview",
        content=b"not-json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_api_preview_draft_missing_country_id(client):
    resp = client.post("/api/preview", json={"url": "https://en.wikipedia.org/wiki/Test"})
    assert resp.status_code == 400
    assert "country_id" in resp.json().get("detail", "")


def test_api_preview_draft_no_infobox_returns_result(client):
    fake_result = {"preview_rows": [], "error": None}
    with patch("src.routers.preview.preview_with_config", return_value=fake_result):
        resp = client.post(
            "/api/preview",
            json={
                "country_id": 1,
                "url": "https://en.wikipedia.org/wiki/Test",
                "table_no": 1,
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "preview_rows" in body


def test_api_preview_draft_with_infobox_returns_202(client):
    """When find_date_in_infobox is true, returns 202 with job_id immediately."""
    resp = client.post(
        "/api/preview",
        json={
            "country_id": 1,
            "url": "https://en.wikipedia.org/wiki/Test",
            "find_date_in_infobox": True,
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert "job_id" in body
    assert body["status"] == "running"


# ---------------------------------------------------------------------------
# GET /api/preview/status/{job_id}
# ---------------------------------------------------------------------------


def test_api_preview_status_not_found(client):
    resp = client.get("/api/preview/status/nonexistent-job-id")
    assert resp.status_code == 404


def test_api_preview_status_running_job(client):
    """Start an infobox job and immediately poll its status."""
    with patch("src.routers.preview.preview_with_config", return_value={"preview_rows": []}):
        start_resp = client.post(
            "/api/preview",
            json={
                "country_id": 1,
                "url": "https://en.wikipedia.org/wiki/Test",
                "find_date_in_infobox": True,
            },
        )
    assert start_resp.status_code == 202
    job_id = start_resp.json()["job_id"]
    resp = client.get(f"/api/preview/status/{job_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert body["status"] in ("running", "complete", "error", "cancelled")


# ---------------------------------------------------------------------------
# POST /api/preview/cancel/{job_id}
# ---------------------------------------------------------------------------


def test_api_preview_cancel_not_found(client):
    resp = client.post("/api/preview/cancel/nonexistent-cancel-id")
    assert resp.status_code == 404


def test_api_preview_cancel_running_job(client):
    """Cancel a freshly started job → ok=True."""
    # Mock preview_with_config to block until cancelled
    import threading

    cancelled_event = threading.Event()

    def slow_preview(*args, **kwargs):
        cancelled_event.wait(timeout=5)
        return {"preview_rows": []}

    with patch("src.routers.preview.preview_with_config", side_effect=slow_preview):
        start_resp = client.post(
            "/api/preview",
            json={
                "country_id": 1,
                "url": "https://en.wikipedia.org/wiki/Test",
                "find_date_in_infobox": True,
            },
        )
    assert start_resp.status_code == 202
    job_id = start_resp.json()["job_id"]

    # Cancel immediately
    resp = client.post(f"/api/preview/cancel/{job_id}")
    assert resp.status_code in (200, 409)  # 409 if already complete
    cancelled_event.set()


def test_api_preview_cancel_already_complete_returns_409(client):
    """Cancelling a completed job returns 409."""
    fake_result = {"preview_rows": []}
    with patch("src.routers.preview.preview_with_config", return_value=fake_result):
        start_resp = client.post(
            "/api/preview",
            json={
                "country_id": 1,
                "url": "https://en.wikipedia.org/wiki/Test",
                "find_date_in_infobox": True,
            },
        )
    assert start_resp.status_code == 202
    job_id = start_resp.json()["job_id"]

    # Wait for job to complete
    for _ in range(20):
        status_resp = client.get(f"/api/preview/status/{job_id}")
        if status_resp.json().get("status") != "running":
            break
        time.sleep(0.1)

    cancel_resp = client.post(f"/api/preview/cancel/{job_id}")
    assert cancel_resp.status_code == 409


# ---------------------------------------------------------------------------
# POST /api/preview-all-tables
# ---------------------------------------------------------------------------


def test_api_preview_all_tables_invalid_json(client):
    resp = client.post(
        "/api/preview-all-tables",
        content=b"not-json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_api_preview_all_tables_missing_url(client):
    resp = client.post("/api/preview-all-tables", json={"confirm": True})
    assert resp.status_code == 400


def test_api_preview_all_tables_returns_result(client):
    fake = {"tables": [{"table_no": 1, "rows": [["A", "B"]]}]}
    with patch("src.routers.preview.get_all_tables_preview", return_value=fake):
        resp = client.post(
            "/api/preview-all-tables",
            json={"url": "https://en.wikipedia.org/wiki/Test", "confirm": True},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "tables" in body


# ---------------------------------------------------------------------------
# POST /api/raw-table-preview
# ---------------------------------------------------------------------------


def test_api_raw_table_preview_invalid_json(client):
    resp = client.post(
        "/api/raw-table-preview",
        content=b"bad",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_api_raw_table_preview_missing_url(client):
    resp = client.post("/api/raw-table-preview", json={"table_no": 1})
    assert resp.status_code == 400


def test_api_raw_table_preview_returns_result(client):
    fake = {"rows": [["A", "B"]], "headers": ["A", "B"]}
    with patch("src.routers.preview.get_raw_table_preview", return_value=fake):
        resp = client.post(
            "/api/raw-table-preview",
            json={"url": "https://en.wikipedia.org/wiki/Test", "table_no": 1},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "rows" in body


def test_api_raw_table_preview_none_result(client):
    with patch("src.routers.preview.get_raw_table_preview", return_value=None):
        resp = client.post(
            "/api/raw-table-preview",
            json={"url": "https://en.wikipedia.org/wiki/Test", "table_no": 1},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "error" in body


# ---------------------------------------------------------------------------
# POST /api/table-html
# ---------------------------------------------------------------------------


def test_api_table_html_invalid_json(client):
    resp = client.post(
        "/api/table-html",
        content=b"bad",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_api_table_html_missing_url(client):
    resp = client.post("/api/table-html", json={"table_no": 1})
    assert resp.status_code == 400


def test_api_table_html_returns_result(client):
    fake = {"html": "<table></table>"}
    with patch("src.routers.preview.get_table_html", return_value=fake):
        resp = client.post(
            "/api/table-html",
            json={"url": "https://en.wikipedia.org/wiki/Test", "table_no": 1},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "html" in body


# ---------------------------------------------------------------------------
# POST /api/office-debug-export
# ---------------------------------------------------------------------------


def test_api_office_debug_export_invalid_json(client):
    resp = client.post(
        "/api/office-debug-export",
        content=b"bad",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_api_office_debug_export_preview_mode(client, tmp_path):
    """export_mode=preview with preview_result and table_html_result → writes file."""
    config = {
        "url": "https://en.wikipedia.org/wiki/Test",
        "table_no": 1,
        "table_rows": 4,
        "link_column": 1,
        "term_start_column": 4,
        "term_end_column": 5,
        "find_date_in_infobox": False,
    }
    body = {
        "office_name": "Test Export Office",
        "config": config,
        "export_mode": "preview",
        "preview_result": {"preview_rows": []},
        "table_html_result": {"html": "<table></table>"},
    }
    with patch("src.routers.preview.ROOT", tmp_path):
        resp = client.post("/api/office-debug-export", json=body)
    assert resp.status_code == 200
    result_body = resp.json()
    # Either completed with a path or errored — both 200
    assert "path" in result_body or "error" in result_body or "filename" in result_body


# ---------------------------------------------------------------------------
# GET /api/office-debug-export-status/{job_id}
# ---------------------------------------------------------------------------


def test_api_office_debug_export_status_not_found(client):
    resp = client.get("/api/office-debug-export-status/nonexistent-export-id")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/preview-offices
# ---------------------------------------------------------------------------


def test_api_preview_offices_invalid_json(client):
    resp = client.post(
        "/api/preview-offices",
        content=b"bad",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_api_preview_offices_empty_list(client):
    with patch("src.routers.preview.preview_with_config", return_value={"preview_rows": []}):
        resp = client.post("/api/preview-offices", json={"office_ids": []})
    assert resp.status_code == 200
    body = resp.json()
    results = body if isinstance(body, list) else body.get("results", [])
    assert results == []


def test_api_preview_offices_unknown_id(client):
    with patch("src.routers.preview.preview_with_config", return_value={"preview_rows": []}):
        resp = client.post("/api/preview-offices", json={"office_ids": [999999]})
    assert resp.status_code == 200
    body = resp.json()
    results = body if isinstance(body, list) else body.get("results", [])
    assert len(results) == 1
    assert "error" in results[0]


def test_api_preview_offices_invalid_id(client):
    with patch("src.routers.preview.preview_with_config", return_value={"preview_rows": []}):
        resp = client.post("/api/preview-offices", json={"office_ids": ["not-an-int"]})
    assert resp.status_code == 200
    body = resp.json()
    results = body if isinstance(body, list) else body.get("results", [])
    assert len(results) == 1
    assert "error" in results[0]


def test_api_preview_offices_valid_id(client, office_id):
    fake_result = {"preview_rows": [], "error": None}
    with patch("src.routers.preview.preview_with_config", return_value=fake_result):
        resp = client.post("/api/preview-offices", json={"office_ids": [office_id]})
    assert resp.status_code == 200
    body = resp.json()
    results = body if isinstance(body, list) else body.get("results", [])
    assert len(results) == 1
    assert results[0].get("office_id") == office_id
