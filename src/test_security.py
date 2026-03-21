"""Security regression tests — runs as part of the normal pytest suite.

Uses FastAPI TestClient (no live server). The DB is initialised into a
temp directory; Datasette startup is suppressed so no subprocess is spawned.

Run in isolation:  pytest src/test_security.py -v
Run with marker:   pytest -m security -v
"""

from __future__ import annotations

import io
import importlib
from pathlib import Path

import pytest
from starlette.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client(tmp_path_factory, monkeypatch_module=None):
    """TestClient with a temp DB and Datasette suppressed."""
    # Use a module-scoped tmp dir so the DB is shared across all tests here.
    tmp = tmp_path_factory.mktemp("sec_db")
    db_path = tmp / "security_test.db"

    import os
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)

    # Suppress Datasette subprocess
    import src.main as main_mod
    original_start = main_mod._start_datasette
    original_stop = main_mod._stop_datasette
    main_mod._start_datasette = lambda: None
    main_mod._stop_datasette = lambda: None

    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c

    main_mod._start_datasette = original_start
    main_mod._stop_datasette = original_stop
    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)


# ---------------------------------------------------------------------------
# H1 — Security headers
# ---------------------------------------------------------------------------

@pytest.mark.security
def test_security_headers_present(client):
    """Every response must carry the three baseline security headers."""
    resp = client.get("/run")
    assert resp.status_code == 200
    assert resp.headers.get("x-frame-options") == "DENY", \
        "X-Frame-Options header missing or incorrect"
    assert resp.headers.get("x-content-type-options") == "nosniff", \
        "X-Content-Type-Options header missing"
    assert resp.headers.get("referrer-policy") == "strict-origin-when-cross-origin", \
        "Referrer-Policy header missing"


# ---------------------------------------------------------------------------
# H2 — Auth enforcement
# ---------------------------------------------------------------------------

@pytest.mark.security
def test_auth_middleware_redirects_unauthenticated_requests():
    """With auth enabled, protected routes must redirect to /login."""
    import os
    prior_db = os.environ.get("OFFICE_HOLDER_DB_PATH")
    tmp_db = Path(__file__).parent.parent / "tmp_auth_test.db"
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(tmp_db)

    import src.main as main_mod
    main_mod._start_datasette = lambda: None
    main_mod._stop_datasette = lambda: None
    original = main_mod._AUTH_ENABLED
    main_mod._AUTH_ENABLED = True

    try:
        # allow_redirects=False so we catch the redirect itself, not its destination
        with TestClient(main_mod.app, raise_server_exceptions=False) as c:
            resp = c.get("/run", follow_redirects=False)
            assert resp.status_code in (301, 302, 307, 308), \
                f"Expected redirect for unauthenticated request, got {resp.status_code}"
            assert "/login" in resp.headers.get("location", ""), \
                "Redirect must point to /login"
    finally:
        main_mod._AUTH_ENABLED = original
        if prior_db is not None:
            os.environ["OFFICE_HOLDER_DB_PATH"] = prior_db
        else:
            os.environ.pop("OFFICE_HOLDER_DB_PATH", None)
        if tmp_db.exists():
            tmp_db.unlink()


@pytest.mark.security
def test_auth_middleware_allows_public_paths():
    """Login page must remain accessible without a session even when auth is enabled."""
    import os
    prior_db = os.environ.get("OFFICE_HOLDER_DB_PATH")
    tmp_db = Path(__file__).parent.parent / "tmp_auth_pub_test.db"
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(tmp_db)

    import src.main as main_mod
    main_mod._start_datasette = lambda: None
    main_mod._stop_datasette = lambda: None
    original = main_mod._AUTH_ENABLED
    main_mod._AUTH_ENABLED = True

    try:
        with TestClient(main_mod.app, raise_server_exceptions=False) as c:
            resp = c.get("/login", follow_redirects=False)
            assert resp.status_code == 200, \
                f"/login must be publicly accessible, got {resp.status_code}"
    finally:
        main_mod._AUTH_ENABLED = original
        if prior_db is not None:
            os.environ["OFFICE_HOLDER_DB_PATH"] = prior_db
        else:
            os.environ.pop("OFFICE_HOLDER_DB_PATH", None)
        if tmp_db.exists():
            tmp_db.unlink()


# ---------------------------------------------------------------------------
# H3 — Path disclosure in /offices/import
# ---------------------------------------------------------------------------

@pytest.mark.security
def test_offices_import_error_does_not_leak_resolved_path(client):
    """A path-traversal attempt must not disclose the server's filesystem layout."""
    resp = client.post("/offices/import", data={"csv_path": "../../etc/passwd"})
    body = resp.text
    # The resolved absolute path must not appear in the response body
    assert "/etc/passwd" not in body, \
        "Response leaks resolved path — path disclosure vulnerability"
    assert "C:\\" not in body and "C:/" not in body, \
        "Response leaks Windows absolute path"


@pytest.mark.security
def test_offices_import_rejects_empty_path(client):
    """Empty csv_path must return a user-friendly error, not a 500."""
    resp = client.post("/offices/import", data={"csv_path": ""})
    assert resp.status_code == 200, "Should render error page (200), not crash"
    assert "required" in resp.text.lower() or "error" in resp.text.lower(), \
        "Expected an error message for empty path"


# ---------------------------------------------------------------------------
# H5 — SQL injection resistance
# ---------------------------------------------------------------------------

@pytest.mark.security
def test_sql_injection_in_offices_query_params(client):
    """SQL injection strings in query params must not crash the app or corrupt the DB."""
    payloads = [
        "'; DROP TABLE offices; --",
        "1 UNION SELECT * FROM sqlite_master --",
        "' OR '1'='1",
    ]
    for payload in payloads:
        resp = client.get(f"/offices?search_url={payload}")
        assert resp.status_code == 200, \
            f"SQL injection payload crashed the app: {payload!r}"

    # DB must still be intact
    resp = client.get("/offices")
    assert resp.status_code == 200, "offices route broken after injection attempt"


# ---------------------------------------------------------------------------
# H6 — File upload validation
# ---------------------------------------------------------------------------

@pytest.mark.security
def test_csv_upload_rejects_non_csv_extension(client):
    """Uploading a non-CSV file to /parties/import must be rejected gracefully."""
    fake_exe = io.BytesIO(b"MZ\x90\x00")  # PE header magic bytes
    resp = client.post(
        "/parties/import",
        files={"file": ("malware.exe", fake_exe, "application/octet-stream")},
    )
    # Must not be a 500; should be 200 (error page) or 400/422
    assert resp.status_code != 500, "File upload validation raised unhandled 500"
    assert resp.status_code in (200, 400, 422), \
        f"Unexpected status {resp.status_code} for non-CSV upload"
    if resp.status_code == 200:
        assert "csv" in resp.text.lower() or "error" in resp.text.lower(), \
            "Expected a validation error message for non-CSV file"


# ---------------------------------------------------------------------------
# H4 & H7 — Input validation on /api/run
# ---------------------------------------------------------------------------

@pytest.mark.security
def test_run_api_requires_individual_ref_for_single_bio_mode(client):
    """single_bio mode without individual_ref must return 400, not start a job."""
    resp = client.post("/api/run", data={"run_mode": "single_bio", "individual_ref": ""})
    assert resp.status_code == 400, \
        f"Expected 400 for missing individual_ref, got {resp.status_code}"


@pytest.mark.security
def test_run_api_invalid_run_mode_does_not_crash(client):
    """Garbage / XSS payload in run_mode must not cause a 500."""
    resp = client.post(
        "/api/run",
        data={"run_mode": "<script>alert(1)</script>", "individual_ref": ""},
    )
    assert resp.status_code != 500, \
        f"Invalid run_mode caused unhandled 500: {resp.text[:200]}"


# ---------------------------------------------------------------------------
# H7 — Type safety on path parameters
# ---------------------------------------------------------------------------

@pytest.mark.security
def test_integer_path_param_type_safety(client):
    """Non-integer office ID in URL must return 422 (FastAPI validation), not 500."""
    resp = client.get("/offices/not-a-number")
    assert resp.status_code in (404, 422), \
        f"Expected 404 or 422 for non-integer office ID, got {resp.status_code}"
