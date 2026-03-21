# -*- coding: utf-8 -*-
"""Router: Run scraper, test scripts, and UI test routes."""

import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import requests
from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from src.routers._deps import templates
from src.db.connection import get_connection
from src.db import offices as db_offices
from src.db import individuals as db_individuals
from src.db import office_category as db_office_category
from src.db import infobox_role_key_filter as db_infobox_role_key_filter
from src.db import test_scripts as db_test_scripts
from src.scraper.runner import run_with_db
from src.scraper.config_test import get_table_html
from src.scraper.test_script_runner import run_test_script, run_test_script_from_html
from src.scraper.wiki_fetch import (
    WIKIPEDIA_REQUEST_HEADERS,
    wiki_url_to_rest_html_url,
    normalize_wiki_url,
)

router = APIRouter()

# Project root (two levels up from this file: src/routers/run.py -> project root)
ROOT = Path(__file__).resolve().parent.parent.parent

# ---------------------------------------------------------------------------
# In-memory job/result stores (only used in this section)
# ---------------------------------------------------------------------------
_run_job_store: dict = {}
_run_job_lock = threading.Lock()

_test_script_result_store: dict = {}
_test_script_result_lock = threading.Lock()

_ui_test_job_store: dict = {}
_ui_test_job_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helper: parse optional int query param
# ---------------------------------------------------------------------------

def _parse_optional_int(value: str | None) -> int | None:
    """Parse query param to int; treat None or empty string as None."""
    if value is None or not str(value).strip():
        return None
    try:
        n = int(str(value).strip())
        return n if n != 0 else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _snapshot_member_pages_for_test(
    *,
    source_url: str,
    config_json: dict,
    html_content: str,
    fixture_stem: str,
    canonical_fixture_mode: bool,
) -> tuple[dict, list[str], list[str], list[dict]]:
    """Return (config_with_fixtures, fetched_urls, saved_files, actual_rows) for infobox-enabled table tests."""
    cfg = dict(config_json or {})
    if not (cfg.get("find_date_in_infobox") and html_content.strip()):
        return cfg, [], [], []
    preview = run_test_script_from_html(
        test_type="table_config",
        html_content=html_content,
        config_json=cfg,
        source_url=source_url,
        expected_json=None,
    )
    rows = preview.get("actual") if isinstance(preview, dict) else []
    if not isinstance(rows, list):
        rows = []

    member_urls: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        u = (row.get("Wiki Link") or "").strip()
        if not u or u == "No link":
            continue
        nu = normalize_wiki_url(u) or u
        if nu in seen:
            continue
        seen.add(nu)
        member_urls.append(nu)

    fixtures: dict[str, str] = {}
    saved_files: list[str] = []
    for idx, member_url in enumerate(member_urls):
        fetch_url = wiki_url_to_rest_html_url(member_url) or member_url
        try:
            resp = requests.get(fetch_url, headers=WIKIPEDIA_REQUEST_HEADERS, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        safe_member = re.sub(r"[^a-zA-Z0-9._-]+", "_", (member_url.split("/")[-1] or f"member_{idx+1}"))
        if canonical_fixture_mode:
            rel_name = f"fixtures/{fixture_stem}__member_{safe_member}.html"
        else:
            rel_name = f"{fixture_stem}_{safe_member}.html"
        dest = db_test_scripts.TEST_SCRIPTS_DIR / rel_name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(resp.text, encoding="utf-8")
        fixtures[member_url] = rel_name
        saved_files.append(rel_name)

    if fixtures:
        cfg["_member_fixtures"] = fixtures
    return cfg, member_urls, saved_files, rows


def _table_config_properties_array(config_json: dict | None) -> list[dict]:
    """Return stable key/value array of table config properties for result payload debug JSON."""
    if not isinstance(config_json, dict):
        return []
    out = []
    for key in sorted(config_json.keys()):
        out.append({"property": key, "value": config_json.get(key)})
    return out


def _slugify_fixture_name(value: str, *, fallback: str = "fixture") -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return slug or fallback


def _build_primary_fixture_rel_path(*, test_name: str, source_url: str, canonical_fixture_mode: bool) -> str:
    if canonical_fixture_mode:
        return f"fixtures/{_slugify_fixture_name(test_name, fallback='test_script')}.html"
    safe_page = re.sub(r"[^a-zA-Z0-9._-]+", "_", (source_url.split("/")[-1] or "wiki_page"))
    return f"{uuid.uuid4().hex}_{safe_page}.html"


def _store_test_script_result(payload: dict) -> str:
    rid = uuid.uuid4().hex
    with _test_script_result_lock:
        _test_script_result_store[rid] = payload
    return rid


# ---------------------------------------------------------------------------
# Test script result routes
# ---------------------------------------------------------------------------

@router.get("/test-scripts/results/{result_id}", response_class=HTMLResponse)
async def test_script_result_page(request: Request, result_id: str):
    with _test_script_result_lock:
        payload = _test_script_result_store.get(result_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Result not found")
    return templates.TemplateResponse("test_script_result.html", {"request": request, "payload": payload, "result_id": result_id})


@router.get("/test-scripts", response_class=HTMLResponse)
async def test_scripts_page(request: Request):
    tests = db_test_scripts.list_tests()
    return templates.TemplateResponse(
        "test_scripts.html",
        {
            "request": request,
            "tests": tests,
            "can_use_office_templates": db_offices.use_hierarchy(),
            "infobox_role_key_filters": db_infobox_role_key_filter.list_infobox_role_key_filters(),
        },
    )


# ---------------------------------------------------------------------------
# UI test helper functions
# ---------------------------------------------------------------------------

def _ui_test_env_defaults() -> dict[str, str]:
    defaults: dict[str, str] = {"base_url": "http://127.0.0.1:8000"}

    offices: list[dict] = []
    try:
        offices = db_offices.list_offices() or []
    except Exception:
        offices = []

    if not offices:
        # Fallback for older schema variants where list_offices may fail.
        try:
            conn = get_connection()
            has_hierarchy = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='office_details'").fetchone()
            if has_hierarchy:
                rows = conn.execute(
                    "SELECT od.id AS id, od.source_page_id AS source_page_id FROM office_details od ORDER BY od.id"
                ).fetchall()
                offices = [{"id": int(r[0]), "source_page_id": int(r[1]) if r[1] is not None else None} for r in rows]
            else:
                rows = conn.execute("SELECT id FROM offices ORDER BY id").fetchall()
                offices = [{"id": int(r[0]), "source_page_id": None} for r in rows]
        except Exception:
            offices = []
        finally:
            try:
                conn.close()
            except Exception:
                pass

    if offices:
        defaults["edit_office_id"] = str(offices[0].get("id") or "")

    # Find one source page with at least two offices for the table reuse test.
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT source_page_id, GROUP_CONCAT(id, ',') AS office_ids, COUNT(*) AS c "
            "FROM office_details GROUP BY source_page_id HAVING COUNT(*) >= 2 ORDER BY source_page_id LIMIT 1"
        ).fetchall()
        if rows:
            ids = [x for x in str(rows[0][1] or "").split(",") if x]
            if len(ids) >= 2:
                defaults["page_edit_url"] = f"/offices/{ids[0]}"
                defaults["office_a_id"] = ids[0]
                defaults["office_b_id"] = ids[1]
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return defaults


def _set_ui_test_job(job_id: str, **updates) -> None:
    with _ui_test_job_lock:
        job = _ui_test_job_store.get(job_id, {})
        job.update(updates)
        _ui_test_job_store[job_id] = job


def _execute_ui_test_run(payload: dict, *, job_id: str | None = None) -> dict:
    """Run Playwright UI tests and return per-test results for UI display."""
    test_path = ROOT / "src" / "test_ui_edit_office_playwright.py"
    if not test_path.exists():
        return {
            "ok": False,
            "error": f"UI test file not found: {test_path}",
            "tests": [],
            "summary": {"passed": 0, "failed": 0, "skipped": 0, "errors": 1},
        }

    defaults = _ui_test_env_defaults()
    base_url = str(payload.get("base_url") or defaults.get("base_url") or "http://127.0.0.1:8000").strip()
    page_edit_url = str(payload.get("page_edit_url") or defaults.get("page_edit_url") or "").strip()
    if page_edit_url and page_edit_url.startswith("/"):
        page_edit_url = base_url.rstrip("/") + page_edit_url
    env_map = {
        "PLAYWRIGHT_BASE_URL": base_url,
        "PLAYWRIGHT_EDIT_OFFICE_ID": str(payload.get("edit_office_id") or defaults.get("edit_office_id") or "").strip(),
        "PLAYWRIGHT_PAGE_EDIT_URL": page_edit_url,
        "PLAYWRIGHT_OFFICE_A_ID": str(payload.get("office_a_id") or defaults.get("office_a_id") or "").strip(),
        "PLAYWRIGHT_OFFICE_B_ID": str(payload.get("office_b_id") or defaults.get("office_b_id") or "").strip(),
    }

    if job_id:
        _set_ui_test_job(job_id, phase="running", progress=30, message="Running pytest...")

    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tf:
        junit_path = tf.name

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        str(test_path),
        "--junitxml",
        junit_path,
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, env={**os.environ, **env_map})

    tests: list[dict] = []
    summary = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0}
    parse_error = None
    if job_id:
        _set_ui_test_job(job_id, phase="parsing", progress=75, message="Parsing test results...")
    try:
        xml_root = ET.parse(junit_path).getroot()
        for case in xml_root.findall(".//testcase"):
            name = case.attrib.get("name") or "(unnamed)"
            classname = case.attrib.get("classname") or ""
            nodeid = f"{classname}::{name}" if classname else name
            status = "passed"
            detail = ""
            if case.find("failure") is not None:
                status = "failed"
                detail = (case.find("failure").attrib.get("message") or case.find("failure").text or "").strip()
            elif case.find("error") is not None:
                status = "error"
                detail = (case.find("error").attrib.get("message") or case.find("error").text or "").strip()
            elif case.find("skipped") is not None:
                status = "skipped"
                detail = (case.find("skipped").attrib.get("message") or case.find("skipped").text or "").strip()
            if status in summary:
                summary[status] += 1
            else:
                summary["errors"] += 1
            tests.append({"name": name, "nodeid": nodeid, "status": status, "detail": detail})
    except Exception as e:
        parse_error = str(e)
        summary["errors"] += 1
    finally:
        try:
            Path(junit_path).unlink(missing_ok=True)
        except Exception:
            pass

    ok = proc.returncode == 0 and parse_error is None
    result = {
        "ok": ok,
        "command": " ".join(cmd),
        "return_code": proc.returncode,
        "summary": summary,
        "tests": tests,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "parse_error": parse_error,
        "applied_env": {k: v for k, v in env_map.items() if v},
    }
    return result


# ---------------------------------------------------------------------------
# UI test routes
# ---------------------------------------------------------------------------

@router.get("/ui-test-scripts", response_class=HTMLResponse)
async def ui_test_scripts_page(request: Request):
    return templates.TemplateResponse(
        "ui_test_scripts.html",
        {
            "request": request,
            "test_path": "src/test_ui_edit_office_playwright.py",
            "defaults": _ui_test_env_defaults(),
        },
    )


@router.get("/run-scenarios-test", response_class=HTMLResponse)
async def run_scenarios_test_page(request: Request):
    return templates.TemplateResponse(
        "run_scenarios_test.html",
        {"request": request, "script_path": "scripts/run_scenarios_test.py"},
    )


@router.post("/api/run-scenarios-test")
async def api_run_scenarios_test():
    """Run the run-scenarios test script in a subprocess (uses test DB only)."""
    script_path = ROOT / "scripts" / "run_scenarios_test.py"
    if not script_path.exists():
        raise HTTPException(status_code=500, detail=f"Test script not found: {script_path}")
    try:
        proc = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return JSONResponse(
            {
                "ok": False,
                "exit_code": -1,
                "stdout": "",
                "stderr": "Test run timed out after 120 seconds.",
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            {
                "ok": False,
                "exit_code": -1,
                "stdout": "",
                "stderr": str(e),
            },
            status_code=200,
        )
    return JSONResponse(
        {
            "ok": proc.returncode == 0,
            "exit_code": proc.returncode,
            "stdout": proc.stdout or "",
            "stderr": proc.stderr or "",
        },
    )


@router.post("/api/ui-test-scripts/run/start")
async def api_run_ui_test_scripts_start(request: Request):
    payload = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    job_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat()
    with _ui_test_job_lock:
        _ui_test_job_store[job_id] = {
            "job_id": job_id,
            "status": "running",
            "phase": "queued",
            "progress": 5,
            "message": "Queued",
            "started_at": started_at,
            "payload": payload,
        }

    def _worker():
        try:
            result = _execute_ui_test_run(payload, job_id=job_id)
            _set_ui_test_job(
                job_id,
                status="done",
                phase="done",
                progress=100,
                message="Completed",
                finished_at=datetime.utcnow().isoformat(),
                result=result,
            )
        except Exception as e:
            _set_ui_test_job(
                job_id,
                status="done",
                phase="done",
                progress=100,
                message="Failed",
                finished_at=datetime.utcnow().isoformat(),
                result={
                    "ok": False,
                    "summary": {"passed": 0, "failed": 0, "skipped": 0, "errors": 1},
                    "tests": [],
                    "stdout": "",
                    "stderr": str(e),
                    "parse_error": str(e),
                    "applied_env": {},
                },
            )

    threading.Thread(target=_worker, daemon=True).start()
    return {"ok": True, "job_id": job_id}


@router.get("/api/ui-test-scripts/run/status/{job_id}")
async def api_run_ui_test_scripts_status(job_id: str):
    with _ui_test_job_lock:
        job = _ui_test_job_store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="UI test run not found")
    response = {
        "ok": True,
        "job_id": job_id,
        "status": job.get("status"),
        "phase": job.get("phase"),
        "progress": job.get("progress", 0),
        "message": job.get("message") or "",
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
    }
    if job.get("status") == "done":
        response["result"] = job.get("result") or {}
    return response


@router.post("/api/ui-test-scripts/run")
async def api_run_ui_test_scripts(request: Request):
    # Backward-compatible synchronous endpoint used by older clients.
    payload = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return _execute_ui_test_run(payload)


@router.get("/api/test-scripts/office-templates/pages")
async def api_test_script_template_pages(q: str = Query(""), limit: int = Query(25)):
    rows = db_offices.search_pages_for_test_script_templates(q, limit=limit)
    return JSONResponse({"ok": True, "pages": rows})


@router.get("/api/test-scripts/office-templates/pages/{source_page_id}")
async def api_test_script_template_page_details(source_page_id: int):
    page = db_offices.get_page(source_page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    offices = db_offices.list_offices_for_page(source_page_id)
    office_rows = []
    for office in offices:
        table_configs = []
        for tc in office.get("table_configs") or []:
            table_configs.append(
                {
                    "id": tc.get("id"),
                    "name": tc.get("name") or "",
                    "enabled": bool(tc.get("enabled")),
                    "table_no": tc.get("table_no"),
                    "table_rows": tc.get("table_rows"),
                    "link_column": tc.get("link_column"),
                    "party_column": tc.get("party_column"),
                    "term_start_column": tc.get("term_start_column"),
                    "term_end_column": tc.get("term_end_column"),
                    "district_column": tc.get("district_column"),
                    "filter_column": tc.get("filter_column"),
                    "filter_criteria": tc.get("filter_criteria") or "",
                    "dynamic_parse": bool(tc.get("dynamic_parse")),
                    "read_right_to_left": bool(tc.get("read_right_to_left")),
                    "find_date_in_infobox": bool(tc.get("find_date_in_infobox")),
                    "years_only": bool(tc.get("years_only")),
                    "parse_rowspan": bool(tc.get("parse_rowspan")),
                    "consolidate_rowspan_terms": bool(tc.get("consolidate_rowspan_terms")),
                    "rep_link": bool(tc.get("rep_link")),
                    "party_link": bool(tc.get("party_link")),
                    "use_full_page_for_table": bool(tc.get("use_full_page_for_table")),
                    "term_dates_merged": bool(tc.get("term_dates_merged")),
                    "party_ignore": bool(tc.get("party_ignore")),
                    "district_ignore": bool(tc.get("district_ignore")),
                    "district_at_large": bool(tc.get("district_at_large")),
                    "ignore_non_links": bool(tc.get("ignore_non_links")),
                    "remove_duplicates": bool(tc.get("remove_duplicates")),
                    "infobox_role_key_filter_id": tc.get("infobox_role_key_filter_id"),
                    "infobox_role_key": (tc.get("infobox_role_key") or "").strip(),
                }
            )
        office_rows.append(
            {
                "id": office.get("id"),
                "name": office.get("name") or "",
                "alt_links": db_offices.list_alt_links(int(office.get("id") or 0)),
                "alt_link_include_main": bool(office.get("alt_link_include_main")),
                "table_configs": table_configs,
            }
        )
    return JSONResponse({"ok": True, "page": {"id": page.get("id"), "url": page.get("url") or ""}, "offices": office_rows})


@router.get("/api/test-scripts/{test_id}")
async def api_get_test_script(test_id: int):
    row = db_test_scripts.get_test(test_id)
    if not row:
        raise HTTPException(status_code=404, detail="Test script not found")
    return JSONResponse({"ok": True, "test": row})


@router.post("/api/test-scripts/preview")
async def api_preview_test_script(request: Request):
    """Fetch Wikipedia HTML, run parser preview, return html payload + actual output."""
    body = await request.json()
    source_url = (body.get("source_url") or "").strip()
    test_type = (body.get("test_type") or "table_config").strip()
    config_json = body.get("config_json") if isinstance(body.get("config_json"), dict) else {}
    expected_json = body.get("expected_json")
    if not source_url:
        raise HTTPException(status_code=400, detail="source_url is required")

    fetch_url = wiki_url_to_rest_html_url(source_url) or source_url
    try:
        resp = requests.get(fetch_url, headers=WIKIPEDIA_REQUEST_HEADERS, timeout=30)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch page: {e}") from e
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Failed to fetch page: HTTP {resp.status_code}")

    html_content = resp.text
    try:
        preview = run_test_script_from_html(
            test_type=test_type,
            html_content=html_content,
            config_json=config_json,
            source_url=source_url,
            expected_json=expected_json,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "html": html_content}, status_code=200)
    return JSONResponse({"ok": True, "html": html_content, **preview})


@router.post("/api/test-scripts")
async def api_create_test_script(request: Request):
    """Create/update script. On update, optionally overwrite local HTML snapshot."""
    body = await request.json()
    test_id_raw = body.get("test_id")
    test_id = int(test_id_raw) if str(test_id_raw).strip().isdigit() else None

    name = (body.get("name") or "").strip()
    test_type = (body.get("test_type") or "table_config").strip()
    source_url = (body.get("source_url") or "").strip()
    html_content = body.get("html") or ""
    enabled = bool(body.get("enabled", True))
    config_json = body.get("config_json") if isinstance(body.get("config_json"), dict) else {}
    expected_json = body.get("expected_json")
    overwrite_html = bool(body.get("overwrite_html", False))
    delete_existing_files = bool(body.get("delete_existing_files", False))
    canonical_fixture_mode = body.get("canonical_fixture_mode") is not False

    def _expected_missing(v):
        return v is None or (isinstance(v, list) and len(v) == 0)

    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    db_test_scripts.ensure_test_scripts_dir()

    # Update existing test
    if test_id is not None:
        existing = db_test_scripts.get_test(test_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Test script not found")

        html_file = existing.get("html_file") or ""
        if overwrite_html:
            if not html_content.strip():
                raise HTTPException(status_code=400, detail="Preview first to fetch new HTML before overwriting")
            rel_html_path = _build_primary_fixture_rel_path(
                test_name=name,
                source_url=source_url,
                canonical_fixture_mode=canonical_fixture_mode,
            )
            fixture_stem = Path(rel_html_path).stem
            dest = db_test_scripts.TEST_SCRIPTS_DIR / rel_html_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(html_content, encoding="utf-8")
            html_file = rel_html_path

            config_json, _member_urls, _member_files, auto_rows = _snapshot_member_pages_for_test(
                source_url=source_url,
                config_json=config_json,
                html_content=html_content,
                fixture_stem=fixture_stem,
                canonical_fixture_mode=canonical_fixture_mode,
            )
            if _expected_missing(expected_json):
                expected_json = auto_rows

            if delete_existing_files and (existing.get("html_file") or "").strip():
                old_rel = (existing["html_file"] or "").strip().replace("\\", "/")
                if old_rel.startswith("test_scripts/"):
                    old_rel = old_rel[len("test_scripts/"):]
                old_path = (db_test_scripts.TEST_SCRIPTS_DIR / old_rel).resolve()
                try:
                    if db_test_scripts.TEST_SCRIPTS_DIR in old_path.parents and old_path.exists():
                        old_path.unlink()
                except Exception:
                    pass
            if delete_existing_files and isinstance(existing.get("config_json"), dict):
                old_fx = existing["config_json"].get("_member_fixtures")
                if isinstance(old_fx, dict):
                    for _, rel_name in old_fx.items():
                        if not isinstance(rel_name, str):
                            continue
                        member_rel = rel_name.strip().replace("\\", "/")
                        if member_rel.startswith("test_scripts/"):
                            member_rel = member_rel[len("test_scripts/"):]
                        old_member = (db_test_scripts.TEST_SCRIPTS_DIR / member_rel).resolve()
                        try:
                            if db_test_scripts.TEST_SCRIPTS_DIR in old_member.parents and old_member.exists():
                                old_member.unlink()
                        except Exception:
                            pass

        db_test_scripts.update_test(test_id, {
            "name": name,
            "test_type": test_type,
            "enabled": enabled,
            "source_url": source_url,
            "html_file": html_file,
            "config_json": config_json,
            "expected_json": expected_json,
        })
        return JSONResponse({"ok": True, "id": test_id, "html_file": html_file, "updated": True})

    # Create new test
    if not html_content.strip():
        raise HTTPException(status_code=400, detail="Preview first to fetch HTML before saving")

    rel_html_path = _build_primary_fixture_rel_path(
        test_name=name,
        source_url=source_url,
        canonical_fixture_mode=canonical_fixture_mode,
    )
    fixture_stem = Path(rel_html_path).stem
    dest = db_test_scripts.TEST_SCRIPTS_DIR / rel_html_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(html_content, encoding="utf-8")

    config_json, _member_urls, _member_files, auto_rows = _snapshot_member_pages_for_test(
        source_url=source_url,
        config_json=config_json,
        html_content=html_content,
        fixture_stem=fixture_stem,
        canonical_fixture_mode=canonical_fixture_mode,
    )
    if _expected_missing(expected_json):
        expected_json = auto_rows

    new_test_id = db_test_scripts.create_test({
        "name": name,
        "test_type": test_type,
        "enabled": enabled,
        "source_url": source_url,
        "html_file": rel_html_path,
        "config_json": config_json,
        "expected_json": expected_json,
    })
    return JSONResponse({"ok": True, "id": new_test_id, "html_file": rel_html_path, "updated": False})


@router.post("/api/test-scripts/{test_id}/enabled")
async def api_toggle_test_script(test_id: int, enabled: int = Form(...)):
    db_test_scripts.update_test_enabled(test_id, bool(enabled))
    return JSONResponse({"ok": True})


@router.post("/api/test-scripts/{test_id}/delete")
async def api_delete_test_script(test_id: int):
    db_test_scripts.delete_test(test_id)
    return RedirectResponse("/test-scripts", status_code=303)


@router.post("/api/test-scripts/{test_id}/run")
async def api_run_one_test_script(test_id: int):
    row = db_test_scripts.get_test(test_id)
    if not row:
        raise HTTPException(status_code=404, detail="Test script not found")
    try:
        result = run_test_script(row)
        payload = {"type": "single", "test_id": test_id, "name": row.get("name"), "result": result, "config_json": row.get("config_json") or {}, "table_config_properties": _table_config_properties_array(row.get("config_json"))}
        rid = _store_test_script_result(payload)
    except Exception as e:
        payload = {"type": "single", "test_id": test_id, "name": row.get("name"), "result": {"passed": False, "error": str(e)}, "config_json": row.get("config_json") or {}, "table_config_properties": _table_config_properties_array(row.get("config_json"))}
        rid = _store_test_script_result(payload)
        return JSONResponse({"ok": False, "name": row.get("name"), "error": str(e), "passed": False, "result_id": rid, "result_url": f"/test-scripts/results/{rid}"}, status_code=200)
    return JSONResponse({"ok": True, "name": row.get("name"), "passed": bool(result.get("passed")), "result_id": rid, "result_url": f"/test-scripts/results/{rid}"})


@router.post("/api/test-scripts/run-enabled")
async def api_run_enabled_test_scripts():
    rows = [t for t in db_test_scripts.list_tests() if t.get("enabled")]
    out = []
    for row in rows:
        try:
            result = run_test_script(row)
            payload = {"type": "single", "test_id": row["id"], "name": row.get("name"), "result": result, "config_json": row.get("config_json") or {}, "table_config_properties": _table_config_properties_array(row.get("config_json"))}
            rid = _store_test_script_result(payload)
            out.append({"id": row["id"], "name": row.get("name"), "passed": bool(result.get("passed")), "result_id": rid, "result_url": f"/test-scripts/results/{rid}"})
        except Exception as e:
            payload = {"type": "single", "test_id": row["id"], "name": row.get("name"), "result": {"passed": False, "error": str(e)}, "config_json": row.get("config_json") or {}, "table_config_properties": _table_config_properties_array(row.get("config_json"))}
            rid = _store_test_script_result(payload)
            out.append({"id": row["id"], "name": row.get("name"), "passed": False, "error": str(e), "result_id": rid, "result_url": f"/test-scripts/results/{rid}"})
    return JSONResponse({"count": len(out), "passed": sum(1 for r in out if r.get("passed")), "results": out})


# ---------------------------------------------------------------------------
# Run scraper page and worker
# ---------------------------------------------------------------------------

@router.get("/run", response_class=HTMLResponse)
async def run_page(request: Request):
    offices = db_offices.list_offices()
    office_categories = db_office_category.list_office_categories()
    return templates.TemplateResponse(
        "run.html",
        {"request": request, "offices": offices, "office_categories": office_categories},
    )


def _run_job_worker(
    job_id: str,
    run_mode: str,
    run_bio: bool,
    run_office_bio: bool,
    refresh_table_cache: bool,
    dry_run: bool,
    test_run: bool,
    max_rows_per_table: int | None,
    office_id_list: list[int] | None,
    individual_ref: str | None = None,
    individual_id_list: list[int] | None = None,
    force_overwrite: bool = False,
):
    def _default_run_progress() -> dict:
        return {
            "office": {"current": 0, "total": 1, "message": "Starting…"},
            "table": {"current": 0, "total": 0, "message": ""},
            "infobox": {"current": 0, "total": 0, "message": ""},
            "bio": {"current": 0, "total": 0, "message": ""},
        }

    def _phase_bucket(phase: str) -> str:
        p = (phase or "").strip().lower()
        if p in ("bio", "living"):
            return "bio"
        if p == "infobox":
            return "infobox"
        if p == "table":
            return "table"
        return "office"

    def progress_callback(phase: str, current: int, total: int, message: str, extra: dict):
        with _run_job_lock:
            if job_id in _run_job_store:
                job = _run_job_store[job_id]
                progress = job.get("progress")
                if not isinstance(progress, dict):
                    progress = _default_run_progress()
                for bucket_name, defaults in _default_run_progress().items():
                    bucket = progress.get(bucket_name)
                    if not isinstance(bucket, dict):
                        progress[bucket_name] = dict(defaults)
                bucket_name = _phase_bucket(phase)
                bucket = progress[bucket_name]
                bucket.update({
                    "current": current,
                    "total": total,
                    "message": message,
                })
                job["progress"] = progress

                # Legacy top-level fields for existing polling clients.
                job.update({
                    "phase": phase,
                    "current": current,
                    "total": total,
                    "message": message,
                    "extra": extra,
                })

    def cancel_check() -> bool:
        with _run_job_lock:
            return _run_job_store.get(job_id, {}).get("cancelled", False)

    try:
        result = run_with_db(
            run_mode=run_mode,
            run_bio=run_bio,
            run_office_bio=run_office_bio,
            refresh_table_cache=refresh_table_cache,
            dry_run=dry_run,
            test_run=test_run,
            max_rows_per_table=max_rows_per_table,
            office_ids=office_id_list,
            individual_ref=individual_ref,
            individual_ids=individual_id_list,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
            force_overwrite=force_overwrite,
        )
        with _run_job_lock:
            if job_id in _run_job_store:
                _run_job_store[job_id]["status"] = "cancelled" if result.get("cancelled") else "complete"
                _run_job_store[job_id]["result"] = result
    except Exception as e:
        with _run_job_lock:
            if job_id in _run_job_store:
                _run_job_store[job_id]["status"] = "error"
                _run_job_store[job_id]["error"] = str(e)


@router.post("/api/run")
async def api_run(
    run_mode: str = Form("delta"),
    individual_ref: str = Form(""),
    office_category_id: str = Form(""),
    force_overwrite: str = Form(""),
    living_only: str = Form(""),
    valid_page_paths_only: str = Form(""),
):
    if run_mode == "single_bio" and not individual_ref.strip():
        raise HTTPException(status_code=400, detail="Individual (ID or Wikipedia URL) required for re-run bio.")
    force_overwrite_bool = str(force_overwrite).strip().lower() in ("1", "true", "yes")
    office_category_id_int = _parse_optional_int(office_category_id)
    living_only_bool = str(living_only).strip().lower() in ("1", "true", "yes")
    valid_page_paths_only_bool = str(valid_page_paths_only).strip().lower() in ("1", "true", "yes")
    run_bio = run_mode == "delta_live"
    run_office_bio = run_mode not in ("full_no_bio", "delta_no_bio", "full_no_bio_refresh", "delta_no_bio_refresh")
    refresh_table_cache = run_mode in ("full_no_bio_refresh", "delta_no_bio_refresh")
    if run_mode == "delta_live":
        mode = "delta"
    elif run_mode in ("full_no_bio", "full_no_bio_refresh"):
        mode = "full"
    elif run_mode in ("delta_no_bio", "delta_no_bio_refresh"):
        mode = "delta"
    else:
        mode = run_mode

    office_id_list: list[int] | None = None
    individual_id_list: list[int] | None = None
    if run_mode == "populate_category_terms":
        if not office_category_id_int:
            raise HTTPException(status_code=400, detail="Office category is required for category populate run.")
        office_id_list = db_offices.get_runnable_unit_ids_for_office_category(office_category_id_int)
        if not office_id_list:
            raise HTTPException(status_code=400, detail="No enabled office tables found for the selected office category.")
        mode = "delta"
        run_bio = False
        run_office_bio = False
        refresh_table_cache = False
    elif run_mode == "selected_bios_by_category":
        if not office_category_id_int:
            raise HTTPException(status_code=400, detail="Office category is required for selected bios run.")
        matches = db_individuals.list_individuals_for_office_category(
            office_category_id_int,
            living_only=living_only_bool,
            valid_page_paths_only=valid_page_paths_only_bool,
        )
        matched_ids = sorted({int(r.get("id")) for r in matches if r.get("id")})
        if not force_overwrite_bool:
            matched_ids = [
                i for i in matched_ids
                if not ((db_individuals.get_individual(i) or {}).get("birth_date") or "").strip()
            ]
        if not matched_ids:
            raise HTTPException(status_code=400, detail="No matching individuals for selected filters.")
        individual_id_list = matched_ids
        mode = "selected_bios"
        run_bio = False
        run_office_bio = False
        refresh_table_cache = False
    job_id = str(uuid.uuid4())
    with _run_job_lock:
        _run_job_store[job_id] = {
            "status": "running",
            "progress": {
                "office": {"current": 0, "total": 1, "message": "Starting…"},
                "table": {"current": 0, "total": 0, "message": ""},
                "infobox": {"current": 0, "total": 0, "message": ""},
                "bio": {"current": 0, "total": 0, "message": ""},
            },
            "phase": "init",
            "current": 0,
            "total": 1,
            "message": "Starting…",
            "extra": {},
            "result": None,
            "error": None,
            "cancelled": False,
        }
    thread = threading.Thread(
        target=_run_job_worker,
        args=(job_id, mode, run_bio, run_office_bio, refresh_table_cache, False, False, None, office_id_list, individual_ref.strip() or None, individual_id_list, force_overwrite_bool),
    )
    thread.start()
    return JSONResponse({"job_id": job_id}, status_code=202)


@router.get("/api/run/matching-individuals")
async def api_run_matching_individuals(
    office_category_id: int,
    living_only: int = 0,
    force_overwrite: int = 0,
    valid_page_paths_only: int = 0,
):
    rows = db_individuals.list_individuals_for_office_category(
        office_category_id,
        living_only=bool(living_only),
        valid_page_paths_only=bool(valid_page_paths_only),
    )
    unique_ids = sorted({int(r.get("id")) for r in rows if r.get("id")})
    eligible_ids = list(unique_ids)
    if not bool(force_overwrite):
        eligible_ids = [
            i for i in unique_ids
            if not ((db_individuals.get_individual(i) or {}).get("birth_date") or "").strip()
        ]
    return JSONResponse({
        "office_category_id": office_category_id,
        "living_only": bool(living_only),
        "force_overwrite": bool(force_overwrite),
        "valid_page_paths_only": bool(valid_page_paths_only),
        "matching_records": len(rows),
        "matching_individuals": len(unique_ids),
        "eligible_individuals": len(eligible_ids),
        "eligible_ids": eligible_ids,
    })


@router.get("/api/run/status/{job_id}")
async def api_run_status(job_id: str):
    with _run_job_lock:
        if job_id not in _run_job_store:
            raise HTTPException(status_code=404, detail="Job not found")
        return _run_job_store[job_id]


@router.post("/api/run/cancel/{job_id}")
async def api_run_cancel(job_id: str):
    """Set job as cancelled so the worker stops at next office iteration."""
    with _run_job_lock:
        if job_id not in _run_job_store:
            raise HTTPException(status_code=404, detail="Job not found")
        job = _run_job_store[job_id]
        if job.get("status") != "running":
            return JSONResponse({"ok": False, "message": "Job is not running"}, status_code=409)
        job["cancelled"] = True
    return JSONResponse({"ok": True})


@router.post("/api/refresh-table-cache")
async def api_refresh_table_cache(request: Request):
    """Re-fetch table HTML from Wikipedia and overwrite local cache. Body: { \"url\", \"table_no\" } or { \"office_id\" }."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    office_id = body.get("office_id")
    use_full_page = False
    if office_id is not None:
        office = db_offices.get_office(int(office_id))
        if not office:
            raise HTTPException(status_code=404, detail="Office not found")
        url = (office.get("url") or "").strip()
        table_no = int(office.get("table_no") or 1)
        use_full_page = bool(office.get("use_full_page_for_table"))
        if not url:
            raise HTTPException(status_code=400, detail="Office has no URL")
    else:
        url = (body.get("url") or "").strip()
        table_no = int(body.get("table_no") or 1)
        use_full_page = bool(body.get("use_full_page"))
        if not url:
            raise HTTPException(status_code=400, detail="url required")
    result = get_table_html(url, table_no, refresh=True, use_full_page=use_full_page)
    if "error" in result:
        raise HTTPException(status_code=502, detail=result["error"])
    return JSONResponse({"ok": True})
