# -*- coding: utf-8 -*-
"""Router: Playwright UI test orchestration."""

import os
import subprocess
import sys
import tempfile
import threading
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.routers._deps import templates
from src.db.connection import get_connection
from src.db import offices as db_offices

router = APIRouter()

# Project root (three levels up from this file: src/routers/ui_tests.py -> project root)
ROOT = Path(__file__).resolve().parent.parent.parent

# ---------------------------------------------------------------------------
# In-memory job store
# ---------------------------------------------------------------------------

_ui_test_job_store: dict = {}
_ui_test_job_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helper functions
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
        request, "ui_test_scripts.html",
        {
            "test_path": "src/test_ui_edit_office_playwright.py",
            "defaults": _ui_test_env_defaults(),
        },
    )


@router.get("/run-scenarios-test", response_class=HTMLResponse)
async def run_scenarios_test_page(request: Request):
    return templates.TemplateResponse(
        request, "run_scenarios_test.html",
        {"script_path": "scripts/run_scenarios_test.py"},
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
