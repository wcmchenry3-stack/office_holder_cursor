# -*- coding: utf-8 -*-
"""Router: Main scraper run — start, status, cancel, and table-cache refresh."""

import threading
import uuid
from pathlib import Path

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.routers._deps import templates
from src.routers._helpers import _parse_optional_int
from src.db import offices as db_offices
from src.db import individuals as db_individuals
from src.db import office_category as db_office_category
from src.scraper.runner import run_with_db
from src.scraper.config_test import get_table_html

router = APIRouter()

# ---------------------------------------------------------------------------
# In-memory job store
# ---------------------------------------------------------------------------

_run_job_store: dict = {}
_run_job_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Run scraper page and worker
# ---------------------------------------------------------------------------

@router.get("/run", response_class=HTMLResponse)
async def run_page(request: Request):
    offices = db_offices.list_offices()
    office_categories = db_office_category.list_office_categories()
    return templates.TemplateResponse(
        request, "run.html",
        {"offices": offices, "office_categories": office_categories},
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
