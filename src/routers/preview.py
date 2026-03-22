# -*- coding: utf-8 -*-
"""Router: Preview (single office) — draft preview, async preview jobs, debug export."""

import re
import threading
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.routers._deps import templates
from src.routers._helpers import (
    _validate_infobox_role_key_filter_id,
    _resolve_infobox_role_key_from_filter_id,
    _office_draft_from_body,
)
from src.db import offices as db_offices
from src.db import refs as db_refs
from src.db import infobox_role_key_filter as db_infobox_role_key_filter
from src.scraper.runner import run_with_db, preview_with_config, parse_full_table_for_export
from src.scraper.config_test import (
    get_raw_table_preview,
    get_all_tables_preview,
    get_table_html,
    get_table_header_from_html,
)

router = APIRouter()

# Project root (two levels up from src/routers/preview.py)
ROOT = Path(__file__).resolve().parent.parent.parent

# ---------------------------------------------------------------------------
# In-memory job/result stores for async preview and debug export
# ---------------------------------------------------------------------------
_preview_job_store: dict = {}
_preview_job_lock = threading.Lock()
_export_job_store: dict = {}
_export_job_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PreviewCancelled(Exception):
    """Raised when the user cancels an async preview job."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sanitize_debug_filename(name: str, max_len: int = 80) -> str:
    """Replace spaces and invalid filename chars with underscore, limit length."""
    s = (name or "office").strip()
    s = re.sub(r'[<>:"/\\|?*\s]+', "_", s)
    s = s.strip("_") or "office"
    return s[:max_len] if len(s) > max_len else s


def _config_bool_export(v) -> bool:
    return v is not None and str(v).strip().lower() in ("true", "1", "yes")


def _col_1_to_0_export(v):
    val = int(v) if v is not None and v != "" else 0
    return (val - 1) if val > 0 else -1


# ---------------------------------------------------------------------------
# Async job workers
# ---------------------------------------------------------------------------


def _preview_job_worker(job_id: str, draft: dict, max_rows: "int | None"):
    def cancel_check() -> bool:
        with _preview_job_lock:
            return _preview_job_store.get(job_id, {}).get("cancelled", False)

    def progress_callback(phase: str, current: int, total: int, message: str, extra: dict):
        if cancel_check():
            raise PreviewCancelled("Stopped.")
        with _preview_job_lock:
            if job_id in _preview_job_store:
                _preview_job_store[job_id].update(
                    {
                        "phase": phase,
                        "current": current,
                        "total": total,
                        "message": message,
                        "extra": extra,
                    }
                )

    try:
        result = preview_with_config(draft, max_rows=max_rows, progress_callback=progress_callback)
        with _preview_job_lock:
            if job_id in _preview_job_store:
                _preview_job_store[job_id]["status"] = "complete"
                _preview_job_store[job_id]["result"] = result
    except PreviewCancelled:
        with _preview_job_lock:
            if job_id in _preview_job_store:
                _preview_job_store[job_id]["status"] = "cancelled"
                _preview_job_store[job_id]["result"] = {"cancelled": True, "message": "Stopped."}
    except Exception as e:
        with _preview_job_lock:
            if job_id in _preview_job_store:
                _preview_job_store[job_id]["status"] = "error"
                _preview_job_store[job_id]["error"] = str(e)


def _export_job_worker(job_id: str, office_name: str, config: dict):
    """Background worker for debug export when find_date_in_infobox: fetch table, parse with progress, write file."""

    def progress_callback(phase: str, current: int, total: int, message: str, extra: dict):
        with _export_job_lock:
            if job_id in _export_job_store:
                _export_job_store[job_id].update(
                    {
                        "phase": phase,
                        "current": current,
                        "total": total,
                        "message": message,
                        "extra": extra or {},
                    }
                )

    try:
        with _export_job_lock:
            if job_id in _export_job_store:
                _export_job_store[job_id].update({"message": "Fetching table…"})
        url = (config.get("url") or "").strip()
        if not url:
            with _export_job_lock:
                if job_id in _export_job_store:
                    _export_job_store[job_id]["status"] = "error"
                    _export_job_store[job_id]["error"] = "No URL in config"
            return
        table_no = int(config.get("table_no") or 1)
        use_full_page = _config_bool_export(config.get("use_full_page_for_table"))
        result = get_table_html(url, table_no=table_no, use_full_page=use_full_page)
        if result.get("error"):
            with _export_job_lock:
                if job_id in _export_job_store:
                    _export_job_store[job_id]["status"] = "error"
                    _export_job_store[job_id]["error"] = str(result.get("error"))
            return
        table_html = result.get("html") or ""
        table_html_result = {"html": table_html} if table_html else {"error": "No HTML"}
        office_row = {
            "url": url,
            "name": (config.get("name") or "").strip(),
            "department": (config.get("department") or "").strip(),
            "notes": (config.get("notes") or "").strip(),
            "table_no": table_no,
            "table_rows": int(config.get("table_rows") or 4),
            "link_column": int(config.get("link_column") or 0),
            "party_column": int(config.get("party_column") or 0),
            "term_start_column": int(config.get("term_start_column") or 4),
            "term_end_column": int(config.get("term_end_column") or 5),
            "district_column": int(config.get("district_column") or 0),
            "filter_column": int(config.get("filter_column") or 0),
            "filter_criteria": (config.get("filter_criteria") or ""),
            "dynamic_parse": _config_bool_export(config.get("dynamic_parse")),
            "read_right_to_left": _config_bool_export(config.get("read_right_to_left")),
            "find_date_in_infobox": _config_bool_export(config.get("find_date_in_infobox")),
            "years_only": _config_bool_export(config.get("years_only")),
            "parse_rowspan": _config_bool_export(config.get("parse_rowspan")),
            "consolidate_rowspan_terms": _config_bool_export(
                config.get("consolidate_rowspan_terms")
            ),
            "rep_link": _config_bool_export(config.get("rep_link")),
            "party_link": _config_bool_export(config.get("party_link")),
            "alt_links": (
                config.get("alt_links") if isinstance(config.get("alt_links"), list) else []
            ),
            "alt_link_include_main": _config_bool_export(config.get("alt_link_include_main")),
            "use_full_page_for_table": use_full_page,
            "term_dates_merged": _config_bool_export(config.get("term_dates_merged")),
            "party_ignore": _config_bool_export(config.get("party_ignore")),
            "district_ignore": _config_bool_export(config.get("district_ignore")),
            "district_at_large": _config_bool_export(config.get("district_at_large")),
            "ignore_non_links": _config_bool_export(config.get("ignore_non_links")),
            "remove_duplicates": _config_bool_export(config.get("remove_duplicates")),
            "infobox_role_key_filter_id": config.get("infobox_role_key_filter_id"),
            "country_name": "",
            "level_name": "",
            "branch_name": "",
            "state_name": "",
        }
        office_row["infobox_role_key"] = (
            config.get("infobox_role_key") or ""
        ).strip() or _resolve_infobox_role_key_from_filter_id(
            office_row.get("infobox_role_key_filter_id")
        )
        full_rows = parse_full_table_for_export(
            office_row, table_html, url, progress_callback=progress_callback
        )
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        safe_name = _sanitize_debug_filename(office_name)
        filename = f"{safe_name}_{timestamp}.txt"
        debug_dir = ROOT / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        filepath = debug_dir / filename
        header_cells = get_table_header_from_html(table_html) if table_html else []
        link_1 = int(config.get("link_column") or 0)
        term_start_1 = int(config.get("term_start_column") or 4)
        term_end_1 = int(config.get("term_end_column") or 5)
        link_0 = _col_1_to_0_export(link_1)
        term_start_0 = _col_1_to_0_export(term_start_1)
        term_end_0 = _col_1_to_0_export(term_end_1)
        lines = [
            "=== INSTRUCTIONS (when you share this file with Cursor) ===",
            "Help me understand the configuration issue. The problem could be:",
            "  1) I simply did not configure correctly — in that case, give me the correct configs.",
            "  2) We came across a new scenario our parser cannot handle — in that case, propose a plan.",
            "When investigating: test the HTML extract (RAW HTML section below) against our parser code",
            "to see if the issue is resolved or still outstanding (e.g. run scripts/test_debug_export.py).",
            "",
            "=== Investigation context ===",
            "Column indices: Form/DB store 1-based column numbers. The parser uses 0-based indices.",
            "Conversion happens in src/db/offices.py in office_row_to_table_config (e.g. form term_start_column 5 -> parser column index 4).",
            "How to verify: From project root run: python scripts/test_debug_export.py debug/"
            + filename,
            "Code reference:",
            "  - Config to 0-based: src/db/offices.py office_row_to_table_config, _col_1based_to_0based",
            "  - Table parsing: src/scraper/table_parser.py process_table, parse_table_row, extract_term_dates",
            "  - Date parsing: src/scraper/table_parser.py DataCleanup.format_date, DataCleanup.parse_date_info",
            "  - Preview pipeline: src/scraper/runner.py preview_with_config",
            "",
            "Office: " + office_name,
            "Exported: " + timestamp,
            "",
            "=== CONFIG (form values used for preview) ===",
        ]
        for k, v in sorted(config.items()):
            lines.append(f"{k}: {v}")
        mapping_parts = [
            "Parser columns (0-based): link=%s, term_start=%s, term_end=%s (from form 1-based: link %s, term_start %s, term_end %s)."
            % (link_0, term_start_0, term_end_0, link_1, term_start_1, term_end_1)
        ]
        if header_cells:
            h_start = (
                header_cells[term_start_0][1] if 0 <= term_start_0 < len(header_cells) else "?"
            )
            h_end = header_cells[term_end_0][1] if 0 <= term_end_0 < len(header_cells) else "?"
            mapping_parts.append("Header at term_start: %r, at term_end: %r." % (h_start, h_end))
        lines.append(" ".join(mapping_parts))
        lines.append("")
        lines.append("=== TABLE STRUCTURE (header row, 0-based column index) ===")
        if header_cells:
            for i, text in header_cells:
                lines.append("Column %s: %s" % (i, text))
        else:
            lines.append("Could not parse table header.")
        lines.append("")
        lines.append("=== EXTRACTED TABLE (full parse with above config) ===")
        if full_rows:
            headers = [
                "Wiki Link",
                "Party",
                "District",
                "Term Start",
                "Term End",
                "Term Start Year",
                "Term End Year",
                "Infobox items",
            ]
            lines.append("\t".join(headers))
            for row in full_rows:
                cells = [
                    str(row.get(h) or "").replace("\t", " ").replace("\n", " ") for h in headers
                ]
                lines.append("\t".join(cells))
        else:
            lines.append("No rows parsed.")
        lines.append("")
        lines.append("=== RAW HTML (selected table) ===")
        if table_html_result.get("error"):
            lines.append(f"Error: {table_html_result.get('error')}")
        else:
            lines.append(table_html_result.get("html") or "(empty)")
        try:
            filepath.write_text("\n".join(lines), encoding="utf-8")
        except Exception as e:
            with _export_job_lock:
                if job_id in _export_job_store:
                    _export_job_store[job_id]["status"] = "error"
                    _export_job_store[job_id]["error"] = str(e)
            return
        with _export_job_lock:
            if job_id in _export_job_store:
                _export_job_store[job_id]["status"] = "complete"
                _export_job_store[job_id]["result"] = {
                    "path": f"debug/{filename}",
                    "filename": filename,
                }
    except Exception as e:
        with _export_job_lock:
            if job_id in _export_job_store:
                _export_job_store[job_id]["status"] = "error"
                _export_job_store[job_id]["error"] = str(e)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

# ---------- Preview (single office) ----------


@router.get("/offices/{office_id}/preview", response_class=HTMLResponse)
async def office_preview_page(request: Request, office_id: int):
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404)
    # #region agent log
    try:
        import json
        from pathlib import Path

        _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
        with open(_log_path, "a", encoding="utf-8") as _f:
            _f.write(
                json.dumps(
                    {
                        "location": "main.py:office_preview_page",
                        "message": "preview page using run_with_db",
                        "data": {
                            "office_id": office_id,
                            "url": (office.get("url") or "")[:80],
                            "table_no": office.get("table_no"),
                            "hypothesisId": "H3",
                        },
                        "timestamp": __import__("time").time() * 1000,
                    }
                )
                + "\n"
            )
    except Exception:
        pass
    # #endregion
    unit_ids = db_offices.get_runnable_unit_ids_for_office(office_id) or [office_id]
    result = run_with_db(
        run_mode="delta",
        run_bio=False,
        dry_run=True,
        test_run=False,
        max_rows_per_table=10,
        office_ids=unit_ids,
    )
    rows = result.get("preview_rows") or []
    raw_table_preview = result.get("raw_table_preview")
    if not rows and office.get("url") and not raw_table_preview:
        raw_table_preview = get_raw_table_preview(
            office["url"],
            int(office.get("table_no") or 1),
            max_rows=10,
        )
    if raw_table_preview and raw_table_preview.get("rows"):
        raw_table_preview = dict(raw_table_preview)
        raw_table_preview["max_cols"] = max((len(r) for r in raw_table_preview["rows"]), default=0)
    return templates.TemplateResponse(
        request,
        "preview.html",
        {
            "office": office,
            "rows": rows,
            "error": result.get("error"),
            "raw_table_preview": raw_table_preview,
        },
    )


@router.get("/api/preview/{office_id}")
async def api_preview(office_id: int):
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404)
    unit_ids = db_offices.get_runnable_unit_ids_for_office(office_id) or [office_id]
    result = run_with_db(
        run_mode="delta",
        run_bio=False,
        dry_run=True,
        test_run=False,
        max_rows_per_table=10,
        office_ids=unit_ids,
    )
    # When parse returned no rows, attach raw table for troubleshooting
    if not result.get("preview_rows") and office.get("url"):
        raw = get_raw_table_preview(
            office["url"],
            int(office.get("table_no") or 1),
            max_rows=10,
        )
        if raw:
            result["raw_table_preview"] = raw
    # Shape expected by Preview all (office_id, name, url, preview_rows, raw_table_preview, error)
    result["office_id"] = office_id
    result["name"] = office.get("name") or ""
    result["url"] = (office.get("url") or "").strip()
    result["error"] = result.get("error")
    return JSONResponse(result)


@router.post("/api/preview")
async def api_preview_draft(request: Request):
    """Preview using draft office config (unsaved form). Body: same fields as office form (country_id, url, table_no, etc.).
    Optional: max_rows (default 10); use max_rows=0 or show_all=true to return all rows.
    When find_date_in_infobox is set, returns 202 with job_id; poll GET /api/preview/status/{job_id} for progress (Processing x of y).
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    country_id = int(body.get("country_id") or 0)
    if not country_id:
        raise HTTPException(status_code=400, detail="country_id required")
    draft = _office_draft_from_body(body, include_ref_names=True)
    try:
        db_offices.validate_office_table_config(
            draft,
            term_dates_merged=draft.get("term_dates_merged", False),
            party_ignore=draft.get("party_ignore", False),
            district_ignore=draft.get("district_ignore", False),
            district_at_large=draft.get("district_at_large", False),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    show_all = body.get("show_all") in (True, 1, "true", "1")
    max_rows_val = body.get("max_rows")
    if show_all or (max_rows_val is not None and int(max_rows_val) == 0):
        max_rows = None
    else:
        max_rows = int(max_rows_val) if max_rows_val is not None else 10
    use_infobox = bool(draft.get("find_date_in_infobox"))
    if use_infobox:
        job_id = str(uuid.uuid4())
        with _preview_job_lock:
            _preview_job_store[job_id] = {
                "status": "running",
                "phase": "infobox",
                "current": 0,
                "total": 1,
                "message": "Starting preview…",
                "extra": {},
                "cancelled": False,
            }
        thread = threading.Thread(target=_preview_job_worker, args=(job_id, draft, max_rows))
        thread.start()
        return JSONResponse({"job_id": job_id, "status": "running"}, status_code=202)
    result = preview_with_config(draft, max_rows=max_rows)
    return JSONResponse(result)


@router.get("/api/preview/status/{job_id}")
async def api_preview_status(job_id: str):
    """Poll preview job progress. Returns status, phase, current, total, message; when complete includes result (preview_rows, etc.)."""
    with _preview_job_lock:
        if job_id not in _preview_job_store:
            raise HTTPException(status_code=404, detail="Job not found")
        job = _preview_job_store[job_id]
        out = {
            "status": job["status"],
            "phase": job.get("phase", "init"),
            "current": job.get("current", 0),
            "total": job.get("total", 1),
            "message": job.get("message", "Starting…"),
            "extra": job.get("extra", {}),
        }
        if job["status"] in ("complete", "error", "cancelled"):
            out["result"] = job.get("result")
            out["error"] = job.get("error")
    return JSONResponse(out)


@router.post("/api/preview/cancel/{job_id}")
async def api_preview_cancel(job_id: str):
    """Request cancellation of an async preview job."""
    with _preview_job_lock:
        if job_id not in _preview_job_store:
            raise HTTPException(status_code=404, detail="Job not found")
        job = _preview_job_store[job_id]
        if job.get("status") != "running":
            return JSONResponse({"ok": False, "message": "Job is not running"}, status_code=409)
        job["cancelled"] = True
    return JSONResponse({"ok": True})


@router.post("/api/preview-all-tables")
async def api_preview_all_tables(request: Request):
    """Fetch URL and return all tables (top 10 rows each). If more than 10 tables, require confirm=1 in body."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    url = (body.get("url") or "").strip()
    # #region agent log
    try:
        with open("c:\\Users\\wcmch\\cursor\\office_holder\\.cursor\\debug.log", "a") as _f:
            _f.write(
                __import__("json").dumps(
                    {
                        "location": "main.py:api_preview_all_tables",
                        "message": "request",
                        "data": {
                            "url_len": len(url),
                            "url_preview": url[:80] if url else "",
                            "has_confirm": body.get("confirm") is True,
                        },
                        "timestamp": __import__("time").time() * 1000,
                    }
                )
                + "\n"
            )
    except Exception:
        pass
    # #endregion
    if not url:
        raise HTTPException(status_code=400, detail="url required")
    confirmed = body.get("confirm") is True
    result = get_all_tables_preview(
        url, max_rows_per_table=10, confirm_threshold=10, confirmed=confirmed
    )
    return JSONResponse(result)


@router.post("/api/raw-table-preview")
async def api_raw_table_preview(request: Request):
    """Fetch URL and return raw cell text for the single table at table_no (1-based). Body: { url, table_no }."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    url = (body.get("url") or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="url required")
    table_no = int(body.get("table_no") or 1)
    result = get_raw_table_preview(url, table_no=table_no, max_rows=10)
    if result is None:
        return JSONResponse({"error": "Failed to fetch URL or parse page"})
    return JSONResponse(result)


@router.post("/api/table-html")
async def api_table_html(request: Request):
    """Fetch URL and return the raw HTML of the table at table_no (1-based). Body: { url, table_no, use_full_page? }."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    url = (body.get("url") or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="url required")
    table_no = int(body.get("table_no") or 1)
    use_full_page = bool(body.get("use_full_page"))
    result = get_table_html(url, table_no=table_no, use_full_page=use_full_page)
    return JSONResponse(result)


@router.post("/api/office-debug-export")
async def api_office_debug_export(request: Request):
    """Write a debug text file with config, preview result, and table HTML. Body: office_name, config, preview_result, table_html_result.
    When find_date_in_infobox is true, returns 202 with job_id; poll GET /api/office-debug-export-status/{job_id} for progress.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    office_name = (body.get("office_name") or "office").strip()
    config = body.get("config") or {}
    preview_result = body.get("preview_result") or {}
    table_html_result = body.get("table_html_result") or {}
    export_mode = (body.get("export_mode") or "full").strip().lower()
    if export_mode not in ("preview", "full"):
        export_mode = "full"
    try:
        db_offices.validate_office_table_config(
            config,
            term_dates_merged=config.get("term_dates_merged") in (True, 1, "1", "true", "TRUE"),
            party_ignore=config.get("party_ignore") in (True, 1, "1", "true", "TRUE"),
            district_ignore=config.get("district_ignore") in (True, 1, "1", "true", "TRUE"),
            district_at_large=config.get("district_at_large") in (True, 1, "1", "true", "TRUE"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    use_infobox = _config_bool_export(config.get("find_date_in_infobox"))

    # Preview mode: no full parse, no infobox; require preview_result and table_html_result from client
    if export_mode == "preview":
        table_html = (
            (table_html_result.get("html") or "") if not table_html_result.get("error") else ""
        )
        header_cells = get_table_header_from_html(table_html) if table_html else []
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        safe_name = _sanitize_debug_filename(office_name)
        filename = f"{safe_name}_{timestamp}.txt"
        debug_dir = ROOT / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        filepath = debug_dir / filename
        link_1 = int(config.get("link_column") or 0)
        term_start_1 = int(config.get("term_start_column") or 4)
        term_end_1 = int(config.get("term_end_column") or 5)

        def _col_1_to_0(v):
            val = int(v) if v is not None and v != "" else 0
            return (val - 1) if val > 0 else -1

        link_0 = _col_1_to_0(link_1)
        term_start_0 = _col_1_to_0(term_start_1)
        term_end_0 = _col_1_to_0(term_end_1)
        lines = [
            "=== INSTRUCTIONS (when you share this file with Cursor) ===",
            "Help me understand the configuration issue. The problem could be:",
            "  1) I simply did not configure correctly — in that case, give me the correct configs.",
            "  2) We came across a new scenario our parser cannot handle — in that case, propose a plan.",
            "When investigating: test the HTML extract (RAW HTML section below) against our parser code",
            "to see if the issue is resolved or still outstanding (e.g. run scripts/test_debug_export.py).",
            "",
            "=== Investigation context ===",
            "Column indices: Form/DB store 1-based column numbers. The parser uses 0-based indices.",
            "Conversion happens in src/db/offices.py in office_row_to_table_config (e.g. form term_start_column 5 -> parser column index 4).",
            "How to verify: From project root run: python scripts/test_debug_export.py debug/"
            + filename,
            "Code reference:",
            "  - Config to 0-based: src/db/offices.py office_row_to_table_config, _col_1based_to_0based",
            "  - Table parsing: src/scraper/table_parser.py process_table, parse_table_row, extract_term_dates",
            "  - Date parsing: src/scraper/table_parser.py DataCleanup.format_date, DataCleanup.parse_date_info",
            "  - Preview pipeline: src/scraper/runner.py preview_with_config",
            "",
            "Office: " + office_name,
            "Exported: " + timestamp,
            "Export mode: Preview (offices only — no full parse, no infobox fetches)",
            "",
            "=== CONFIG (form values used for preview) ===",
        ]
        for k, v in sorted(config.items()):
            lines.append(f"{k}: {v}")
        mapping_parts = [
            "Parser columns (0-based): link=%s, term_start=%s, term_end=%s (from form 1-based: link %s, term_start %s, term_end %s)."
            % (link_0, term_start_0, term_end_0, link_1, term_start_1, term_end_1)
        ]
        if header_cells:
            h_start = (
                header_cells[term_start_0][1] if 0 <= term_start_0 < len(header_cells) else "?"
            )
            h_end = header_cells[term_end_0][1] if 0 <= term_end_0 < len(header_cells) else "?"
            mapping_parts.append("Header at term_start: %r, at term_end: %r." % (h_start, h_end))
        lines.append(" ".join(mapping_parts))
        lines.append("")
        lines.append("=== TABLE STRUCTURE (header row, 0-based column index) ===")
        if header_cells:
            for i, text in header_cells:
                lines.append("Column %s: %s" % (i, text))
        else:
            lines.append("Could not parse table header.")
        lines.append("")
        lines.append("=== EXTRACTED TABLE (Preview - offices only) ===")
        preview_rows = preview_result.get("preview_rows") or []
        if preview_rows:
            headers = [
                "Wiki Link",
                "Party",
                "District",
                "Term Start",
                "Term End",
                "Term Start Year",
                "Term End Year",
                "Infobox items",
            ]
            lines.append("\t".join(headers))
            for row in preview_rows:
                cells = [
                    str(row.get(h) or "").replace("\t", " ").replace("\n", " ") for h in headers
                ]
                lines.append("\t".join(cells))
        else:
            lines.append("No preview rows.")
        lines.append("")
        lines.append("=== RAW HTML (selected table) ===")
        if table_html_result.get("error"):
            lines.append(f"Error: {table_html_result.get('error')}")
        else:
            lines.append(table_html_result.get("html") or "(empty)")
        try:
            filepath.write_text("\n".join(lines), encoding="utf-8")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Could not write file: {e}")
        return JSONResponse({"path": f"debug/{filename}", "filename": filename})

    # Full export: async when find_date_in_infobox, else sync with full parse
    if export_mode == "full" and use_infobox:
        job_id = str(uuid.uuid4())
        with _export_job_lock:
            _export_job_store[job_id] = {
                "status": "running",
                "phase": "infobox",
                "current": 0,
                "total": 1,
                "message": "Starting export…",
                "extra": {},
            }
        thread = threading.Thread(target=_export_job_worker, args=(job_id, office_name, config))
        thread.start()
        return JSONResponse({"job_id": job_id, "status": "running"}, status_code=202)

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    safe_name = _sanitize_debug_filename(office_name)
    filename = f"{safe_name}_{timestamp}.txt"
    debug_dir = ROOT / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    filepath = debug_dir / filename

    table_html = (table_html_result.get("html") or "") if not table_html_result.get("error") else ""
    header_cells = get_table_header_from_html(table_html) if table_html else []

    def _config_bool(v):
        return v is not None and str(v).strip().lower() in ("true", "1", "yes")

    def _col_1_to_0(v):
        val = int(v) if v is not None and v != "" else 0
        return (val - 1) if val > 0 else -1

    link_1 = int(config.get("link_column") or 0)
    term_start_1 = int(config.get("term_start_column") or 4)
    term_end_1 = int(config.get("term_end_column") or 5)
    link_0 = _col_1_to_0(link_1)
    term_start_0 = _col_1_to_0(term_start_1)
    term_end_0 = _col_1_to_0(term_end_1)

    lines = [
        "=== INSTRUCTIONS (when you share this file with Cursor) ===",
        "Help me understand the configuration issue. The problem could be:",
        "  1) I simply did not configure correctly — in that case, give me the correct configs.",
        "  2) We came across a new scenario our parser cannot handle — in that case, propose a plan.",
        "When investigating: test the HTML extract (RAW HTML section below) against our parser code",
        "to see if the issue is resolved or still outstanding (e.g. run scripts/test_debug_export.py).",
        "",
        "=== Investigation context ===",
        "Column indices: Form/DB store 1-based column numbers. The parser uses 0-based indices.",
        "Conversion happens in src/db/offices.py in office_row_to_table_config (e.g. form term_start_column 5 -> parser column index 4).",
        "How to verify: From project root run: python scripts/test_debug_export.py debug/"
        + filename,
        "Code reference:",
        "  - Config to 0-based: src/db/offices.py office_row_to_table_config, _col_1based_to_0based",
        "  - Table parsing: src/scraper/table_parser.py process_table, parse_table_row, extract_term_dates",
        "  - Date parsing: src/scraper/table_parser.py DataCleanup.format_date, DataCleanup.parse_date_info",
        "  - Preview pipeline: src/scraper/runner.py preview_with_config",
        "",
        "Office: " + office_name,
        "Exported: " + timestamp,
        "",
        "=== CONFIG (form values used for preview) ===",
    ]
    for k, v in sorted(config.items()):
        lines.append(f"{k}: {v}")
    # Config column mapping
    lines.append("")
    mapping_parts = [
        "Parser columns (0-based): link=%s, term_start=%s, term_end=%s (from form 1-based: link %s, term_start %s, term_end %s)."
        % (link_0, term_start_0, term_end_0, link_1, term_start_1, term_end_1)
    ]
    if header_cells:
        h_start = header_cells[term_start_0][1] if 0 <= term_start_0 < len(header_cells) else "?"
        h_end = header_cells[term_end_0][1] if 0 <= term_end_0 < len(header_cells) else "?"
        mapping_parts.append("Header at term_start: %r, at term_end: %r." % (h_start, h_end))
    lines.append(" ".join(mapping_parts))
    # TABLE STRUCTURE (header row)
    lines.append("")
    lines.append("=== TABLE STRUCTURE (header row, 0-based column index) ===")
    if header_cells:
        for i, text in header_cells:
            lines.append("Column %s: %s" % (i, text))
    else:
        lines.append("Could not parse table header.")
    lines.append("")
    lines.append("=== EXTRACTED TABLE (full parse with above config) ===")
    # Run parser on full table HTML so export shows all rows, not just the first 10 from preview
    full_rows = []
    if table_html and config.get("url"):
        try:
            office_row = {
                "url": config.get("url") or "",
                "name": config.get("name") or "",
                "department": config.get("department") or "",
                "notes": config.get("notes") or "",
                "table_no": int(config.get("table_no") or 1),
                "table_rows": int(config.get("table_rows") or 4),
                "link_column": int(config.get("link_column") or 0),
                "party_column": int(config.get("party_column") or 0),
                "term_start_column": int(config.get("term_start_column") or 4),
                "term_end_column": int(config.get("term_end_column") or 5),
                "district_column": int(config.get("district_column") or 0),
                "filter_column": int(config.get("filter_column") or 0),
                "filter_criteria": (config.get("filter_criteria") or ""),
                "dynamic_parse": _config_bool(config.get("dynamic_parse")),
                "read_right_to_left": _config_bool(config.get("read_right_to_left")),
                "find_date_in_infobox": _config_bool(config.get("find_date_in_infobox")),
                "years_only": _config_bool(config.get("years_only")),
                "parse_rowspan": _config_bool(config.get("parse_rowspan")),
                "consolidate_rowspan_terms": _config_bool(config.get("consolidate_rowspan_terms")),
                "rep_link": _config_bool(config.get("rep_link")),
                "party_link": _config_bool(config.get("party_link")),
                "alt_links": (
                    config.get("alt_links") if isinstance(config.get("alt_links"), list) else []
                ),
                "alt_link_include_main": _config_bool(config.get("alt_link_include_main")),
                "use_full_page_for_table": _config_bool(config.get("use_full_page_for_table")),
                "term_dates_merged": _config_bool(config.get("term_dates_merged")),
                "party_ignore": _config_bool(config.get("party_ignore")),
                "district_ignore": _config_bool(config.get("district_ignore")),
                "district_at_large": _config_bool(config.get("district_at_large")),
                "remove_duplicates": _config_bool(config.get("remove_duplicates")),
                "infobox_role_key_filter_id": config.get("infobox_role_key_filter_id"),
                "country_name": "",
                "level_name": "",
                "branch_name": "",
                "state_name": "",
            }
            office_row["infobox_role_key"] = (
                config.get("infobox_role_key") or ""
            ).strip() or _resolve_infobox_role_key_from_filter_id(
                office_row.get("infobox_role_key_filter_id")
            )
            full_rows = parse_full_table_for_export(office_row, table_html, office_row["url"])
        except Exception as e:
            full_rows = []
            lines.append("Parse error: " + str(e))
    if full_rows:
        headers = [
            "Wiki Link",
            "Party",
            "District",
            "Term Start",
            "Term End",
            "Term Start Year",
            "Term End Year",
            "Infobox items",
        ]
        lines.append("\t".join(headers))
        for row in full_rows:
            cells = [str(row.get(h) or "").replace("\t", " ").replace("\n", " ") for h in headers]
            lines.append("\t".join(cells))
    else:
        preview_rows = preview_result.get("preview_rows") or []
        if preview_rows:
            lines.append("(Full parse produced no rows; showing preview sample below.)")
            headers = [
                "Wiki Link",
                "Party",
                "District",
                "Term Start",
                "Term End",
                "Term Start Year",
                "Term End Year",
                "Infobox items",
            ]
            lines.append("\t".join(headers))
            for row in preview_rows:
                cells = [
                    str(row.get(h) or "").replace("\t", " ").replace("\n", " ") for h in headers
                ]
                lines.append("\t".join(cells))
        else:
            err = preview_result.get("error")
            if err:
                lines.append(f"Error: {err}")
            raw = preview_result.get("raw_table_preview") or {}
            raw_rows = raw.get("rows") or []
            if raw_rows:
                lines.append("Parse failed. Raw table (first 10 rows):")
                for i, row in enumerate(raw_rows):
                    lines.append("  Row {}: {}".format(i + 1, " | ".join(str(c) for c in row)))
            elif not err:
                lines.append("No rows parsed.")
    lines.append("")
    lines.append("=== RAW HTML (selected table) ===")
    if table_html_result.get("error"):
        lines.append(f"Error: {table_html_result.get('error')}")
    else:
        lines.append(table_html_result.get("html") or "(empty)")

    try:
        filepath.write_text("\n".join(lines), encoding="utf-8")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not write file: {e}")

    return JSONResponse({"path": f"debug/{filename}", "filename": filename})


@router.get("/api/office-debug-export-status/{job_id}")
async def api_office_debug_export_status(job_id: str):
    """Poll debug export job progress. Returns status, phase, current, total, message; when complete includes result (path, filename)."""
    with _export_job_lock:
        if job_id not in _export_job_store:
            raise HTTPException(status_code=404, detail="Job not found")
        job = _export_job_store[job_id]
        out = {
            "status": job["status"],
            "phase": job.get("phase", "init"),
            "current": job.get("current", 0),
            "total": job.get("total", 1),
            "message": job.get("message", "Starting…"),
            "extra": job.get("extra", {}),
        }
        if job["status"] in ("complete", "error"):
            out["result"] = job.get("result")
            out["error"] = job.get("error")
    return JSONResponse(out)


@router.post("/api/preview-offices")
async def api_preview_offices(request: Request):
    """Run top-10 preview for each given office_id using saved config. Body: { office_ids: [1, 2, ...] }."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    office_ids = body.get("office_ids") or []
    if not isinstance(office_ids, list):
        office_ids = []
    results = []
    for oid in office_ids:
        try:
            oid = int(oid)
        except (TypeError, ValueError):
            results.append(
                {
                    "office_id": oid,
                    "name": None,
                    "url": None,
                    "preview_rows": [],
                    "raw_table_preview": None,
                    "error": "Invalid office id",
                }
            )
            continue
        office = db_offices.get_office(oid)
        if not office:
            results.append(
                {
                    "office_id": oid,
                    "name": None,
                    "url": None,
                    "preview_rows": [],
                    "raw_table_preview": None,
                    "error": "Office not found",
                }
            )
            continue
        name = office.get("name") or ""
        url = (office.get("url") or "").strip()
        pr = preview_with_config(office, max_rows=10)
        results.append(
            {
                "office_id": oid,
                "name": name,
                "url": url,
                "preview_rows": pr.get("preview_rows") or [],
                "raw_table_preview": pr.get("raw_table_preview"),
                "error": pr.get("error"),
            }
        )
    return JSONResponse({"results": results})
