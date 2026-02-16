# -*- coding: utf-8 -*-
"""
Office Holder app: local UI and API for Wikipedia office/bio scraper.
Run: uvicorn src.main:app --reload
From project root: office_holder/
"""

import json
import re
import tempfile
from datetime import datetime
from pathlib import Path
import sys
import threading
import uuid

# Ensure project root is on path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi import FastAPI, File, Request, Form, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.db.connection import init_db, get_connection
from src.db import offices as db_offices
from src.db import parties as db_parties
from src.db import refs as db_refs
from src.db import individuals as db_individuals
from src.db import office_terms as db_office_terms
from src.db import reports as db_reports
from src.db.bulk_import import bulk_import_offices_from_csv, bulk_import_parties_from_csv
from src.scraper.runner import run_with_db, preview_with_config, parse_full_table_for_export
from src.scraper.config_test import test_office_config, get_raw_table_preview, get_all_tables_preview, get_table_html, get_table_header_from_html

app = FastAPI(title="Office Holder")
# Resolve to absolute path so template dir is correct regardless of process cwd
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Job progress store for async run (in-memory, single-user)
_run_job_store: dict = {}
_run_job_lock = threading.Lock()
_populate_job_store: dict = {}
_populate_job_lock = threading.Lock()
_preview_job_store: dict = {}
_preview_job_lock = threading.Lock()
_export_job_store: dict = {}
_export_job_lock = threading.Lock()

# Stoppable process types: server-side (e.g. "run") have a cancel endpoint and job store with cancelled flag;
# client-side (e.g. "preview_all") use a Stop button and a running/stopped flag (optional AbortController).
# To add a new type: follow the same pattern (job store + cancel_check + cancel API for server-side;
# flag + Stop button for client-side) and append to this list.
PROCESS_TYPES = ["run", "preview_all"]


class PreviewCancelled(Exception):
    """Raised when the user cancels an async preview job."""


def _office_draft_from_body(body: dict, *, include_ref_names: bool = False) -> dict:
    """Build office draft dict from JSON body. If include_ref_names, add country_name, level_name, branch_name, state_name from db_refs."""
    term_dates_merged = body.get("term_dates_merged") in (True, 1, "1", "true", "TRUE")
    party_ignore = body.get("party_ignore") in (True, 1, "1", "true", "TRUE")
    district_ignore = body.get("district_ignore") in (True, 1, "1", "true", "TRUE")
    district_at_large = body.get("district_at_large") in (True, 1, "1", "true", "TRUE")
    term_start = int(body.get("term_start_column") or 4)
    term_end = int(body.get("term_end_column") or 5) if not term_dates_merged else term_start
    draft = {
        "url": (body.get("url") or "").strip(),
        "name": (body.get("name") or "").strip(),
        "department": (body.get("department") or "").strip(),
        "notes": (body.get("notes") or "").strip(),
        "table_no": int(body.get("table_no") or 1),
        "table_rows": int(body.get("table_rows") or 4),
        "link_column": int(body.get("link_column") or 1),
        "party_column": int(body.get("party_column") or 0),
        "term_start_column": term_start,
        "term_end_column": term_end,
        "district_column": int(body.get("district_column") or 0),
        "dynamic_parse": body.get("dynamic_parse", True),
        "read_right_to_left": body.get("read_right_to_left", False),
        "find_date_in_infobox": body.get("find_date_in_infobox", False),
        "years_only": body.get("years_only", False),
        "parse_rowspan": body.get("parse_rowspan", False),
        "consolidate_rowspan_terms": body.get("consolidate_rowspan_terms", False),
        "rep_link": body.get("rep_link", False),
        "party_link": body.get("party_link", False),
        "alt_links": body.get("alt_links") if isinstance(body.get("alt_links"), list) else ([(body.get("alt_link") or "").strip()] if (body.get("alt_link") or "").strip() else []),
        "alt_link_include_main": body.get("alt_link_include_main", False),
        "use_full_page_for_table": body.get("use_full_page_for_table", False),
        "term_dates_merged": term_dates_merged,
        "party_ignore": party_ignore,
        "district_ignore": district_ignore,
        "district_at_large": district_at_large,
    }
    if include_ref_names:
        country_id = int(body.get("country_id") or 0)
        draft["country_name"] = db_refs.get_country_name(country_id)
        draft["level_name"] = db_refs.get_level_name(int(body.get("level_id") or 0) or None)
        draft["branch_name"] = db_refs.get_branch_name(int(body.get("branch_id") or 0) or None)
        draft["state_name"] = db_refs.get_state_name(int(body.get("state_id") or 0) or None)
    return draft


@app.on_event("startup")
def startup():
    try:
        init_db()
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise RuntimeError(f"Database startup failed: {e}") from e


# ---------- Office config CRUD ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return RedirectResponse("/offices", status_code=302)


@app.get("/offices", response_class=HTMLResponse)
async def offices_list(request: Request):
    offices = db_offices.list_offices()
    counts = db_office_terms.get_terms_counts_by_office()
    for o in offices:
        o["terms_count"] = counts.get(o["id"], 0)
    saved = request.query_params.get("saved") == "1"
    validation_error = request.query_params.get("error") or None
    imported_count = request.query_params.get("count")
    imported_errors = request.query_params.get("errors")
    imported = request.query_params.get("imported") == "1"
    return templates.TemplateResponse(
        "offices.html",
        {"request": request, "offices": offices, "saved": saved, "validation_error": validation_error, "imported": imported, "imported_count": imported_count, "imported_errors": imported_errors},
    )


@app.get("/offices/new", response_class=HTMLResponse)
async def office_new(request: Request):
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        "page_form.html",
        {"request": request, "office": None, "countries": countries, "levels": levels, "branches": branches, "states": [], "nav_ids": "", "nav_prev_id": None, "nav_next_id": None, "terms_count": 0, "form_template": "page_form"},
    )


@app.post("/offices/new")
async def office_create(request: Request):
    form = await request.form()
    action = form.get("action", "save_and_close")
    alt_links = [v.strip() for v in form.getlist("alt_links") if v and isinstance(v, str) and v.strip()]
    alt_link_include_main = form.get("alt_link_include_main") == "1"
    data = {
        "country_id": int(form.get("country_id") or 0), "state_id": int(form.get("state_id") or 0) or None, "level_id": int(form.get("level_id") or 0) or None, "branch_id": int(form.get("branch_id") or 0) or None,
        "department": (form.get("department") or "").strip(), "name": (form.get("name") or "").strip(), "enabled": form.get("enabled") == "1", "notes": (form.get("notes") or "").strip(), "url": (form.get("url") or "").strip(),
        "table_no": int(form.get("table_no") or 1), "table_rows": int(form.get("table_rows") or 4),
        "link_column": int(form.get("link_column") or 1), "party_column": int(form.get("party_column") or 0),
        "term_start_column": int(form.get("term_start_column") or 4), "term_end_column": int(form.get("term_end_column") or 5),
        "district_column": int(form.get("district_column") or 0),
        "dynamic_parse": form.get("dynamic_parse") == "1",
        "read_right_to_left": form.get("read_right_to_left") == "1",
        "find_date_in_infobox": form.get("date_source") == "find_date_in_infobox",
        "years_only": form.get("date_source") == "years_only",
        "parse_rowspan": form.get("parse_rowspan") == "1",
        "consolidate_rowspan_terms": form.get("consolidate_rowspan_terms") == "1",
        "rep_link": form.get("rep_link") == "1",
        "party_link": form.get("party_link") == "1",
        "alt_links": alt_links,
        "alt_link_include_main": alt_link_include_main,
        "use_full_page_for_table": form.get("use_full_page_for_table") == "1",
        "term_dates_merged": form.get("term_dates_merged") == "1",
        "party_ignore": form.get("party_ignore") == "1",
        "district_ignore": (form.get("district_mode") or "column") == "no_district",
        "district_at_large": (form.get("district_mode") or "column") == "at_large",
    }
    try:
        new_id = db_offices.create_office(data)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse("/offices?error=" + quote(str(e)), status_code=302)
    if action == "save":
        return RedirectResponse(f"/offices/{new_id}?saved=1", status_code=302)
    return RedirectResponse("/offices?saved=1", status_code=302)


# ---------- Bulk import (must be before /offices/{office_id} so "import" is not matched as office_id) ----------
@app.get("/offices/import", response_class=HTMLResponse)
async def offices_import_page(request: Request):
    # #region agent log
    try:
        with open(ROOT / ".cursor" / "debug.log", "a", encoding="utf-8") as _f:
            _f.write('{"id":"import_page","timestamp":' + str(int(__import__("time").time() * 1000)) + ',"location":"main.py:offices_import_page","message":"GET /offices/import handler entered","data":{"path":"/offices/import"},"hypothesisId":"A"}\n')
    except Exception:
        pass
    # #endregion
    return templates.TemplateResponse("import.html", {"request": request})


@app.post("/offices/import")
async def offices_import(request: Request, csv_path: str = Form("")):
    if not csv_path.strip():
        return templates.TemplateResponse("import.html", {"request": request, "error": "Path is required"})
    path = Path(csv_path.strip())
    if not path.is_absolute():
        path = ROOT / path
    if not path.exists():
        return templates.TemplateResponse("import.html", {"request": request, "error": f"File not found: {path}"})
    try:
        imported, errors = bulk_import_offices_from_csv(path)
        return RedirectResponse(f"/offices?imported=1&count={imported}&errors={errors}", status_code=302)
    except Exception as e:
        return templates.TemplateResponse("import.html", {"request": request, "error": str(e)})


@app.post("/offices/add-office-to-page")
async def office_add_to_page(request: Request):
    """Add a new office (and table) to an existing page. Form: source_page_id. Redirects to new office edit."""
    form = await request.form()
    try:
        source_page_id = int(form.get("source_page_id") or 0)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="source_page_id required")
    if source_page_id <= 0:
        raise HTTPException(status_code=400, detail="source_page_id required")
    offices_on_page = db_offices.list_offices_for_page(source_page_id)
    if not offices_on_page:
        raise HTTPException(status_code=404, detail="Page not found or has no offices")
    first = offices_on_page[0]
    data = {
        "country_id": first.get("country_id") or 0,
        "state_id": first.get("state_id"),
        "level_id": first.get("level_id"),
        "branch_id": first.get("branch_id"),
        "department": (first.get("department") or "").strip(),
        "name": "New office",
        "enabled": False,
        "notes": "",
        "url": first.get("url") or "",
        "table_no": int(first.get("table_no") or 1),
        "table_rows": int(first.get("table_rows") or 4),
        "link_column": int(first.get("link_column") or 1),
        "party_column": int(first.get("party_column") or 0),
        "term_start_column": int(first.get("term_start_column") or 4),
        "term_end_column": int(first.get("term_end_column") or 5),
        "district_column": int(first.get("district_column") or 0),
        "dynamic_parse": bool(first.get("dynamic_parse")),
        "read_right_to_left": bool(first.get("read_right_to_left")),
        "find_date_in_infobox": bool(first.get("find_date_in_infobox")),
        "years_only": bool(first.get("years_only")),
        "parse_rowspan": bool(first.get("parse_rowspan")),
        "consolidate_rowspan_terms": bool(first.get("consolidate_rowspan_terms")),
        "rep_link": bool(first.get("rep_link")),
        "party_link": bool(first.get("party_link")),
        "alt_links": list(first.get("alt_links") or []),
        "alt_link_include_main": bool(first.get("alt_link_include_main")),
        "use_full_page_for_table": bool(first.get("use_full_page_for_table")),
        "term_dates_merged": bool(first.get("term_dates_merged")),
        "party_ignore": bool(first.get("party_ignore")),
        "district_ignore": bool(first.get("district_ignore")),
        "district_at_large": bool(first.get("district_at_large")),
    }
    try:
        new_id = db_offices.create_office_for_page(source_page_id, data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return RedirectResponse(f"/offices/{new_id}?saved=0#section-office-{new_id}", status_code=302)


@app.post("/pages/{source_page_id}/delete")
async def page_delete(source_page_id: int):
    """Delete page and all its offices. Redirect to /offices. Confirmation must be done in UI (onsubmit confirm)."""
    db_offices.delete_page(source_page_id)
    return RedirectResponse("/offices", status_code=302)


@app.post("/pages/{source_page_id}")
async def page_update(request: Request, source_page_id: int):
    """Update only the page (URL, location). Used when editing one page with multiple offices."""
    form = await request.form()
    page_data = {
        "url": (form.get("url") or "").strip(),
        "country_id": int(form.get("country_id") or 0),
        "state_id": int(form.get("state_id") or 0) or None,
        "level_id": int(form.get("level_id") or 0) or None,
        "branch_id": int(form.get("branch_id") or 0) or None,
        "notes": (form.get("notes") or "").strip(),
        "enabled": form.get("enabled") == "1",
        "allow_reuse_tables": form.get("allow_reuse_tables") == "1",
    }
    try:
        db_offices.update_page(source_page_id, page_data)
    except ValueError as e:
        from urllib.parse import quote
        offices_on_page = db_offices.list_offices_for_page(source_page_id)
        base = f"/offices/{offices_on_page[0]['id']}" if offices_on_page else "/offices"
        return RedirectResponse(f"{base}?error=" + quote(str(e)), status_code=302)
    first_office_id = db_offices.list_offices_for_page(source_page_id)[0]["id"]
    return RedirectResponse(f"/offices/{first_office_id}?saved=1#section-page", status_code=302)


@app.get("/api/export-config")
async def api_export_config():
    """Return full hierarchy for all pages (each page with offices, alt_links, tables) as JSON download."""
    data = db_offices.get_full_export()
    return Response(
        content=json.dumps(data, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="office-config-export.json"'},
    )


@app.get("/api/pages/{source_page_id}/export-config")
async def api_page_export_config(source_page_id: int):
    """Return full page hierarchy (page, offices with alt_links and tables) for one page as JSON download."""
    data = db_offices.get_page_export(source_page_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Page not found or hierarchy not in use")
    return Response(
        content=json.dumps(data, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="page-{source_page_id}-config.json"'},
    )


@app.get("/offices/{office_id}", response_class=HTMLResponse)
async def office_edit_page(request: Request, office_id: int):
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404)
    office["alt_links"] = db_offices.list_alt_links(office_id)
    offices_on_page = None
    source_page_id = office.get("source_page_id")
    page_data = None
    if source_page_id is not None:
        offices_on_page = db_offices.list_offices_for_page(source_page_id)
        page_data = db_offices.get_page(source_page_id)
        for o in offices_on_page or []:
            o["terms_count"] = db_office_terms.count_terms_for_office(o["id"])
    saved = request.query_params.get("saved") == "1"
    validation_error = request.query_params.get("error") or None
    nav_ids_raw = request.query_params.get("nav_ids") or ""
    nav_ids = [int(x.strip()) for x in nav_ids_raw.split(",") if x.strip().isdigit()]
    nav_prev_id = None
    nav_next_id = None
    nav_current = None
    nav_total = None
    if nav_ids and office_id in nav_ids:
        idx = nav_ids.index(office_id)
        nav_current = idx + 1
        nav_total = len(nav_ids)
        if idx > 0:
            nav_prev_id = nav_ids[idx - 1]
        if idx < len(nav_ids) - 1:
            nav_next_id = nav_ids[idx + 1]
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    country_id_for_states = (page_data or office).get("country_id") or office.get("country_id") or 0
    states = db_refs.list_states(country_id_for_states) if country_id_for_states else []
    terms_count = db_office_terms.count_terms_for_office(office_id)
    return templates.TemplateResponse(
        "page_form.html",
        {"request": request, "office": office, "offices_on_page": offices_on_page, "source_page_id": source_page_id, "page_data": page_data, "countries": countries, "levels": levels, "branches": branches, "states": states, "nav_ids": nav_ids_raw, "nav_prev_id": nav_prev_id, "nav_next_id": nav_next_id, "nav_current": nav_current, "nav_total": nav_total, "terms_count": terms_count, "saved": saved, "validation_error": validation_error, "form_template": "page_form"},
    )


def _form_to_table_config(form, i: int) -> dict:
    """Build one table config dict from form using index i for tc_* getlist."""
    def _get(key_flat: str, key_tc: str):
        lst = form.getlist(key_tc)
        if lst and i < len(lst) and lst[i] is not None and str(lst[i]).strip() != "":
            return lst[i]
        return form.get(key_flat) if i == 0 else None
    def _int(key_flat: str, key_tc: str, default: int) -> int:
        v = _get(key_flat, key_tc)
        if v is None or v == "":
            return default
        try:
            return int(v)
        except (TypeError, ValueError):
            return default
    def _bool(key_flat: str, key_tc: str) -> bool:
        v = _get(key_flat, key_tc)
        return v in (True, "1", 1, "true", "TRUE")
    tc_id = _get("tc_id", "tc_id")
    if tc_id is not None and str(tc_id).strip() != "":
        try:
            tc_id = int(tc_id)
        except (TypeError, ValueError):
            tc_id = None
    else:
        tc_id = None
    date_src = _get("date_source", "tc_date_source") or ""
    dist_mode = _get("district_mode", "tc_district_mode") or "column"
    return {
        "id": tc_id,
        "table_no": _int("table_no", "tc_table_no", 1),
        "table_rows": _int("table_rows", "tc_table_rows", 4),
        "link_column": _int("link_column", "tc_link_column", 1),
        "party_column": _int("party_column", "tc_party_column", 0),
        "term_start_column": _int("term_start_column", "tc_term_start_column", 4),
        "term_end_column": _int("term_end_column", "tc_term_end_column", 5),
        "district_column": _int("district_column", "tc_district_column", 0),
        "dynamic_parse": _bool("dynamic_parse", "tc_dynamic_parse"),
        "read_right_to_left": _bool("read_right_to_left", "tc_read_right_to_left"),
        "find_date_in_infobox": date_src == "find_date_in_infobox",
        "years_only": date_src == "years_only",
        "parse_rowspan": _bool("parse_rowspan", "tc_parse_rowspan"),
        "consolidate_rowspan_terms": _bool("consolidate_rowspan_terms", "tc_consolidate_rowspan_terms"),
        "rep_link": _bool("rep_link", "tc_rep_link"),
        "party_link": _bool("party_link", "tc_party_link"),
        "enabled": _bool("enabled", "tc_enabled") if _get("tc_enabled", "tc_enabled") is not None else True,
        "use_full_page_for_table": _bool("use_full_page_for_table", "tc_use_full_page_for_table"),
        "term_dates_merged": _bool("term_dates_merged", "tc_term_dates_merged"),
        "party_ignore": _bool("party_ignore", "tc_party_ignore"),
        "district_ignore": dist_mode == "no_district",
        "district_at_large": dist_mode == "at_large",
        "notes": _get("notes", "tc_notes") or "",
        "name": _get("name", "tc_name") or "",
    }


@app.post("/offices/{office_id}")
async def office_update(request: Request, office_id: int):
    form = await request.form()
    action = form.get("action", "save_and_close")
    office_only = form.get("office_only") == "1"
    nav_ids = (form.get("nav_ids") or "").strip()
    alt_links = [v.strip() for v in form.getlist("alt_links") if v and isinstance(v, str) and v.strip()]
    alt_link_include_main = form.get("alt_link_include_main") == "1"
    data = {
        "country_id": int(form.get("country_id") or 0), "state_id": int(form.get("state_id") or 0) or None, "level_id": int(form.get("level_id") or 0) or None, "branch_id": int(form.get("branch_id") or 0) or None,
        "department": (form.get("department") or "").strip(), "name": (form.get("name") or "").strip(), "enabled": form.get("enabled") == "1", "notes": (form.get("notes") or "").strip(), "url": (form.get("url") or "").strip(),
        "table_no": int(form.get("table_no") or 1), "table_rows": int(form.get("table_rows") or 4),
        "link_column": int(form.get("link_column") or 1), "party_column": int(form.get("party_column") or 0),
        "term_start_column": int(form.get("term_start_column") or 4), "term_end_column": int(form.get("term_end_column") or 5),
        "district_column": int(form.get("district_column") or 0),
        "dynamic_parse": form.get("dynamic_parse") == "1",
        "read_right_to_left": form.get("read_right_to_left") == "1",
        "find_date_in_infobox": form.get("date_source") == "find_date_in_infobox",
        "years_only": form.get("date_source") == "years_only",
        "parse_rowspan": form.get("parse_rowspan") == "1",
        "consolidate_rowspan_terms": form.get("consolidate_rowspan_terms") == "1",
        "rep_link": form.get("rep_link") == "1",
        "party_link": form.get("party_link") == "1",
        "alt_links": alt_links,
        "alt_link_include_main": alt_link_include_main,
        "use_full_page_for_table": form.get("use_full_page_for_table") == "1",
        "term_dates_merged": form.get("term_dates_merged") == "1",
        "party_ignore": form.get("party_ignore") == "1",
        "district_ignore": (form.get("district_mode") or "column") == "no_district",
        "district_at_large": (form.get("district_mode") or "column") == "at_large",
    }
    tc_ids = form.getlist("tc_id")
    tc_table_nos = form.getlist("tc_table_no")
    if tc_table_nos or tc_ids:
        n = max(len(tc_ids), len(tc_table_nos), 1)
        data["table_configs"] = [_form_to_table_config(form, i) for i in range(n)]
    try:
        db_offices.update_office(office_id, data, office_only=office_only)
    except ValueError as e:
        from urllib.parse import quote
        q = "?error=" + quote(str(e)) + ("&nav_ids=" + nav_ids.strip() if nav_ids and nav_ids.strip() else "")
        return RedirectResponse(f"/offices/{office_id}{q}", status_code=302)
    if action == "save":
        q = "?saved=1" + ("&nav_ids=" + nav_ids.strip() if nav_ids and nav_ids.strip() else "")
        hash_frag = "#section-office-" + str(office_id) if office_only else ""
        return RedirectResponse(f"/offices/{office_id}{q}{hash_frag}", status_code=302)
    return RedirectResponse("/offices?saved=1", status_code=302)


@app.post("/offices/{office_id}/delete")
async def office_delete(office_id: int):
    db_offices.delete_office(office_id)
    return RedirectResponse("/offices", status_code=302)


@app.post("/offices/{office_id}/table/{tc_id}/delete")
async def table_delete(office_id: int, tc_id: int):
    """Delete one table config. Redirect back to office edit. Confirmation must be done in UI."""
    try:
        db_offices.delete_table(tc_id)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse(f"/offices/{office_id}?error=" + quote(str(e)), status_code=302)
    return RedirectResponse(f"/offices/{office_id}?saved=1", status_code=302)


@app.post("/offices/{office_id}/table/{tc_id}/move")
async def table_move(
    office_id: int,
    tc_id: int,
    to_office_id: int = Form(...),
    delete_source_office_if_empty: str = Form(""),
):
    """Move a table config to another office on the same page. Returns 409 with requires_confirm if source would be empty; client may resubmit with delete_source_office_if_empty=1."""
    delete_flag = str(delete_source_office_if_empty).strip().lower() in ("1", "true", "yes")
    try:
        db_offices.move_table(tc_id, to_office_id, delete_source_office_if_empty=delete_flag)
    except ValueError as e:
        msg = str(e)
        if msg.startswith("OFFICE_WOULD_BE_EMPTY:"):
            source_name = msg.split(":", 1)[-1].strip() or "Office"
            return JSONResponse(
                {"requires_confirm": True, "source_office_name": source_name},
                status_code=409,
            )
        return JSONResponse({"error": msg}, status_code=400)
    redirect_url = f"/offices/{to_office_id}?saved=1"
    return JSONResponse({"redirect": redirect_url})


@app.post("/offices/{office_id}/duplicate")
async def office_duplicate(office_id: int):
    """Create a copy of the office (same config, new name) and redirect to the new office's edit page."""
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404)
    copy_name = (office.get("name") or "Office").strip()
    if not copy_name.lower().startswith("copy of"):
        copy_name = "Copy of " + copy_name
    data = {
        "country_id": office.get("country_id") or 0,
        "state_id": office.get("state_id"),
        "level_id": office.get("level_id"),
        "branch_id": office.get("branch_id"),
        "department": office.get("department") or "",
        "name": copy_name,
        "enabled": False,
        "notes": office.get("notes") or "",
        "url": office.get("url") or "",
        "table_no": int(office.get("table_no") or 1),
        "table_rows": int(office.get("table_rows") or 4),
        "link_column": int(office.get("link_column") or 1),
        "party_column": int(office.get("party_column") or 0),
        "term_start_column": int(office.get("term_start_column") or 4),
        "term_end_column": int(office.get("term_end_column") or 5),
        "district_column": int(office.get("district_column") or 0),
        "dynamic_parse": bool(office.get("dynamic_parse")),
        "read_right_to_left": bool(office.get("read_right_to_left")),
        "find_date_in_infobox": bool(office.get("find_date_in_infobox")),
        "years_only": bool(office.get("years_only")),
        "parse_rowspan": bool(office.get("parse_rowspan")),
        "consolidate_rowspan_terms": bool(office.get("consolidate_rowspan_terms")),
        "rep_link": bool(office.get("rep_link")),
        "party_link": bool(office.get("party_link")),
        "alt_links": db_offices.list_alt_links(office_id),
        "alt_link_include_main": bool(office.get("alt_link_include_main")),
        "use_full_page_for_table": bool(office.get("use_full_page_for_table")),
        "term_dates_merged": bool(office.get("term_dates_merged")),
        "party_ignore": bool(office.get("party_ignore")),
        "district_ignore": bool(office.get("district_ignore")),
        "district_at_large": bool(office.get("district_at_large")),
    }
    table_configs = office.get("table_configs")
    if table_configs:
        data["table_configs"] = [{k: v for k, v in tc.items() if k != "id"} for tc in table_configs]
    try:
        new_id = db_offices.create_office(data)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse("/offices/" + str(office_id) + "?error=" + quote(str(e)), status_code=302)
    return RedirectResponse(f"/offices/{new_id}?saved=1", status_code=302)


@app.post("/api/offices/{office_id}/enabled")
async def api_office_enabled(office_id: int, enabled: int = Form(1)):
    db_offices.set_office_enabled(office_id, enabled == 1)
    return JSONResponse({"ok": True})


@app.post("/api/offices/enabled-all")
async def api_offices_enabled_all(enabled: int = Form(1)):
    db_offices.set_all_offices_enabled(enabled == 1)
    return JSONResponse({"ok": True, "enabled": enabled})


@app.get("/api/offices/{office_id}/test-config")
async def api_office_test_config(office_id: int):
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404, detail="Office not found")
    ok, message = test_office_config(office)
    return JSONResponse({"ok": ok, "message": message})


REVALIDATE_MSG_MISSING_HOLDERS = "New list is missing office holders that were in existing data. Kept existing terms."


def _populate_job_worker(job_id: str, office_id: int, force_override: bool = False):
    def progress_callback(phase: str, current: int, total: int, message: str, extra: dict):
        with _populate_job_lock:
            if job_id in _populate_job_store:
                _populate_job_store[job_id].update({
                    "phase": phase,
                    "current": current,
                    "total": total,
                    "message": message,
                    "extra": extra,
                })

    def cancel_check() -> bool:
        with _populate_job_lock:
            return _populate_job_store.get(job_id, {}).get("cancelled", False)

    office = db_offices.get_office(office_id)
    if not office:
        with _populate_job_lock:
            if job_id in _populate_job_store:
                _populate_job_store[job_id]["status"] = "error"
                _populate_job_store[job_id]["error"] = "Office not found"
        return
    unit_ids = db_offices.get_runnable_unit_ids_for_office(office_id) or [office_id]
    existing = db_office_terms.get_existing_terms_for_office(unit_ids[0])
    reprocessed = len(existing) > 0
    try:
        result = run_with_db(
            run_mode="delta",
            run_bio=False,
            dry_run=False,
            test_run=False,
            office_ids=unit_ids,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
            force_replace_office_ids=unit_ids if force_override else None,
        )
        terms_parsed = result.get("terms_parsed") or 0
        with _populate_job_lock:
            if job_id not in _populate_job_store:
                return
            if result.get("cancelled"):
                _populate_job_store[job_id]["status"] = "cancelled"
                _populate_job_store[job_id]["result"] = {
                    "ok": False,
                    "message": "Stopped after %s terms." % terms_parsed,
                    "terms_parsed": terms_parsed,
                    "cancelled": True,
                }
                return
        err = result.get("message") or (result.get("error") if not result.get("office_count") else None)
        revalidate_failed = result.get("revalidate_failed") and terms_parsed == 0
        revalidate_msg = result.get("revalidate_message")
        can_force_override = revalidate_msg == REVALIDATE_MSG_MISSING_HOLDERS
        revalidate_missing_holders = result.get("revalidate_missing_holders")  # full list for "View full list" in new window
        with _populate_job_lock:
            if job_id in _populate_job_store:
                _populate_job_store[job_id]["status"] = "complete"
                if revalidate_failed and revalidate_msg:
                    res = {"ok": False, "message": revalidate_msg, "can_force_override": can_force_override}
                    if revalidate_missing_holders:
                        res["revalidate_missing_holders"] = revalidate_missing_holders
                    _populate_job_store[job_id]["result"] = res
                elif err and terms_parsed == 0:
                    _populate_job_store[job_id]["result"] = {"ok": False, "message": err}
                else:
                    msg = "Terms reprocessed (%s terms)." % terms_parsed if reprocessed else "Terms populated (%s terms)." % terms_parsed
                    _populate_job_store[job_id]["result"] = {"ok": True, "message": msg, "terms_parsed": terms_parsed, "reprocessed": reprocessed}
    except Exception as e:
        with _populate_job_lock:
            if job_id in _populate_job_store:
                _populate_job_store[job_id]["status"] = "error"
                _populate_job_store[job_id]["error"] = str(e)


@app.post("/api/offices/{office_id}/populate-terms")
async def api_office_populate_terms(office_id: int, request: Request):
    """Start populate-terms job. Returns 202 with job_id; poll status endpoint for progress.
    Optional JSON body: {\"force_override\": true} to replace even when new list is missing existing holders."""
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404, detail="Office not found")
    force_override = False
    if request.headers.get("content-type", "").strip().startswith("application/json"):
        try:
            body = await request.json()
            force_override = body.get("force_override") in (True, 1, "true", "1")
        except Exception:
            pass
    job_id = str(uuid.uuid4())
    with _populate_job_lock:
        _populate_job_store[job_id] = {
            "status": "running",
            "phase": "init",
            "current": 0,
            "total": 1,
            "message": "Starting…",
            "extra": {},
            "result": None,
            "error": None,
            "office_id": office_id,
            "cancelled": False,
        }
    thread = threading.Thread(target=_populate_job_worker, args=(job_id, office_id, force_override))
    thread.start()
    return JSONResponse({"job_id": job_id}, status_code=202)


@app.get("/api/offices/{office_id}/populate-terms/status/{job_id}")
async def api_office_populate_terms_status(office_id: int, job_id: str):
    """Return populate-terms job status. Used for polling."""
    with _populate_job_lock:
        if job_id not in _populate_job_store:
            raise HTTPException(status_code=404, detail="Job not found")
        job = _populate_job_store[job_id]
        if job.get("office_id") != office_id:
            raise HTTPException(status_code=404, detail="Job not found")
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


@app.post("/api/offices/{office_id}/populate-terms/cancel/{job_id}")
async def api_office_populate_terms_cancel(office_id: int, job_id: str):
    """Request cancellation of a populate-terms job."""
    with _populate_job_lock:
        if job_id not in _populate_job_store:
            raise HTTPException(status_code=404, detail="Job not found")
        job = _populate_job_store[job_id]
        if job.get("office_id") != office_id:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.get("status") != "running":
            return JSONResponse({"ok": False, "message": "Job is not running"}, status_code=409)
        job["cancelled"] = True
    return JSONResponse({"ok": True})


@app.post("/api/offices/test-config")
async def api_office_test_config_draft(request: Request):
    """Test config using draft JSON (unsaved form). Body: url, table_no, table_rows, link_column, party_column, term_start_column, term_end_column, district_column, and optional booleans."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    draft = _office_draft_from_body(body, include_ref_names=False)
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
    ok, message = test_office_config(draft)
    return JSONResponse({"ok": ok, "message": message})


# ---------- Parties ----------
@app.get("/parties", response_class=HTMLResponse)
async def parties_list(request: Request):
    parties = db_parties.list_parties()
    saved = request.query_params.get("saved") == "1"
    imported_count = request.query_params.get("count")
    imported_errors = request.query_params.get("errors")
    imported = request.query_params.get("imported") == "1"
    return templates.TemplateResponse(
        "parties.html",
        {"request": request, "parties": parties, "saved": saved, "imported": imported, "imported_count": imported_count, "imported_errors": imported_errors},
    )


@app.get("/parties/import", response_class=HTMLResponse)
async def parties_import_page(request: Request):
    return templates.TemplateResponse("import_parties.html", {"request": request})


@app.post("/parties/import")
async def parties_import(
    request: Request,
    mode: str = Form("append"),
    csv_file: UploadFile = File(None),
):
    if not csv_file or not csv_file.filename:
        return templates.TemplateResponse(
            "import_parties.html",
            {"request": request, "error": "Please choose a CSV file to upload.", "mode": mode},
        )
    if not csv_file.filename.lower().endswith(".csv"):
        return templates.TemplateResponse(
            "import_parties.html",
            {"request": request, "error": "File must be a .csv file.", "mode": mode},
        )
    try:
        content = await csv_file.read()
    except Exception as e:
        return templates.TemplateResponse(
            "import_parties.html",
            {"request": request, "error": f"Could not read file: {e}", "mode": mode},
        )
    try:
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        try:
            overwrite = mode == "overwrite"
            imported, errors = bulk_import_parties_from_csv(tmp_path, overwrite=overwrite)
            return RedirectResponse(
                f"/parties?imported=1&count={imported}&errors={errors}", status_code=302
            )
        finally:
            tmp_path.unlink(missing_ok=True)
    except Exception as e:
        return templates.TemplateResponse(
            "import_parties.html",
            {"request": request, "error": str(e), "mode": mode},
        )


@app.get("/parties/new", response_class=HTMLResponse)
async def party_new(request: Request):
    countries = db_refs.list_countries()
    return templates.TemplateResponse("party_form.html", {"request": request, "party": None, "countries": countries})


@app.post("/parties/new")
async def party_create(
    country_id: int = Form(0),
    party_name: str = Form(""),
    party_link: str = Form(""),
):
    db_parties.create_party({"country_id": country_id, "party_name": party_name, "party_link": party_link})
    return RedirectResponse("/parties?saved=1", status_code=302)


@app.get("/parties/{party_id}", response_class=HTMLResponse)
async def party_edit_page(request: Request, party_id: int):
    party = db_parties.get_party(party_id)
    if not party:
        raise HTTPException(status_code=404)
    countries = db_refs.list_countries()
    return templates.TemplateResponse("party_form.html", {"request": request, "party": party, "countries": countries})


@app.post("/parties/{party_id}")
async def party_update(
    party_id: int,
    country_id: int = Form(0),
    party_name: str = Form(""),
    party_link: str = Form(""),
):
    db_parties.update_party(party_id, {"country_id": country_id, "party_name": party_name, "party_link": party_link})
    return RedirectResponse("/parties?saved=1", status_code=302)


@app.post("/parties/{party_id}/delete")
async def party_delete(party_id: int):
    db_parties.delete_party(party_id)
    return RedirectResponse("/parties", status_code=302)


# ---------- Reference data (for dropdowns) ----------
@app.get("/api/countries")
async def api_countries():
    return JSONResponse(db_refs.list_countries())


@app.get("/api/states")
async def api_states(country_id: int = Query(0)):
    if not country_id:
        return JSONResponse([])
    return JSONResponse(db_refs.list_states(country_id))


@app.get("/api/levels")
async def api_levels():
    return JSONResponse(db_refs.list_levels())


@app.get("/api/branches")
async def api_branches():
    return JSONResponse(db_refs.list_branches())


# ---------- Run scraper ----------
@app.get("/run", response_class=HTMLResponse)
async def run_page(request: Request):
    offices = db_offices.list_offices()
    return templates.TemplateResponse("run.html", {"request": request, "offices": offices})


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
):
    def progress_callback(phase: str, current: int, total: int, message: str, extra: dict):
        with _run_job_lock:
            if job_id in _run_job_store:
                _run_job_store[job_id].update({
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
            progress_callback=progress_callback,
            cancel_check=cancel_check,
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


@app.post("/api/run")
async def api_run(run_mode: str = Form("delta"), individual_ref: str = Form("")):
    if run_mode == "single_bio" and not individual_ref.strip():
        raise HTTPException(status_code=400, detail="Individual (ID or Wikipedia URL) required for re-run bio.")
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
    job_id = str(uuid.uuid4())
    with _run_job_lock:
        _run_job_store[job_id] = {
            "status": "running",
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
        args=(job_id, mode, run_bio, run_office_bio, refresh_table_cache, False, False, None, None, individual_ref.strip() or None),
    )
    thread.start()
    return JSONResponse({"job_id": job_id}, status_code=202)


@app.get("/api/run/status/{job_id}")
async def api_run_status(job_id: str):
    with _run_job_lock:
        if job_id not in _run_job_store:
            raise HTTPException(status_code=404, detail="Job not found")
        return _run_job_store[job_id]


@app.post("/api/run/cancel/{job_id}")
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


@app.post("/api/refresh-table-cache")
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


# ---------- Data views ----------
@app.get("/data/individuals", response_class=HTMLResponse)
async def data_individuals(request: Request, limit: int = Query(100, le=500), offset: int = Query(0)):
    individuals = db_individuals.list_individuals(limit=limit, offset=offset)
    return templates.TemplateResponse("individuals.html", {"request": request, "individuals": individuals})


@app.get("/data/office-terms", response_class=HTMLResponse)
async def data_office_terms(request: Request, limit: int = Query(100, le=500), offset: int = Query(0), office_id: int = Query(None)):
    terms = db_office_terms.list_office_terms(limit=limit, offset=offset, office_id=office_id)
    offices = db_offices.list_offices()
    return templates.TemplateResponse("office_terms.html", {"request": request, "terms": terms, "offices": offices})


@app.get("/report/milestones", response_class=HTMLResponse)
async def report_milestones(request: Request):
    recent_deaths = db_reports.get_recent_deaths()
    recent_term_ends = db_reports.get_recent_term_ends()
    recent_term_starts = db_reports.get_recent_term_starts()
    return templates.TemplateResponse(
        "milestone_report.html",
        {
            "request": request,
            "recent_deaths": recent_deaths,
            "recent_term_ends": recent_term_ends,
            "recent_term_starts": recent_term_starts,
        },
    )


# ---------- Preview (single office) ----------
@app.get("/offices/{office_id}/preview", response_class=HTMLResponse)
async def office_preview_page(request: Request, office_id: int):
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404)
    # #region agent log
    import json
    from pathlib import Path
    _log_path = Path(__file__).resolve().parent.parent / ".cursor" / "debug.log"
    with open(_log_path, "a", encoding="utf-8") as _f:
        _f.write(json.dumps({"location": "main.py:office_preview_page", "message": "preview page using run_with_db", "data": {"office_id": office_id, "url": (office.get("url") or "")[:80], "table_no": office.get("table_no"), "hypothesisId": "H3"}, "timestamp": __import__("time").time() * 1000}) + "\n")
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
        "preview.html",
        {"request": request, "office": office, "rows": rows, "error": result.get("error"), "raw_table_preview": raw_table_preview},
    )


@app.get("/api/preview/{office_id}")
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


def _preview_job_worker(job_id: str, draft: dict, max_rows: int | None):
    def cancel_check() -> bool:
        with _preview_job_lock:
            return _preview_job_store.get(job_id, {}).get("cancelled", False)

    def progress_callback(phase: str, current: int, total: int, message: str, extra: dict):
        if cancel_check():
            raise PreviewCancelled("Stopped.")
        with _preview_job_lock:
            if job_id in _preview_job_store:
                _preview_job_store[job_id].update({
                    "phase": phase,
                    "current": current,
                    "total": total,
                    "message": message,
                    "extra": extra,
                })
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


@app.post("/api/preview")
async def api_preview_draft(request: Request):
    """Preview using draft office config (unsaved form). Body: same fields as office form (country_id, url, table_no, etc.).
    Optional: max_rows (default 10); use max_rows=0 or show_all=true to return all rows.
    When find_date_in_infobox is set, returns 202 with job_id; poll GET /api/preview/status/{job_id} for progress (Processing x of y)."""
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


@app.get("/api/preview/status/{job_id}")
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


@app.post("/api/preview/cancel/{job_id}")
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


@app.post("/api/preview-all-tables")
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
            _f.write(__import__("json").dumps({"location": "main.py:api_preview_all_tables", "message": "request", "data": {"url_len": len(url), "url_preview": url[:80] if url else "", "has_confirm": body.get("confirm") is True}, "timestamp": __import__("time").time() * 1000}) + "\n")
    except Exception:
        pass
    # #endregion
    if not url:
        raise HTTPException(status_code=400, detail="url required")
    confirmed = body.get("confirm") is True
    result = get_all_tables_preview(url, max_rows_per_table=10, confirm_threshold=10, confirmed=confirmed)
    return JSONResponse(result)


@app.post("/api/raw-table-preview")
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


@app.post("/api/table-html")
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


def _export_job_worker(job_id: str, office_name: str, config: dict):
    """Background worker for debug export when find_date_in_infobox: fetch table, parse with progress, write file."""
    def progress_callback(phase: str, current: int, total: int, message: str, extra: dict):
        with _export_job_lock:
            if job_id in _export_job_store:
                _export_job_store[job_id].update({
                    "phase": phase, "current": current, "total": total, "message": message, "extra": extra or {},
                })
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
            "dynamic_parse": _config_bool_export(config.get("dynamic_parse")),
            "read_right_to_left": _config_bool_export(config.get("read_right_to_left")),
            "find_date_in_infobox": _config_bool_export(config.get("find_date_in_infobox")),
            "years_only": _config_bool_export(config.get("years_only")),
            "parse_rowspan": _config_bool_export(config.get("parse_rowspan")),
            "consolidate_rowspan_terms": _config_bool_export(config.get("consolidate_rowspan_terms")),
            "rep_link": _config_bool_export(config.get("rep_link")),
            "party_link": _config_bool_export(config.get("party_link")),
            "alt_links": config.get("alt_links") if isinstance(config.get("alt_links"), list) else [],
            "alt_link_include_main": _config_bool_export(config.get("alt_link_include_main")),
            "use_full_page_for_table": use_full_page,
            "term_dates_merged": _config_bool_export(config.get("term_dates_merged")),
            "party_ignore": _config_bool_export(config.get("party_ignore")),
            "district_ignore": _config_bool_export(config.get("district_ignore")),
            "district_at_large": _config_bool_export(config.get("district_at_large")),
            "country_name": "", "level_name": "", "branch_name": "", "state_name": "",
        }
        full_rows = parse_full_table_for_export(office_row, table_html, url, progress_callback=progress_callback)
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
            "How to verify: From project root run: python scripts/test_debug_export.py debug/" + filename,
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
            h_start = header_cells[term_start_0][1] if 0 <= term_start_0 < len(header_cells) else "?"
            h_end = header_cells[term_end_0][1] if 0 <= term_end_0 < len(header_cells) else "?"
            mapping_parts.append('Header at term_start: %r, at term_end: %r.' % (h_start, h_end))
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
            headers = ["Wiki Link", "Party", "District", "Term Start", "Term End", "Term Start Year", "Term End Year", "Infobox items"]
            lines.append("\t".join(headers))
            for row in full_rows:
                cells = [str(row.get(h) or "").replace("\t", " ").replace("\n", " ") for h in headers]
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
                _export_job_store[job_id]["result"] = {"path": f"debug/{filename}", "filename": filename}
    except Exception as e:
        with _export_job_lock:
            if job_id in _export_job_store:
                _export_job_store[job_id]["status"] = "error"
                _export_job_store[job_id]["error"] = str(e)


@app.post("/api/office-debug-export")
async def api_office_debug_export(request: Request):
    """Write a debug text file with config, preview result, and table HTML. Body: office_name, config, preview_result, table_html_result.
    When find_date_in_infobox is true, returns 202 with job_id; poll GET /api/office-debug-export-status/{job_id} for progress."""
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
        table_html = (table_html_result.get("html") or "") if not table_html_result.get("error") else ""
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
            "How to verify: From project root run: python scripts/test_debug_export.py debug/" + filename,
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
            h_start = header_cells[term_start_0][1] if 0 <= term_start_0 < len(header_cells) else "?"
            h_end = header_cells[term_end_0][1] if 0 <= term_end_0 < len(header_cells) else "?"
            mapping_parts.append('Header at term_start: %r, at term_end: %r.' % (h_start, h_end))
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
            headers = ["Wiki Link", "Party", "District", "Term Start", "Term End", "Term Start Year", "Term End Year", "Infobox items"]
            lines.append("\t".join(headers))
            for row in preview_rows:
                cells = [str(row.get(h) or "").replace("\t", " ").replace("\n", " ") for h in headers]
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
        "How to verify: From project root run: python scripts/test_debug_export.py debug/" + filename,
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
        mapping_parts.append('Header at term_start: %r, at term_end: %r.' % (h_start, h_end))
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
                "dynamic_parse": _config_bool(config.get("dynamic_parse")),
                "read_right_to_left": _config_bool(config.get("read_right_to_left")),
                "find_date_in_infobox": _config_bool(config.get("find_date_in_infobox")),
                "years_only": _config_bool(config.get("years_only")),
                "parse_rowspan": _config_bool(config.get("parse_rowspan")),
                "consolidate_rowspan_terms": _config_bool(config.get("consolidate_rowspan_terms")),
                "rep_link": _config_bool(config.get("rep_link")),
                "party_link": _config_bool(config.get("party_link")),
                "alt_links": config.get("alt_links") if isinstance(config.get("alt_links"), list) else [],
                "alt_link_include_main": _config_bool(config.get("alt_link_include_main")),
                "use_full_page_for_table": _config_bool(config.get("use_full_page_for_table")),
                "term_dates_merged": _config_bool(config.get("term_dates_merged")),
                "party_ignore": _config_bool(config.get("party_ignore")),
                "district_ignore": _config_bool(config.get("district_ignore")),
                "district_at_large": _config_bool(config.get("district_at_large")),
                "country_name": "", "level_name": "", "branch_name": "", "state_name": "",
            }
            full_rows = parse_full_table_for_export(office_row, table_html, office_row["url"])
        except Exception as e:
            full_rows = []
            lines.append("Parse error: " + str(e))
    if full_rows:
        headers = ["Wiki Link", "Party", "District", "Term Start", "Term End", "Term Start Year", "Term End Year", "Infobox items"]
        lines.append("\t".join(headers))
        for row in full_rows:
            cells = [str(row.get(h) or "").replace("\t", " ").replace("\n", " ") for h in headers]
            lines.append("\t".join(cells))
    else:
        preview_rows = preview_result.get("preview_rows") or []
        if preview_rows:
            lines.append("(Full parse produced no rows; showing preview sample below.)")
            headers = ["Wiki Link", "Party", "District", "Term Start", "Term End", "Term Start Year", "Term End Year", "Infobox items"]
            lines.append("\t".join(headers))
            for row in preview_rows:
                cells = [str(row.get(h) or "").replace("\t", " ").replace("\n", " ") for h in headers]
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


@app.get("/api/office-debug-export-status/{job_id}")
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


@app.post("/api/preview-offices")
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
            results.append({"office_id": oid, "name": None, "url": None, "preview_rows": [], "raw_table_preview": None, "error": "Invalid office id"})
            continue
        office = db_offices.get_office(oid)
        if not office:
            results.append({"office_id": oid, "name": None, "url": None, "preview_rows": [], "raw_table_preview": None, "error": "Office not found"})
            continue
        name = office.get("name") or ""
        url = (office.get("url") or "").strip()
        pr = preview_with_config(office, max_rows=10)
        results.append({
            "office_id": oid,
            "name": name,
            "url": url,
            "preview_rows": pr.get("preview_rows") or [],
            "raw_table_preview": pr.get("raw_table_preview"),
            "error": pr.get("error"),
        })
    return JSONResponse({"results": results})
