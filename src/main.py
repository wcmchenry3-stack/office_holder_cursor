# -*- coding: utf-8 -*-
"""
Office Holder app: local UI and API for Wikipedia office/bio scraper.
Run: uvicorn src.main:app --reload
From project root: office_holder/
"""

import json
import os
import sqlite3
import re
import tempfile
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
import sys
import threading
import uuid

import requests

# Ensure project root is on path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from markupsafe import Markup
from fastapi import FastAPI, File, Request, Form, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.db.connection import init_db, get_connection
from src.db import offices as db_offices
from src.db import parties as db_parties
from src.db import refs as db_refs
from src.db import office_category as db_office_category
from src.db import infobox_role_key_filter as db_infobox_role_key_filter
from src.db import individuals as db_individuals
from src.db import office_terms as db_office_terms
from src.db import reports as db_reports
from src.db import test_scripts as db_test_scripts
from src.db.bulk_import import bulk_import_offices_from_csv, bulk_import_parties_from_csv
from src.scraper.runner import run_with_db, preview_with_config, parse_full_table_for_export
from src.scraper.config_test import test_office_config, get_raw_table_preview, get_all_tables_preview, get_table_html, get_table_header_from_html
from src.scraper.test_script_runner import run_test_script, run_test_script_from_html
from src.scraper.wiki_fetch import WIKIPEDIA_REQUEST_HEADERS, wiki_url_to_rest_html_url, normalize_wiki_url

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
_test_script_result_store: dict = {}
_test_script_result_lock = threading.Lock()
_ui_test_job_store: dict = {}
_ui_test_job_lock = threading.Lock()

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
        "filter_column": int(body.get("filter_column") or 0),
        "filter_criteria": (body.get("filter_criteria") or "").strip(),
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
        "ignore_non_links": body.get("ignore_non_links") in (True, 1, "1", "true", "TRUE"),
        "remove_duplicates": body.get("remove_duplicates") in (True, 1, "1", "true", "TRUE"),
        "infobox_role_key_filter_id": _validate_infobox_role_key_filter_id(body.get("infobox_role_key_filter_id")),
    }
    draft["infobox_role_key"] = (body.get("infobox_role_key") or "").strip() or _resolve_infobox_role_key_from_filter_id(
        draft.get("infobox_role_key_filter_id")
    )
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


# ---------- Favicon (avoid 404 in console) ----------
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Return 204 so the browser's automatic favicon request doesn't 404."""
    return Response(status_code=204)


# ---------- Office config CRUD ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return RedirectResponse("/offices", status_code=302)


def _list_return_query(
    country_id: int | None = None,
    state_id: int | None = None,
    level_id: int | None = None,
    branch_id: int | None = None,
    office_category_id: int | None = None,
    enabled: str | None = None,
    limit: str | None = None,
    office_count: str | None = None,
) -> str:
    """Build query string for returning to the page list with filters applied (for Cancel link)."""
    parts: list[str] = []
    if country_id:
        parts.append(f"country_id={country_id}")
    if state_id:
        parts.append(f"state_id={state_id}")
    if level_id:
        parts.append(f"level_id={level_id}")
    if branch_id:
        parts.append(f"branch_id={branch_id}")
    if office_category_id:
        parts.append(f"office_category_id={office_category_id}")
    if enabled is not None and str(enabled).strip():
        parts.append("enabled=" + str(enabled).strip())
    if limit is not None and str(limit).strip():
        parts.append("limit=" + str(limit).strip())
    if office_count is not None and str(office_count).strip() and str(office_count).strip() != "all":
        parts.append("office_count=" + str(office_count).strip())
    return "&".join(parts)


def _validate_infobox_role_key_filter_id(filter_id: str | int | None) -> int | None:
    """Normalize and validate optional infobox role-key filter id."""
    if filter_id is None:
        return None
    raw = str(filter_id).strip()
    if not raw:
        return None
    try:
        fid = int(raw)
    except (TypeError, ValueError) as e:
        raise ValueError("Infobox role key filter must be an integer") from e
    if fid <= 0:
        return None
    if not db_infobox_role_key_filter.get_infobox_role_key_filter(fid):
        raise ValueError(f"Infobox role key filter {fid} was not found")
    return fid


def _resolve_infobox_role_key_from_filter_id(filter_id: str | int | None) -> str:
    """Resolve filter id to role_key text; return empty string when missing/unset."""
    try:
        fid = _validate_infobox_role_key_filter_id(filter_id)
    except ValueError:
        return ""
    if not fid:
        return ""
    f = db_infobox_role_key_filter.get_infobox_role_key_filter(fid)
    if not f:
        return ""
    return (f.get("role_key") or "").strip()


def _parse_optional_int(value: str | None) -> int | None:
    """Parse query param to int; treat None or empty string as None."""
    if value is None or not str(value).strip():
        return None
    try:
        n = int(str(value).strip())
        return n if n != 0 else None
    except ValueError:
        return None


@app.get("/offices", response_class=HTMLResponse)
async def offices_list(
    request: Request,
    country_id: str | None = Query(None),
    state_id: str | None = Query(None),
    level_id: str | None = Query(None),
    branch_id: str | None = Query(None),
    office_category_id: str | None = Query(None),
    enabled: str | None = Query(None),
    limit: str | None = Query(None),
    office_count: str | None = Query("all"),
):
    saved = request.query_params.get("saved") == "1"
    page_saved = request.query_params.get("page_saved") == "1"
    validation_error = request.query_params.get("error") or None
    imported_count = request.query_params.get("count")
    imported_errors = request.query_params.get("errors")
    imported = request.query_params.get("imported") == "1"

    if db_offices.use_hierarchy():
        # Parse limit: "20", "50", "100", "all" or missing -> int or None
        limit_int: int | None = None
        if limit and limit.strip().lower() != "all":
            try:
                limit_int = int(limit.strip())
            except ValueError:
                pass
        enabled_int: int | None = None
        if enabled is not None and enabled.strip() in ("0", "1"):
            enabled_int = int(enabled.strip())
        cid = _parse_optional_int(country_id)
        sid = _parse_optional_int(state_id)
        lid = _parse_optional_int(level_id)
        bid = _parse_optional_int(branch_id)
        ocid = _parse_optional_int(office_category_id)
        office_count_val = (office_count or "all").strip().lower()
        if office_count_val not in ("all", "gt0", "eq0"):
            office_count_val = "all"
        pages = db_offices.list_pages(
            country_id=cid,
            state_id=sid,
            level_id=lid,
            branch_id=bid,
            office_category_id=ocid,
            enabled=enabled_int,
            limit=limit_int,
            office_count_filter=office_count_val,
        )
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        office_categories = db_office_category.list_office_categories()
        filter_country_id = cid
        states = db_refs.list_states(filter_country_id) if filter_country_id else []
        nav_ids = ",".join(str(p["first_office_id"]) for p in pages if p.get("first_office_id"))
        list_return_query = _list_return_query(
            country_id=cid, state_id=sid, level_id=lid, branch_id=bid,
            office_category_id=ocid,
            enabled=enabled.strip() if enabled else None,
            limit=limit.strip() if limit else None,
            office_count=office_count_val if office_count_val != "all" else None,
        )
        return templates.TemplateResponse(
            "offices.html",
            {
                "request": request,
                "page_search_view": True,
                "pages": pages,
                "nav_ids": nav_ids,
                "list_return_query": list_return_query,
                "offices": [],
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "office_categories": office_categories,
                "states": states,
                "filter_country_id": filter_country_id,
                "filter_state_id": sid,
                "filter_level_id": lid,
                "filter_branch_id": bid,
                "filter_office_category_id": ocid,
                "filter_enabled": enabled.strip() if enabled else "",
                "filter_limit": limit.strip() if limit else "20",
                "filter_office_count": office_count_val,
                "saved": saved,
                "validation_error": validation_error,
                "imported": imported,
                "imported_count": imported_count,
                "imported_errors": imported_errors,
            },
        )

    offices = db_offices.list_offices()
    counts = db_office_terms.get_terms_counts_by_office()
    for o in offices:
        o["terms_count"] = counts.get(o["id"], 0)
    return templates.TemplateResponse(
        "offices.html",
        {"request": request, "page_search_view": False, "offices": offices, "pages": [], "saved": saved, "validation_error": validation_error, "imported": imported, "imported_count": imported_count, "imported_errors": imported_errors},
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
        "country_id": int(form.get("country_id") or 0), "state_id": int(form.get("state_id") or 0) or None, "city_id": int(form.get("city_id") or 0) or None, "level_id": int(form.get("level_id") or 0) or None, "branch_id": int(form.get("branch_id") or 0) or None,
        "department": (form.get("department") or "").strip(), "name": (form.get("name") or "").strip(), "enabled": form.get("enabled") == "1", "notes": (form.get("notes") or "").strip(), "url": (form.get("url") or "").strip(),
        "table_no": int(form.get("table_no") or 1), "table_rows": int(form.get("table_rows") or 4),
        "link_column": int(form.get("link_column") or 1), "party_column": int(form.get("party_column") or 0),
        "term_start_column": int(form.get("term_start_column") or 4), "term_end_column": int(form.get("term_end_column") or 5),
        "district_column": int(form.get("district_column") or 0),
        "filter_column": int(form.get("filter_column") or 0),
        "filter_criteria": (form.get("filter_criteria") or "").strip(),
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
        "ignore_non_links": form.get("ignore_non_links") == "1",
        "remove_duplicates": form.get("remove_duplicates") == "1",
        "infobox_role_key": (form.get("infobox_role_key") or "").strip(),
    }
    try:
        _validate_level_state_city(data.get("level_id"), data.get("state_id"), data.get("city_id"), data.get("branch_id"))
    except ValueError as e:
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        states = db_refs.list_states(int(data.get("country_id") or 0)) if data.get("country_id") else []
        cities = db_refs.list_cities(data.get("state_id")) if data.get("state_id") else []
        return templates.TemplateResponse(
            "page_form.html",
            {
                "request": request,
                "office": {**data, "alt_links": alt_links},
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "states": states,
                "cities": cities,
                "nav_ids": "",
                "nav_prev_id": None,
                "nav_next_id": None,
                "terms_count": 0,
                "form_template": "page_form",
                "validation_error": str(e),
            },
        )
    url = (data.get("url") or "").strip()
    if url:
        existing_page_id = db_offices.get_source_page_id_by_url(url)
        if existing_page_id is not None:
            offices_on_page = db_offices.list_offices_for_page(existing_page_id)
            first_office_id = offices_on_page[0]["id"] if offices_on_page else None
            countries = db_refs.list_countries()
            levels = db_refs.list_levels()
            branches = db_refs.list_branches()
            states = db_refs.list_states(int(data.get("country_id") or 0)) if data.get("country_id") else []
            cities = db_refs.list_cities(data.get("state_id")) if data.get("state_id") else []
            edit_link = Markup(f'<a href="/offices/{first_office_id}">Edit the existing page</a>') if first_office_id else Markup.escape("Edit the existing page from the office list.")
            validation_error = Markup("A page with this URL already exists. ") + edit_link + Markup(" instead.")
            return templates.TemplateResponse(
                "page_form.html",
                {
                    "request": request,
                    "office": {**data, "alt_links": alt_links},
                    "countries": countries,
                    "levels": levels,
                    "branches": branches,
                    "states": states,
                    "cities": cities,
                    "nav_ids": "",
                    "nav_prev_id": None,
                    "nav_next_id": None,
                    "terms_count": 0,
                    "form_template": "page_form",
                    "validation_error": validation_error,
                },
            )
    try:
        new_id = db_offices.create_office(data)
    except ValueError as e:
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        states = db_refs.list_states(int(data.get("country_id") or 0)) if data.get("country_id") else []
        cities = db_refs.list_cities(data.get("state_id")) if data.get("state_id") else []
        return templates.TemplateResponse(
            "page_form.html",
            {
                "request": request,
                "office": {**data, "alt_links": alt_links},
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "states": states,
                "cities": cities,
                "nav_ids": "",
                "nav_prev_id": None,
                "nav_next_id": None,
                "terms_count": 0,
                "form_template": "page_form",
                "validation_error": str(e),
            },
        )
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
        "ignore_non_links": bool(first.get("ignore_non_links")),
        "remove_duplicates": bool(first.get("remove_duplicates")),
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


@app.post("/api/pages/{source_page_id}/enabled")
async def api_page_enabled(source_page_id: int, enabled: int = Form(1)):
    """Toggle page enabled (0 or 1). Returns 400 if page not found or update fails."""
    page = db_offices.get_page(source_page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    try:
        db_offices.update_page(source_page_id, {**page, "enabled": enabled == 1})
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return JSONResponse({"ok": True})


def _page_redirect_query(nav_q: str, list_return_q: str) -> str:
    parts = []
    if nav_q:
        parts.append("nav_ids=" + nav_q)
    if list_return_q:
        parts.append(list_return_q)
    return "&".join(parts)


def _validate_level_state_city(level_id, state_id, city_id, branch_id=None) -> None:
    """Raise ValueError if level/state/city combination is invalid. Federal: state and city empty (except Federal+Legislative allows state, not city); State: state required, city empty; Local: state and city required."""
    if not level_id:
        return
    level_name = (db_refs.get_level_name(int(level_id) if level_id else 0) or "").strip().lower()
    branch_name = (db_refs.get_branch_name(int(branch_id) if branch_id else 0) or "").strip().lower()
    state_set = state_id is not None and state_id != 0
    city_set = city_id is not None and city_id != 0
    if level_name == "federal":
        if branch_name == "legislative":
            if city_set:
                raise ValueError("For Federal Legislative level, City must be empty (State is allowed).")
        else:
            if state_set or city_set:
                raise ValueError("For Federal level, State and City must be empty.")
    elif level_name == "state":
        if not state_set:
            raise ValueError("For State level, State is required.")
        if city_set:
            raise ValueError("For State level, City must be empty.")
    elif level_name == "local":
        if not state_set:
            raise ValueError("For Local level, State is required.")
        if not city_set:
            raise ValueError("For Local level, City is required.")


@app.post("/pages/{source_page_id}")
async def page_update(request: Request, source_page_id: int):
    """Update only the page (URL, location). Used when editing one page with multiple offices."""
    form = await request.form()
    save_all = request.headers.get("X-Save-All") == "1"
    nav_q = (form.get("nav_ids") or "").strip()
    list_return_q = (form.get("list_return_query") or "").strip()
    page_data = {
        "url": (form.get("url") or "").strip(),
        "country_id": int(form.get("country_id") or 0),
        "state_id": int(form.get("state_id") or 0) or None,
        "city_id": int(form.get("city_id") or 0) or None,
        "level_id": int(form.get("level_id") or 0) or None,
        "branch_id": int(form.get("branch_id") or 0) or None,
        "notes": (form.get("notes") or "").strip(),
        "enabled": form.get("enabled") == "1",
        "allow_reuse_tables": form.get("allow_reuse_tables") == "1",
    }
    try:
        _validate_level_state_city(page_data.get("level_id"), page_data.get("state_id"), page_data.get("city_id"), page_data.get("branch_id"))
        db_offices.update_page(source_page_id, page_data)
    except ValueError as e:
        from urllib.parse import quote
        offices_on_page = db_offices.list_offices_for_page(source_page_id)
        base = f"/offices/{offices_on_page[0]['id']}" if offices_on_page else "/offices"
        q = "?error=" + quote(str(e))
        if nav_q or list_return_q:
            q += "&" + _page_redirect_query(nav_q, list_return_q)
        redirect_url = f"{base}{q}"
        if save_all:
            return JSONResponse({"ok": False, "error": str(e), "redirect": redirect_url})
        return RedirectResponse(redirect_url, status_code=302)
    first_office_id = db_offices.list_offices_for_page(source_page_id)[0]["id"]
    url = f"/offices/{first_office_id}?page_saved=1"
    if nav_q or list_return_q:
        url += "&" + _page_redirect_query(nav_q, list_return_q)
    url += "#section-page"
    if save_all:
        return JSONResponse({"ok": True, "redirect": url})
    return RedirectResponse(url, status_code=302)


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
    page_saved = request.query_params.get("page_saved") == "1"
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
    q = request.query_params
    list_return_query = _list_return_query(
        country_id=_parse_optional_int(q.get("country_id")),
        state_id=_parse_optional_int(q.get("state_id")),
        level_id=_parse_optional_int(q.get("level_id")),
        branch_id=_parse_optional_int(q.get("branch_id")),
        enabled=q.get("enabled") or None,
        limit=q.get("limit") or None,
        office_count=q.get("office_count") or None,
    )
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    country_id_for_states = (page_data or office).get("country_id") or office.get("country_id") or 0
    states = db_refs.list_states(country_id_for_states) if country_id_for_states else []
    state_id_for_cities = (page_data or {}).get("state_id")
    cities = db_refs.list_cities(state_id_for_cities) if state_id_for_cities else []
    terms_count = db_office_terms.count_terms_for_office(office_id)
    context_obj = page_data or office
    office_categories = db_office_category.list_categories_for_office(
        context_obj.get("country_id"), context_obj.get("level_id"), context_obj.get("branch_id")
    )
    infobox_role_key_filters = db_infobox_role_key_filter.list_filters_for_context(
        context_obj.get("country_id"), context_obj.get("level_id"), context_obj.get("branch_id")
    )
    return templates.TemplateResponse(
        "page_form.html",
        {"request": request, "office": office, "offices_on_page": offices_on_page, "source_page_id": source_page_id, "page_data": page_data, "countries": countries, "levels": levels, "branches": branches, "states": states, "cities": cities, "nav_ids": nav_ids_raw, "nav_prev_id": nav_prev_id, "nav_next_id": nav_next_id, "nav_current": nav_current, "nav_total": nav_total, "list_return_query": list_return_query, "terms_count": terms_count, "saved": saved, "page_saved": page_saved, "validation_error": validation_error, "form_template": "page_form", "office_categories": office_categories, "infobox_role_key_filters": infobox_role_key_filters},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
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
    def _term_dates_merged_for_index(f, idx: int, get_fn, bool_fn) -> bool:
        # Per-index name so each table config gets its own value (unchecked checkboxes omit the key).
        v = f.get("tc_term_dates_merged_" + str(idx))
        if v is not None and str(v).strip() != "":
            return str(v).strip().lower() in ("true", "1", "yes")
        return bool_fn("term_dates_merged", "tc_term_dates_merged")
    tc_id = _get("tc_id", "tc_id")
    if tc_id is not None and str(tc_id).strip() != "":
        try:
            tc_id = int(tc_id)
        except (TypeError, ValueError):
            tc_id = None
    else:
        tc_id = None
    enabled_key = "tc_enabled_" + str(tc_id) if tc_id is not None else "tc_enabled_new_" + str(i)
    enabled_val = form.get(enabled_key)
    enabled = enabled_val == "1" if enabled_val is not None else (_bool("enabled", "tc_enabled") if i == 0 else False)
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
        "filter_column": _int("filter_column", "tc_filter_column", 0),
        "filter_criteria": (_get("filter_criteria", "tc_filter_criteria") or "").strip(),
        "dynamic_parse": _bool("dynamic_parse", "tc_dynamic_parse"),
        "read_right_to_left": _bool("read_right_to_left", "tc_read_right_to_left"),
        "find_date_in_infobox": date_src == "find_date_in_infobox",
        "years_only": date_src == "years_only",
        "parse_rowspan": _bool("parse_rowspan", "tc_parse_rowspan"),
        "consolidate_rowspan_terms": _bool("consolidate_rowspan_terms", "tc_consolidate_rowspan_terms"),
        "rep_link": _bool("rep_link", "tc_rep_link"),
        "party_link": _bool("party_link", "tc_party_link"),
        "enabled": enabled,
        "use_full_page_for_table": _bool("use_full_page_for_table", "tc_use_full_page_for_table"),
        "term_dates_merged": _term_dates_merged_for_index(form, i, _get, _bool),
        "party_ignore": _bool("party_ignore", "tc_party_ignore"),
        "district_ignore": dist_mode == "no_district",
        "district_at_large": dist_mode == "at_large",
        "ignore_non_links": _bool("ignore_non_links", "tc_ignore_non_links"),
        "remove_duplicates": _bool("remove_duplicates", "tc_remove_duplicates"),
        "notes": _get("notes", "tc_notes") or "",
        "name": _get("name", "tc_name") or "",
        "infobox_role_key_filter_id": _validate_infobox_role_key_filter_id(_get("infobox_role_key_filter_id", "tc_infobox_role_key_filter_id")),
    }


@app.post("/offices/{office_id}")
async def office_update(request: Request, office_id: int):
    form = await request.form()
    action = form.get("action", "save_and_close")
    office_only = form.get("office_only") == "1"
    nav_ids = (form.get("nav_ids") or "").strip()
    list_return_query = (form.get("list_return_query") or "").strip()
    alt_links = [v.strip() for v in form.getlist("alt_links") if v and isinstance(v, str) and v.strip()]
    alt_link_include_main = form.get("alt_link_include_main") == "1"
    data = {
        "country_id": int(form.get("country_id") or 0), "state_id": int(form.get("state_id") or 0) or None, "city_id": int(form.get("city_id") or 0) or None, "level_id": int(form.get("level_id") or 0) or None, "branch_id": int(form.get("branch_id") or 0) or None,
        "department": (form.get("department") or "").strip(), "name": (form.get("name") or "").strip(), "enabled": form.get("enabled") == "1", "notes": (form.get("notes") or "").strip(), "url": (form.get("url") or "").strip(),
        "office_category_id": form.get("office_category_id") or None,
        "table_no": int(form.get("table_no") or 1), "table_rows": int(form.get("table_rows") or 4),
        "link_column": int(form.get("link_column") or 1), "party_column": int(form.get("party_column") or 0),
        "term_start_column": int(form.get("term_start_column") or 4), "term_end_column": int(form.get("term_end_column") or 5),
        "district_column": int(form.get("district_column") or 0),
        "filter_column": int(form.get("filter_column") or 0),
        "filter_criteria": (form.get("filter_criteria") or "").strip(),
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
        "ignore_non_links": form.get("ignore_non_links") == "1",
        "remove_duplicates": form.get("remove_duplicates") == "1",
        "infobox_role_key_filter_id": _validate_infobox_role_key_filter_id(form.get("infobox_role_key_filter_id")),
    }
    tc_ids = form.getlist("tc_id")
    tc_table_nos = form.getlist("tc_table_no")
    if tc_table_nos or tc_ids:
        n = max(len(tc_ids), len(tc_table_nos), 1)
        data["table_configs"] = [_form_to_table_config(form, i) for i in range(n)]
    save_all = request.headers.get("X-Save-All") == "1"
    try:
        _validate_level_state_city(data.get("level_id"), data.get("state_id"), data.get("city_id"), data.get("branch_id"))
        updated = db_offices.update_office(office_id, data, office_only=office_only)
        if not updated:
            raise ValueError("Save failed: office was not updated")
        # Verify infobox_role_key persistence before returning saved=1.
        saved_office = db_offices.get_office(office_id)
        if not saved_office:
            raise ValueError("Save verification failed: office not found after update")
        expected_role_keys: dict[int, str] = {}
        submitted_tcs = data.get("table_configs")
        if isinstance(submitted_tcs, list) and submitted_tcs:
            for tc in submitted_tcs:
                try:
                    tno = int(tc.get("table_no") or 1)
                except (TypeError, ValueError):
                    tno = 1
                expected_role_keys[tno] = str(tc.get("infobox_role_key_filter_id") or "")
        else:
            try:
                tno = int(data.get("table_no") or 1)
            except (TypeError, ValueError):
                tno = 1
            expected_role_keys[tno] = str(data.get("infobox_role_key_filter_id") or "")

        actual_role_keys: dict[int, str] = {}
        saved_tcs = saved_office.get("table_configs") if isinstance(saved_office, dict) else None
        if isinstance(saved_tcs, list) and saved_tcs:
            for tc in saved_tcs:
                try:
                    tno = int(tc.get("table_no") or 1)
                except (TypeError, ValueError):
                    continue
                actual_role_keys[tno] = str(tc.get("infobox_role_key_filter_id") or "")
        else:
            try:
                tno = int(saved_office.get("table_no") or 1)
            except (TypeError, ValueError):
                tno = 1
            actual_role_keys[tno] = str(saved_office.get("infobox_role_key_filter_id") or "")

        mismatches = []
        for tno, expected_val in expected_role_keys.items():
            actual_val = (actual_role_keys.get(tno) or "").strip()
            if expected_val != actual_val:
                mismatches.append(f"table {tno}: expected {expected_val!r}, got {actual_val!r}")
        if mismatches:
            raise ValueError("Save verification failed for infobox_role_key_filter_id: " + "; ".join(mismatches))
    except ValueError as e:
        from urllib.parse import quote
        q = "?error=" + quote(str(e))
        if nav_ids:
            q += "&nav_ids=" + nav_ids
        if list_return_query:
            q += "&" + list_return_query
        redirect_url = f"/offices/{office_id}{q}"
        if save_all:
            return JSONResponse({"ok": False, "error": str(e), "redirect": redirect_url})
        return RedirectResponse(redirect_url, status_code=302)
    except sqlite3.IntegrityError as e:
        from urllib.parse import quote
        msg = "Save failed due to conflicting table settings: " + str(e)
        q = "?error=" + quote(msg)
        if nav_ids:
            q += "&nav_ids=" + nav_ids
        if list_return_query:
            q += "&" + list_return_query
        redirect_url = f"/offices/{office_id}{q}"
        if save_all:
            return JSONResponse({"ok": False, "error": msg, "redirect": redirect_url})
        return RedirectResponse(redirect_url, status_code=302)
    if action == "save":
        q = "?saved=1"
        if nav_ids:
            q += "&nav_ids=" + nav_ids
        if list_return_query:
            q += "&" + list_return_query
        hash_frag = "#section-office-" + str(office_id) if office_only else ""
        redirect_url = f"/offices/{office_id}{q}{hash_frag}"
        if save_all:
            return JSONResponse({"ok": True, "redirect": redirect_url})
        return RedirectResponse(redirect_url, status_code=302)
    url = "/offices?saved=1"
    if list_return_query:
        url += "&" + list_return_query
    if save_all:
        return JSONResponse({"ok": True, "redirect": url})
    return RedirectResponse(url, status_code=302)


@app.post("/offices/{office_id}/delete")
async def office_delete(office_id: int):
    db_offices.delete_office(office_id)
    return RedirectResponse("/offices", status_code=302)


@app.post("/offices/{office_id}/table/{tc_id}/delete")
async def table_delete(
    office_id: int,
    tc_id: int,
    return_query: str = Form(""),
):
    """Delete one table config. Redirect back to office edit. Confirmation must be done in UI."""
    try:
        db_offices.delete_table(tc_id)
    except ValueError as e:
        from urllib.parse import quote
        url = f"/offices/{office_id}?error=" + quote(str(e))
        if return_query and return_query.strip():
            q = return_query.strip().lstrip("?")
            if q:
                url += "&" + q
        return RedirectResponse(url, status_code=302)
    url = f"/offices/{office_id}?saved=1"
    if return_query and return_query.strip():
        q = return_query.strip().lstrip("?")
        if q:
            from urllib.parse import parse_qsl, urlencode
            params = parse_qsl(q, keep_blank_values=True)
            params = [(k, v) for k, v in params if k.lower() != "saved"]
            if params:
                url += "&" + urlencode(params)
    return RedirectResponse(url, status_code=302)


@app.post("/offices/{office_id}/table/{tc_id}/move")
async def table_move(
    office_id: int,
    tc_id: int,
    to_office_id: int = Form(...),
    delete_source_office_if_empty: str = Form(""),
    return_query: str = Form(""),
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
    if return_query and return_query.strip():
        from urllib.parse import parse_qsl, urlencode
        q = return_query.strip().lstrip("?")
        if q:
            params = parse_qsl(q, keep_blank_values=True)
            params = [(k, v) for k, v in params if k.lower() != "saved"]
            if params:
                redirect_url += "&" + urlencode(params)
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
        "filter_column": int(office.get("filter_column") or 0),
        "filter_criteria": (office.get("filter_criteria") or ""),
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
        "ignore_non_links": bool(office.get("ignore_non_links")),
        "remove_duplicates": bool(office.get("remove_duplicates")),
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


@app.get("/api/offices/{office_id}/table-configs")
async def api_office_table_configs(office_id: int, table_no: int | None = None):
    """Return saved table config details for one office (including infobox_role_key)."""
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404, detail="Office not found")
    tcs = office.get("table_configs") if isinstance(office.get("table_configs"), list) and office.get("table_configs") else []
    if not tcs:
        tcs = [{
            "id": office.get("id"),
            "table_no": office.get("table_no"),
            "infobox_role_key_filter_id": office.get("infobox_role_key_filter_id"),
            "infobox_role_key": (office.get("infobox_role_key") or "").strip(),
        }]
    if table_no is not None:
        tcs = [tc for tc in tcs if int(tc.get("table_no") or 1) == int(table_no)]
    out = []
    for tc in tcs:
        out.append({
            "id": tc.get("id"),
            "table_no": int(tc.get("table_no") or 1),
            "name": tc.get("name") or "",
            "enabled": bool(tc.get("enabled")),
            "find_date_in_infobox": bool(tc.get("find_date_in_infobox")),
            "infobox_role_key_filter_id": tc.get("infobox_role_key_filter_id"),
            "infobox_role_key": (tc.get("infobox_role_key") or "").strip(),
        })
    return JSONResponse({"ok": True, "office_id": office_id, "table_configs": out})


@app.post("/api/offices/{office_id}/set-infobox-role-key")
async def api_office_set_infobox_role_key(office_id: int, request: Request):
    """Deprecated: use /api/offices/{office_id}/set-infobox-role-key-filter."""
    return JSONResponse(
        {
            "ok": False,
            "deprecated": True,
            "message": "Use /api/offices/{office_id}/set-infobox-role-key-filter with table_config_id and infobox_role_key_filter_id.",
        },
        status_code=410,
    )


@app.post("/api/offices/{office_id}/set-infobox-role-key-filter")
async def api_office_set_infobox_role_key_filter(office_id: int, request: Request):
    """Set infobox role-key filter by office table_config_id.

    Body JSON: {"table_config_id": 123, "infobox_role_key_filter_id": 7}
    """
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404, detail="Office not found")
    try:
        body = await request.json()
    except Exception:
        body = {}
    table_config_id_raw = (body or {}).get("table_config_id")
    if table_config_id_raw in (None, ""):
        raise HTTPException(status_code=400, detail="table_config_id is required")
    try:
        table_config_id = int(table_config_id_raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="table_config_id must be an integer")
    tcs = office.get("table_configs") if isinstance(office.get("table_configs"), list) and office.get("table_configs") else []
    match_tc = next((tc for tc in tcs if int(tc.get("id") or 0) == table_config_id), None)
    if not match_tc:
        raise HTTPException(status_code=404, detail=f"No table config {table_config_id} found for office {office_id}")
    try:
        filter_id = _validate_infobox_role_key_filter_id((body or {}).get("infobox_role_key_filter_id"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    updated = db_offices.set_infobox_role_key_by_table_config_id(table_config_id, str(filter_id or ""))
    if not updated:
        raise HTTPException(status_code=404, detail=f"Table config {table_config_id} not found")
    office_after = db_offices.get_office_by_table_config_id(table_config_id)
    if not office_after:
        raise HTTPException(status_code=404, detail=f"Table config {table_config_id} not found after save")
    return JSONResponse({
        "ok": True,
        "message": "Saved",
        "office_id": office_id,
        "table_config": {
            "id": int(office_after.get("id") or table_config_id),
            "table_no": int(office_after.get("table_no") or 1),
            "infobox_role_key_filter_id": office_after.get("infobox_role_key_filter_id"),
            "infobox_role_key": (office_after.get("infobox_role_key") or "").strip(),
            "enabled": bool(office_after.get("enabled")),
            "find_date_in_infobox": bool(office_after.get("find_date_in_infobox")),
        },
    })


@app.get("/api/table-configs/{table_config_id}")
async def api_table_config_get(table_config_id: int):
    """Return one table config row (including infobox_role_key) by office_table_config.id."""
    office = db_offices.get_office_by_table_config_id(table_config_id)
    if not office:
        raise HTTPException(status_code=404, detail=f"Table config {table_config_id} not found")
    return JSONResponse({
        "ok": True,
        "table_config": {
            "id": int(office.get("id") or table_config_id),
            "office_details_id": int(office.get("office_details_id") or 0) or None,
            "table_no": int(office.get("table_no") or 1),
            "name": office.get("name") or "",
            "infobox_role_key_filter_id": office.get("infobox_role_key_filter_id"),
            "infobox_role_key": (office.get("infobox_role_key") or "").strip(),
        },
    })


@app.post("/api/table-configs/{table_config_id}/set-infobox-role-key")
async def api_table_config_set_infobox_role_key(table_config_id: int, request: Request):
    """Deprecated: use /api/table-configs/{table_config_id}/set-infobox-role-key-filter."""
    return JSONResponse(
        {
            "ok": False,
            "deprecated": True,
            "message": "Use /api/table-configs/{table_config_id}/set-infobox-role-key-filter with infobox_role_key_filter_id.",
        },
        status_code=410,
    )


@app.post("/api/table-configs/{table_config_id}/set-infobox-role-key-filter")
async def api_table_config_set_infobox_role_key_filter(table_config_id: int, request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    try:
        filter_id = _validate_infobox_role_key_filter_id((body or {}).get("infobox_role_key_filter_id"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    updated = db_offices.set_infobox_role_key_by_table_config_id(table_config_id, str(filter_id or ""))
    if not updated:
        raise HTTPException(status_code=404, detail=f"Table config {table_config_id} not found")
    office = db_offices.get_office_by_table_config_id(table_config_id)
    if not office:
        raise HTTPException(status_code=404, detail=f"Table config {table_config_id} not found after save")
    return JSONResponse(
        {
            "ok": True,
            "message": "Saved",
            "table_config": {
                "id": int(office.get("id") or table_config_id),
                "office_details_id": int(office.get("office_details_id") or 0) or None,
                "table_no": int(office.get("table_no") or 1),
                "name": office.get("name") or "",
                "infobox_role_key_filter_id": office.get("infobox_role_key_filter_id"),
                "infobox_role_key": (office.get("infobox_role_key") or "").strip(),
            },
        }
    )


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
        can_force_override = revalidate_failed and REVALIDATE_MSG_MISSING_HOLDERS in (revalidate_msg or "")
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


# ---------- Reference data (manage) ----------
@app.get("/refs", response_class=HTMLResponse)
async def refs_index(request: Request):
    return templates.TemplateResponse("refs.html", {"request": request})


@app.get("/refs/countries", response_class=HTMLResponse)
async def refs_countries_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        "refs_countries.html",
        {"request": request, "countries": countries, "saved": saved, "error": error},
    )


@app.get("/refs/countries/new", response_class=HTMLResponse)
async def refs_country_new(request: Request):
    return templates.TemplateResponse("refs_country_form.html", {"request": request, "country": None})


@app.post("/refs/countries/new")
async def refs_country_create(request: Request, name: str = Form("")):
    try:
        db_refs.create_country(name)
        return RedirectResponse("/refs/countries?saved=1", status_code=302)
    except ValueError as e:
        return templates.TemplateResponse(
            "refs_country_form.html",
            {"request": request, "country": {"name": name}, "validation_error": str(e)},
        )


@app.get("/refs/countries/{country_id}", response_class=HTMLResponse)
async def refs_country_edit(request: Request, country_id: int):
    country = db_refs.get_country(country_id)
    if not country:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse("refs_country_form.html", {"request": request, "country": country})


@app.post("/refs/countries/{country_id}")
async def refs_country_update(request: Request, country_id: int, name: str = Form("")):
    try:
        db_refs.update_country(country_id, name)
        return RedirectResponse("/refs/countries?saved=1", status_code=302)
    except ValueError as e:
        country = db_refs.get_country(country_id)
        if not country:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse(
            "refs_country_form.html",
            {"request": request, "country": {**country, "name": name}, "validation_error": str(e)},
        )


@app.post("/refs/countries/{country_id}/delete")
async def refs_country_delete(country_id: int):
    try:
        db_refs.delete_country(country_id)
        return RedirectResponse("/refs/countries", status_code=302)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse("/refs/countries?error=" + quote(str(e)), status_code=302)


@app.get("/refs/states", response_class=HTMLResponse)
async def refs_states_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    states = db_refs.list_states_with_country()
    return templates.TemplateResponse(
        "refs_states.html",
        {"request": request, "states": states, "saved": saved, "error": error},
    )


@app.get("/refs/states/new", response_class=HTMLResponse)
async def refs_state_new(request: Request):
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        "refs_state_form.html", {"request": request, "state": None, "countries": countries}
    )


@app.post("/refs/states/new")
async def refs_state_create(request: Request, country_id: int = Form(0), name: str = Form("")):
    try:
        db_refs.create_state(country_id, name)
        return RedirectResponse("/refs/states?saved=1", status_code=302)
    except ValueError as e:
        countries = db_refs.list_countries()
        return templates.TemplateResponse(
            "refs_state_form.html",
            {"request": request, "state": None, "countries": countries, "validation_error": str(e), "form_country_id": country_id, "form_name": name},
        )


@app.get("/refs/states/{state_id}", response_class=HTMLResponse)
async def refs_state_edit(request: Request, state_id: int):
    state = db_refs.get_state(state_id)
    if not state:
        raise HTTPException(status_code=404)
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        "refs_state_form.html", {"request": request, "state": state, "countries": countries}
    )


@app.post("/refs/states/{state_id}")
async def refs_state_update(request: Request, state_id: int, country_id: int = Form(0), name: str = Form("")):
    try:
        db_refs.update_state(state_id, country_id, name)
        return RedirectResponse("/refs/states?saved=1", status_code=302)
    except ValueError as e:
        state = db_refs.get_state(state_id)
        if not state:
            raise HTTPException(status_code=404)
        countries = db_refs.list_countries()
        return templates.TemplateResponse(
            "refs_state_form.html",
            {"request": request, "state": {**state, "country_id": country_id, "name": name}, "countries": countries, "validation_error": str(e)},
        )


@app.post("/refs/states/{state_id}/delete")
async def refs_state_delete(state_id: int):
    try:
        db_refs.delete_state(state_id)
        return RedirectResponse("/refs/states", status_code=302)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse("/refs/states?error=" + quote(str(e)), status_code=302)


@app.get("/refs/levels", response_class=HTMLResponse)
async def refs_levels_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    levels = db_refs.list_levels()
    return templates.TemplateResponse(
        "refs_levels.html",
        {"request": request, "levels": levels, "saved": saved, "error": error},
    )


@app.get("/refs/levels/new", response_class=HTMLResponse)
async def refs_level_new(request: Request):
    return templates.TemplateResponse("refs_level_form.html", {"request": request, "level": None})


@app.post("/refs/levels/new")
async def refs_level_create(request: Request, name: str = Form("")):
    try:
        db_refs.create_level(name)
        return RedirectResponse("/refs/levels?saved=1", status_code=302)
    except ValueError as e:
        return templates.TemplateResponse(
            "refs_level_form.html",
            {"request": request, "level": {"name": name}, "validation_error": str(e)},
        )


@app.get("/refs/levels/{level_id}", response_class=HTMLResponse)
async def refs_level_edit(request: Request, level_id: int):
    level = db_refs.get_level(level_id)
    if not level:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse("refs_level_form.html", {"request": request, "level": level})


@app.post("/refs/levels/{level_id}")
async def refs_level_update(request: Request, level_id: int, name: str = Form("")):
    try:
        db_refs.update_level(level_id, name)
        return RedirectResponse("/refs/levels?saved=1", status_code=302)
    except ValueError as e:
        level = db_refs.get_level(level_id)
        if not level:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse(
            "refs_level_form.html",
            {"request": request, "level": {**level, "name": name}, "validation_error": str(e)},
        )


@app.post("/refs/levels/{level_id}/delete")
async def refs_level_delete(level_id: int):
    try:
        db_refs.delete_level(level_id)
        return RedirectResponse("/refs/levels", status_code=302)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse("/refs/levels?error=" + quote(str(e)), status_code=302)


@app.get("/refs/branches", response_class=HTMLResponse)
async def refs_branches_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        "refs_branches.html",
        {"request": request, "branches": branches, "saved": saved, "error": error},
    )


@app.get("/refs/branches/new", response_class=HTMLResponse)
async def refs_branch_new(request: Request):
    return templates.TemplateResponse("refs_branch_form.html", {"request": request, "branch": None})


@app.post("/refs/branches/new")
async def refs_branch_create(request: Request, name: str = Form("")):
    try:
        db_refs.create_branch(name)
        return RedirectResponse("/refs/branches?saved=1", status_code=302)
    except ValueError as e:
        return templates.TemplateResponse(
            "refs_branch_form.html",
            {"request": request, "branch": {"name": name}, "validation_error": str(e)},
        )


@app.get("/refs/branches/{branch_id}", response_class=HTMLResponse)
async def refs_branch_edit(request: Request, branch_id: int):
    branch = db_refs.get_branch(branch_id)
    if not branch:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse("refs_branch_form.html", {"request": request, "branch": branch})


@app.post("/refs/branches/{branch_id}")
async def refs_branch_update(request: Request, branch_id: int, name: str = Form("")):
    try:
        db_refs.update_branch(branch_id, name)
        return RedirectResponse("/refs/branches?saved=1", status_code=302)
    except ValueError as e:
        branch = db_refs.get_branch(branch_id)
        if not branch:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse(
            "refs_branch_form.html",
            {"request": request, "branch": {**branch, "name": name}, "validation_error": str(e)},
        )


@app.post("/refs/branches/{branch_id}/delete")
async def refs_branch_delete(branch_id: int):
    try:
        db_refs.delete_branch(branch_id)
        return RedirectResponse("/refs/branches", status_code=302)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse("/refs/branches?error=" + quote(str(e)), status_code=302)


# ---------- Cities (reference data) ----------
@app.get("/refs/cities", response_class=HTMLResponse)
async def refs_cities_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    cities = db_refs.list_cities_with_country_state()
    return templates.TemplateResponse(
        "refs_cities.html",
        {"request": request, "cities": cities, "saved": saved, "error": error},
    )


@app.get("/refs/cities/new", response_class=HTMLResponse)
async def refs_city_new(request: Request):
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        "refs_city_form.html", {"request": request, "city": None, "states": [], "countries": countries}
    )


@app.post("/refs/cities/new")
async def refs_city_create(request: Request, state_id: int = Form(0), name: str = Form("")):
    try:
        db_refs.create_city(state_id, name)
        return RedirectResponse("/refs/cities?saved=1", status_code=302)
    except ValueError as e:
        countries = db_refs.list_countries()
        state_row = db_refs.get_state(state_id) if state_id else None
        form_country_id = state_row.get("country_id") if state_row else None
        states = db_refs.list_states(form_country_id) if form_country_id else []
        return templates.TemplateResponse(
            "refs_city_form.html",
            {"request": request, "city": None, "states": states, "countries": countries, "validation_error": str(e), "form_state_id": state_id, "form_name": name, "form_country_id": form_country_id},
        )


@app.get("/refs/cities/{city_id}", response_class=HTMLResponse)
async def refs_city_edit(request: Request, city_id: int):
    city = db_refs.get_city(city_id)
    if not city:
        raise HTTPException(status_code=404)
    state_row = db_refs.get_state(city["state_id"]) if city.get("state_id") else None
    form_country_id = state_row.get("country_id") if state_row else None
    states = db_refs.list_states(form_country_id) if form_country_id else []
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        "refs_city_form.html", {"request": request, "city": city, "states": states, "countries": countries, "form_country_id": form_country_id}
    )


@app.post("/refs/cities/{city_id}")
async def refs_city_update(request: Request, city_id: int, state_id: int = Form(0), name: str = Form("")):
    try:
        db_refs.update_city(city_id, state_id, name)
        return RedirectResponse("/refs/cities?saved=1", status_code=302)
    except ValueError as e:
        city = db_refs.get_city(city_id)
        if not city:
            raise HTTPException(status_code=404)
        state_row = db_refs.get_state(state_id) if state_id else None
        form_country_id = state_row.get("country_id") if state_row else None
        states = db_refs.list_states(form_country_id) if form_country_id else []
        countries = db_refs.list_countries()
        return templates.TemplateResponse(
            "refs_city_form.html",
            {"request": request, "city": {**city, "state_id": state_id, "name": name}, "states": states, "countries": countries, "form_country_id": form_country_id, "validation_error": str(e)},
        )


@app.post("/refs/cities/{city_id}/delete")
async def refs_city_delete(city_id: int):
    try:
        db_refs.delete_city(city_id)
        return RedirectResponse("/refs/cities", status_code=302)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse("/refs/cities?error=" + quote(str(e)), status_code=302)


# ---------- Office categories (reference data) ----------
@app.get("/refs/office-categories", response_class=HTMLResponse)
async def refs_office_categories_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    categories = db_office_category.list_office_categories()
    return templates.TemplateResponse(
        "refs_office_categories.html",
        {"request": request, "categories": categories, "saved": saved, "error": error},
    )


@app.get("/refs/office-categories/new", response_class=HTMLResponse)
async def refs_office_category_new(request: Request):
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        "refs_office_category_form.html",
        {"request": request, "category": None, "countries": countries, "levels": levels, "branches": branches},
    )


def _form_ids(form, key: str) -> list[int]:
    """Return list of int ids from form getlist(key), ignoring empty/zero."""
    raw = form.getlist(key) if hasattr(form, "getlist") else []
    ids = []
    for v in raw:
        try:
            n = int(v) if v else 0
            if n:
                ids.append(n)
        except (TypeError, ValueError):
            pass
    return ids


@app.post("/refs/office-categories/new")
async def refs_office_category_create(request: Request):
    form = await request.form()
    name = (form.get("name") or "").strip()
    country_ids = _form_ids(form, "country_ids")
    level_ids = _form_ids(form, "level_ids")
    branch_ids = _form_ids(form, "branch_ids")
    try:
        db_office_category.create_office_category(name, country_ids, level_ids, branch_ids)
        return RedirectResponse("/refs/office-categories?saved=1", status_code=302)
    except ValueError as e:
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        return templates.TemplateResponse(
            "refs_office_category_form.html",
            {
                "request": request,
                "category": None,
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "validation_error": str(e),
                "form_name": name,
                "form_country_ids": country_ids,
                "form_level_ids": level_ids,
                "form_branch_ids": branch_ids,
            },
        )


@app.get("/refs/office-categories/{category_id}", response_class=HTMLResponse)
async def refs_office_category_edit(request: Request, category_id: int):
    category = db_office_category.get_office_category(category_id)
    if not category:
        raise HTTPException(status_code=404)
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        "refs_office_category_form.html",
        {"request": request, "category": category, "countries": countries, "levels": levels, "branches": branches},
    )


@app.post("/refs/office-categories/{category_id}")
async def refs_office_category_update(request: Request, category_id: int):
    form = await request.form()
    name = (form.get("name") or "").strip()
    country_ids = _form_ids(form, "country_ids")
    level_ids = _form_ids(form, "level_ids")
    branch_ids = _form_ids(form, "branch_ids")
    try:
        updated = db_office_category.update_office_category(category_id, name, country_ids, level_ids, branch_ids)
        if not updated:
            raise HTTPException(status_code=404)
        return RedirectResponse("/refs/office-categories?saved=1", status_code=302)
    except ValueError as e:
        category = db_office_category.get_office_category(category_id)
        if not category:
            raise HTTPException(status_code=404)
        category = {**category, "name": name, "country_ids": country_ids, "level_ids": level_ids, "branch_ids": branch_ids}
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        return templates.TemplateResponse(
            "refs_office_category_form.html",
            {
                "request": request,
                "category": category,
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "validation_error": str(e),
            },
        )


@app.post("/refs/office-categories/{category_id}/delete")
async def refs_office_category_delete(category_id: int):
    try:
        db_office_category.delete_office_category(category_id)
        return RedirectResponse("/refs/office-categories", status_code=302)
    except ValueError as e:
        from urllib.parse import quote
        return RedirectResponse("/refs/office-categories?error=" + quote(str(e)), status_code=302)


# ---------- Infobox role key filters (reference data) ----------
@app.get("/refs/infobox-role-key-filters", response_class=HTMLResponse)
async def refs_infobox_role_key_filters_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    filters = db_infobox_role_key_filter.list_infobox_role_key_filters()
    return templates.TemplateResponse(
        "refs_infobox_role_key_filters.html",
        {"request": request, "filters": filters, "saved": saved, "error": error},
    )


@app.get("/refs/infobox-role-key-filters/new", response_class=HTMLResponse)
async def refs_infobox_role_key_filter_new(request: Request):
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        "refs_infobox_role_key_filter_form.html",
        {"request": request, "filter_obj": None, "countries": countries, "levels": levels, "branches": branches},
    )


@app.post("/refs/infobox-role-key-filters/new")
async def refs_infobox_role_key_filter_create(request: Request):
    form = await request.form()
    name = (form.get("name") or "").strip()
    role_key = (form.get("role_key") or "").strip()
    country_ids = _form_ids(form, "country_ids")
    level_ids = _form_ids(form, "level_ids")
    branch_ids = _form_ids(form, "branch_ids")
    try:
        db_infobox_role_key_filter.create_infobox_role_key_filter(name, role_key, country_ids, level_ids, branch_ids)
        return RedirectResponse("/refs/infobox-role-key-filters?saved=1", status_code=302)
    except ValueError as e:
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        return templates.TemplateResponse(
            "refs_infobox_role_key_filter_form.html",
            {
                "request": request,
                "filter_obj": None,
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "validation_error": str(e),
                "form_name": name,
                "form_role_key": role_key,
                "form_country_ids": country_ids,
                "form_level_ids": level_ids,
                "form_branch_ids": branch_ids,
            },
        )


@app.get("/refs/infobox-role-key-filters/{filter_id}", response_class=HTMLResponse)
async def refs_infobox_role_key_filter_edit(request: Request, filter_id: int):
    filter_obj = db_infobox_role_key_filter.get_infobox_role_key_filter(filter_id)
    if not filter_obj:
        raise HTTPException(status_code=404)
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        "refs_infobox_role_key_filter_form.html",
        {"request": request, "filter_obj": filter_obj, "countries": countries, "levels": levels, "branches": branches},
    )


@app.post("/refs/infobox-role-key-filters/{filter_id}")
async def refs_infobox_role_key_filter_update(request: Request, filter_id: int):
    form = await request.form()
    name = (form.get("name") or "").strip()
    role_key = (form.get("role_key") or "").strip()
    country_ids = _form_ids(form, "country_ids")
    level_ids = _form_ids(form, "level_ids")
    branch_ids = _form_ids(form, "branch_ids")
    try:
        updated = db_infobox_role_key_filter.update_infobox_role_key_filter(
            filter_id, name, role_key, country_ids, level_ids, branch_ids
        )
        if not updated:
            raise HTTPException(status_code=404)
        return RedirectResponse("/refs/infobox-role-key-filters?saved=1", status_code=302)
    except ValueError as e:
        filter_obj = db_infobox_role_key_filter.get_infobox_role_key_filter(filter_id)
        if not filter_obj:
            raise HTTPException(status_code=404)
        filter_obj = {
            **filter_obj,
            "name": name,
            "role_key": role_key,
            "country_ids": country_ids,
            "level_ids": level_ids,
            "branch_ids": branch_ids,
        }
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        return templates.TemplateResponse(
            "refs_infobox_role_key_filter_form.html",
            {
                "request": request,
                "filter_obj": filter_obj,
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "validation_error": str(e),
            },
        )


@app.post("/refs/infobox-role-key-filters/{filter_id}/delete")
async def refs_infobox_role_key_filter_delete(filter_id: int):
    db_infobox_role_key_filter.delete_infobox_role_key_filter(filter_id)
    return RedirectResponse("/refs/infobox-role-key-filters", status_code=302)


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


@app.get("/api/cities")
async def api_cities(state_id: int = Query(0)):
    if not state_id:
        return JSONResponse([])
    return JSONResponse(db_refs.list_cities(state_id))


# ---------- Run scraper ----------


def _snapshot_member_pages_for_test(
    *,
    source_url: str,
    config_json: dict,
    html_content: str,
    file_prefix: str,
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
        rel_name = f"{file_prefix}_{safe_member}.html"
        dest = db_test_scripts.TEST_SCRIPTS_DIR / rel_name
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

def _store_test_script_result(payload: dict) -> str:
    rid = uuid.uuid4().hex
    with _test_script_result_lock:
        _test_script_result_store[rid] = payload
    return rid


@app.get("/test-scripts/results/{result_id}", response_class=HTMLResponse)
async def test_script_result_page(request: Request, result_id: str):
    with _test_script_result_lock:
        payload = _test_script_result_store.get(result_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Result not found")
    return templates.TemplateResponse("test_script_result.html", {"request": request, "payload": payload, "result_id": result_id})


@app.get("/test-scripts", response_class=HTMLResponse)
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


@app.get("/ui-test-scripts", response_class=HTMLResponse)
async def ui_test_scripts_page(request: Request):
    return templates.TemplateResponse(
        "ui_test_scripts.html",
        {
            "request": request,
            "test_path": "src/test_ui_edit_office_playwright.py",
            "defaults": _ui_test_env_defaults(),
        },
    )


@app.post("/api/ui-test-scripts/run/start")
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


@app.get("/api/ui-test-scripts/run/status/{job_id}")
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


@app.post("/api/ui-test-scripts/run")
async def api_run_ui_test_scripts(request: Request):
    # Backward-compatible synchronous endpoint used by older clients.
    payload = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return _execute_ui_test_run(payload)


@app.get("/api/test-scripts/office-templates/pages")
async def api_test_script_template_pages(q: str = Query(""), limit: int = Query(25)):
    rows = db_offices.search_pages_for_test_script_templates(q, limit=limit)
    return JSONResponse({"ok": True, "pages": rows})


@app.get("/api/test-scripts/office-templates/pages/{source_page_id}")
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


@app.get("/api/test-scripts/{test_id}")
async def api_get_test_script(test_id: int):
    row = db_test_scripts.get_test(test_id)
    if not row:
        raise HTTPException(status_code=404, detail="Test script not found")
    return JSONResponse({"ok": True, "test": row})


@app.post("/api/test-scripts/preview")
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


@app.post("/api/test-scripts")
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
            safe_page = re.sub(r"[^a-zA-Z0-9._-]+", "_", (source_url.split("/")[-1] or "wiki_page"))
            prefix = f"{uuid.uuid4().hex}_{safe_page}"
            dest = db_test_scripts.TEST_SCRIPTS_DIR / f"{prefix}.html"
            dest.write_text(html_content, encoding="utf-8")
            html_file = dest.name

            config_json, _member_urls, _member_files, auto_rows = _snapshot_member_pages_for_test(
                source_url=source_url,
                config_json=config_json,
                html_content=html_content,
                file_prefix=prefix,
            )
            if _expected_missing(expected_json):
                expected_json = auto_rows

            if delete_existing_files and (existing.get("html_file") or "").strip():
                old_path = (db_test_scripts.TEST_SCRIPTS_DIR / existing["html_file"]).resolve()
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
                        old_member = (db_test_scripts.TEST_SCRIPTS_DIR / rel_name).resolve()
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

    safe_page = re.sub(r"[^a-zA-Z0-9._-]+", "_", (source_url.split("/")[-1] or "wiki_page"))
    prefix = f"{uuid.uuid4().hex}_{safe_page}"
    dest = db_test_scripts.TEST_SCRIPTS_DIR / f"{prefix}.html"
    dest.write_text(html_content, encoding="utf-8")

    config_json, _member_urls, _member_files, auto_rows = _snapshot_member_pages_for_test(
        source_url=source_url,
        config_json=config_json,
        html_content=html_content,
        file_prefix=prefix,
    )
    if _expected_missing(expected_json):
        expected_json = auto_rows

    new_test_id = db_test_scripts.create_test({
        "name": name,
        "test_type": test_type,
        "enabled": enabled,
        "source_url": source_url,
        "html_file": dest.name,
        "config_json": config_json,
        "expected_json": expected_json,
    })
    return JSONResponse({"ok": True, "id": new_test_id, "html_file": dest.name, "updated": False})


@app.post("/api/test-scripts/{test_id}/enabled")
async def api_toggle_test_script(test_id: int, enabled: int = Form(...)):
    db_test_scripts.update_test_enabled(test_id, bool(enabled))
    return JSONResponse({"ok": True})


@app.post("/api/test-scripts/{test_id}/delete")
async def api_delete_test_script(test_id: int):
    db_test_scripts.delete_test(test_id)
    return RedirectResponse("/test-scripts", status_code=303)


@app.post("/api/test-scripts/{test_id}/run")
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


@app.post("/api/test-scripts/run-enabled")
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

@app.get("/run", response_class=HTMLResponse)
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


@app.post("/api/run")
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




@app.get("/api/run/matching-individuals")
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
            "filter_column": int(config.get("filter_column") or 0),
            "filter_criteria": (config.get("filter_criteria") or ""),
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
            "ignore_non_links": _config_bool_export(config.get("ignore_non_links")),
            "remove_duplicates": _config_bool_export(config.get("remove_duplicates")),
            "infobox_role_key_filter_id": config.get("infobox_role_key_filter_id"),
            "country_name": "", "level_name": "", "branch_name": "", "state_name": "",
        }
        office_row["infobox_role_key"] = (config.get("infobox_role_key") or "").strip() or _resolve_infobox_role_key_from_filter_id(
            office_row.get("infobox_role_key_filter_id")
        )
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
                "alt_links": config.get("alt_links") if isinstance(config.get("alt_links"), list) else [],
                "alt_link_include_main": _config_bool(config.get("alt_link_include_main")),
                "use_full_page_for_table": _config_bool(config.get("use_full_page_for_table")),
                "term_dates_merged": _config_bool(config.get("term_dates_merged")),
                "party_ignore": _config_bool(config.get("party_ignore")),
                "district_ignore": _config_bool(config.get("district_ignore")),
                "district_at_large": _config_bool(config.get("district_at_large")),
                "remove_duplicates": _config_bool(config.get("remove_duplicates")),
                "infobox_role_key_filter_id": config.get("infobox_role_key_filter_id"),
                "country_name": "", "level_name": "", "branch_name": "", "state_name": "",
            }
            office_row["infobox_role_key"] = (config.get("infobox_role_key") or "").strip() or _resolve_infobox_role_key_from_filter_id(
                office_row.get("infobox_role_key_filter_id")
            )
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
