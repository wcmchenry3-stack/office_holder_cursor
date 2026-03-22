# -*- coding: utf-8 -*-
"""Office config CRUD and related routes."""

import json
import os
import re
import sqlite3
import tempfile
import threading
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode

from markupsafe import Markup
from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from src.routers._deps import templates
from src.routers._helpers import (
    _validate_infobox_role_key_filter_id,
    _resolve_infobox_role_key_from_filter_id,
    _parse_optional_int,
    _office_draft_from_body,
)
from src.db.connection import get_connection
from src.db import offices as db_offices
from src.db import refs as db_refs
from src.db import parties as db_parties
from src.db import office_category as db_office_category
from src.db import infobox_role_key_filter as db_infobox_role_key_filter
from src.db import individuals as db_individuals
from src.db import office_terms as db_office_terms
from src.db.bulk_import import bulk_import_offices_from_csv
from src.scraper.runner import run_with_db, preview_with_config, parse_full_table_for_export, find_best_matching_table_for_existing_terms
from src.scraper.config_test import test_office_config, get_raw_table_preview, get_all_tables_preview, get_table_html, get_table_header_from_html
from src.scraper.test_script_runner import run_test_script, run_test_script_from_html
from src.scraper.wiki_fetch import normalize_wiki_url

router = APIRouter()

ROOT = Path(__file__).resolve().parent.parent.parent

# Job progress store for async populate-bio/terms (in-memory, single-user)
_populate_job_store: dict = {}
_populate_job_lock = threading.Lock()

REVALIDATE_MSG_MISSING_HOLDERS = "New list is missing office holders that were in existing data. Kept existing terms."


def _list_return_query(
    country_id=None,
    state_id=None,
    level_id=None,
    branch_id=None,
    office_category_id=None,
    enabled=None,
    limit=None,
    office_count=None,
) -> str:
    """Build query string for returning to the page list with filters applied (for Cancel link)."""
    parts: list = []
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


def _page_redirect_query(nav_q: str, list_return_q: str) -> str:
    parts = []
    if nav_q:
        parts.append("nav_ids=" + nav_q)
    if list_return_q:
        parts.append(list_return_q)
    return "&".join(parts)


def _validate_level_state_city(level_id, state_id, city_id, branch_id=None) -> None:
    """Raise ValueError if level/state/city combination is invalid."""
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


# ---------- Office config CRUD ----------

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return RedirectResponse("/offices", status_code=302)


@router.get("/offices", response_class=HTMLResponse)
async def offices_list(
    request: Request,
    country_id: "str | None" = Query(None),
    state_id: "str | None" = Query(None),
    level_id: "str | None" = Query(None),
    branch_id: "str | None" = Query(None),
    office_category_id: "str | None" = Query(None),
    enabled: "str | None" = Query(None),
    limit: "str | None" = Query(None),
    office_count: "str | None" = Query("all"),
    search_url: "str | None" = Query(None),
    search_office_id: "str | None" = Query(None),
):
    saved = request.query_params.get("saved") == "1"
    page_saved = request.query_params.get("page_saved") == "1"
    validation_error = request.query_params.get("error") or None
    imported_count = request.query_params.get("count")
    imported_errors = request.query_params.get("errors")
    imported = request.query_params.get("imported") == "1"

    # Quick jump by office id or page URL
    search_url_val = (search_url or "").strip() if search_url else ""
    search_office_id_val = _parse_optional_int(search_office_id)
    if search_office_id_val is not None:
        office = db_offices.get_office(search_office_id_val)
        if office:
            return RedirectResponse(f"/offices/{search_office_id_val}", status_code=302)
    if search_url_val:
        page_id = db_offices.get_source_page_id_by_url(search_url_val)
        if page_id is not None:
            offices_on_page = db_offices.list_offices_for_page(page_id)
            first_office_id = offices_on_page[0]["id"] if offices_on_page else None
            if first_office_id:
                return RedirectResponse(f"/offices/{first_office_id}", status_code=302)

    if db_offices.use_hierarchy():
        # Parse limit: "20", "50", "100", "all" or missing -> int or None
        limit_int: "int | None" = None
        if limit and limit.strip().lower() != "all":
            try:
                limit_int = int(limit.strip())
            except ValueError:
                pass
        enabled_int: "int | None" = None
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
            request, "offices.html",
            {
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
                "search_url": search_url_val,
                "search_office_id": search_office_id_val,
            },
        )

    offices = db_offices.list_offices()
    counts = db_office_terms.get_terms_counts_by_office()
    for o in offices:
        o["terms_count"] = counts.get(o["id"], 0)
    return templates.TemplateResponse(
        request, "offices.html",
        {
            "page_search_view": False,
            "offices": offices,
            "pages": [],
            "saved": saved,
            "validation_error": validation_error,
            "imported": imported,
            "imported_count": imported_count,
            "imported_errors": imported_errors,
            "search_url": search_url_val,
            "search_office_id": search_office_id_val,
        },
    )


@router.get("/offices/new", response_class=HTMLResponse)
async def office_new(request: Request):
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        request, "page_form.html",
        {"office": None, "countries": countries, "levels": levels, "branches": branches, "states": [], "nav_ids": "", "nav_prev_id": None, "nav_next_id": None, "terms_count": 0, "form_template": "page_form"},
    )


@router.post("/offices/new")
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
            request, "page_form.html",
            {
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
                request, "page_form.html",
                {
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
            request, "page_form.html",
            {
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

@router.get("/offices/import", response_class=HTMLResponse)
async def offices_import_page(request: Request):
    # #region agent log
    try:
        with open(ROOT / ".cursor" / "debug.log", "a", encoding="utf-8") as _f:
            _f.write('{"id":"import_page","timestamp":' + str(int(__import__("time").time() * 1000)) + ',"location":"main.py:offices_import_page","message":"GET /offices/import handler entered","data":{"path":"/offices/import"},"hypothesisId":"A"}\n')
    except Exception:
        pass
    # #endregion
    return templates.TemplateResponse(request, "import.html")


@router.post("/offices/import")
async def offices_import(request: Request, csv_path: str = Form("")):
    if not csv_path.strip():
        return templates.TemplateResponse(request, "import.html", {"error": "Path is required"})
    path = Path(csv_path.strip())
    if not path.is_absolute():
        path = ROOT / path
    if not path.exists():
        return templates.TemplateResponse(request, "import.html", {"error": "File not found. Check the path and try again."})
    try:
        imported, errors = bulk_import_offices_from_csv(path)
        return RedirectResponse(f"/offices?imported=1&count={imported}&errors={errors}", status_code=302)
    except Exception as e:
        return templates.TemplateResponse(request, "import.html", {"error": str(e)})


@router.post("/offices/add-office-to-page")
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


@router.post("/pages/{source_page_id}/delete")
async def page_delete(source_page_id: int):
    """Delete page and all its offices. Redirect to /offices. Confirmation must be done in UI (onsubmit confirm)."""
    db_offices.delete_page(source_page_id)
    return RedirectResponse("/offices", status_code=302)


@router.post("/api/pages/{source_page_id}/enabled")
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


@router.post("/pages/{source_page_id}")
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
        "disable_auto_table_update": form.get("disable_auto_table_update") == "1",
    }
    try:
        _validate_level_state_city(page_data.get("level_id"), page_data.get("state_id"), page_data.get("city_id"), page_data.get("branch_id"))
        db_offices.update_page(source_page_id, page_data)
    except ValueError as e:
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


@router.get("/api/export-config")
async def api_export_config():
    """Return full hierarchy for all pages (each page with offices, alt_links, tables) as JSON download."""
    data = db_offices.get_full_export()
    return Response(
        content=json.dumps(data, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="office-config-export.json"'},
    )


@router.get("/api/pages/{source_page_id}/export-config")
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


@router.get("/offices/{office_id}", response_class=HTMLResponse)
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
        request, "page_form.html",
        {"office": office, "offices_on_page": offices_on_page, "source_page_id": source_page_id, "page_data": page_data, "countries": countries, "levels": levels, "branches": branches, "states": states, "cities": cities, "nav_ids": nav_ids_raw, "nav_prev_id": nav_prev_id, "nav_next_id": nav_next_id, "nav_current": nav_current, "nav_total": nav_total, "list_return_query": list_return_query, "terms_count": terms_count, "saved": saved, "page_saved": page_saved, "validation_error": validation_error, "form_template": "page_form", "office_categories": office_categories, "infobox_role_key_filters": infobox_role_key_filters},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@router.post("/offices/{office_id}")
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
        expected_role_keys: dict = {}
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

        actual_role_keys: dict = {}
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


@router.post("/offices/{office_id}/delete")
async def office_delete(office_id: int):
    db_offices.delete_office(office_id)
    return RedirectResponse("/offices", status_code=302)


@router.post("/offices/{office_id}/table/{tc_id}/delete")
async def table_delete(
    office_id: int,
    tc_id: int,
    return_query: str = Form(""),
):
    """Delete one table config. Redirect back to office edit. Confirmation must be done in UI."""
    try:
        db_offices.delete_table(tc_id)
    except ValueError as e:
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
            params = parse_qsl(q, keep_blank_values=True)
            params = [(k, v) for k, v in params if k.lower() != "saved"]
            if params:
                url += "&" + urlencode(params)
    return RedirectResponse(url, status_code=302)


@router.post("/offices/{office_id}/table/{tc_id}/move")
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
        q = return_query.strip().lstrip("?")
        if q:
            params = parse_qsl(q, keep_blank_values=True)
            params = [(k, v) for k, v in params if k.lower() != "saved"]
            if params:
                redirect_url += "&" + urlencode(params)
    return JSONResponse({"redirect": redirect_url})


@router.post("/offices/{office_id}/duplicate")
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
        return RedirectResponse("/offices/" + str(office_id) + "?error=" + quote(str(e)), status_code=302)
    return RedirectResponse(f"/offices/{new_id}?saved=1", status_code=302)


@router.post("/api/offices/{office_id}/enabled")
async def api_office_enabled(office_id: int, enabled: int = Form(1)):
    db_offices.set_office_enabled(office_id, enabled == 1)
    return JSONResponse({"ok": True})


@router.post("/api/offices/enabled-all")
async def api_offices_enabled_all(enabled: int = Form(1)):
    db_offices.set_all_offices_enabled(enabled == 1)
    return JSONResponse({"ok": True, "enabled": enabled})


@router.get("/api/offices/{office_id}/table-configs")
async def api_office_table_configs(office_id: int, table_no: "int | None" = None):
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


@router.post("/api/offices/{office_id}/set-infobox-role-key")
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


@router.post("/api/offices/{office_id}/set-infobox-role-key-filter")
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


@router.get("/api/table-configs/{table_config_id}")
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


@router.post("/api/table-configs/{table_config_id}/set-infobox-role-key")
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


@router.post("/api/table-configs/{table_config_id}/set-infobox-role-key-filter")
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
            }}
    )


@router.get("/api/offices/{office_id}/test-config")
async def api_office_test_config(office_id: int):
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404, detail="Office not found")
    ok, message = test_office_config(office)
    return JSONResponse({"ok": ok, "message": message})


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


@router.post("/api/offices/{office_id}/populate-terms")
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


@router.get("/api/offices/{office_id}/populate-terms/status/{job_id}")
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


@router.post("/api/offices/{office_id}/find-matching-table")
async def api_office_find_matching_table(office_id: int, request: Request):
    """Find a better matching table_no for this office/table by comparing against existing terms.
    Body (optional): {"office_table_config_id": int, "confirm": bool}
    """
    office = db_offices.get_office(office_id)
    if not office:
        raise HTTPException(status_code=404, detail="Office not found")
    try:
        body = await request.json()
    except Exception:
        body = {}
    requested_tc_id = int(body.get("office_table_config_id") or 0) or None
    confirm = body.get("confirm") in (True, 1, "true", "1")

    target_tc_id = requested_tc_id
    if not target_tc_id:
        unit_ids = db_offices.get_runnable_unit_ids_for_office(office_id) or [office_id]
        target_tc_id = int(unit_ids[0])
    office_row = db_offices.get_office_by_table_config_id(int(target_tc_id))
    if not office_row:
        raise HTTPException(status_code=404, detail="Table config not found")

    existing_terms = db_office_terms.get_existing_terms_for_office(int(target_tc_id))
    if not existing_terms:
        return JSONResponse({"ok": False, "message": "No existing terms to compare."})

    search = find_best_matching_table_for_existing_terms(office_row, existing_terms)
    found_table_no = search.get("found_table_no")
    if not found_table_no:
        return JSONResponse({
            "ok": False,
            "message": "No better matching table was found on this page.",
            "missing_before": search.get("missing_before"),
        })

    if confirm:
        with get_connection() as conn:
            conn.execute(
                "UPDATE office_table_config SET table_no = ?, updated_at = datetime('now') WHERE id = ?",
                (int(found_table_no), int(target_tc_id)),
            )
            conn.commit()
        return JSONResponse({
            "ok": True,
            "updated": True,
            "table_no": int(found_table_no),
            "message": f"Updated table number to {int(found_table_no)}.",
            "missing_before": search.get("missing_before"),
            "missing_after": search.get("missing_after"),
            "missing_after_labels": search.get("missing_labels_after") or [],
        })

    return JSONResponse({
        "ok": True,
        "updated": False,
        "table_no": int(found_table_no),
        "message": f"Found a better matching table: {int(found_table_no)}. Confirm to use it?",
        "missing_before": search.get("missing_before"),
        "missing_after": search.get("missing_after"),
        "missing_after_labels": search.get("missing_labels_after") or [],
    })


@router.post("/api/offices/{office_id}/populate-terms/cancel/{job_id}")
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


@router.post("/api/offices/test-config")
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
