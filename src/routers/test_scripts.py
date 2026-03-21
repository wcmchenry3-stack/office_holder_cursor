# -*- coding: utf-8 -*-
"""Router: Test script CRUD and result storage."""

import threading
import uuid
from pathlib import Path

import requests
from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.routers._deps import templates
from src.db import offices as db_offices
from src.db import infobox_role_key_filter as db_infobox_role_key_filter
from src.db import test_scripts as db_test_scripts
from src.scraper.test_script_runner import run_test_script, run_test_script_from_html
from src.scraper.wiki_fetch import (
    WIKIPEDIA_REQUEST_HEADERS,
    wiki_url_to_rest_html_url,
    normalize_wiki_url,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# In-memory result store
# ---------------------------------------------------------------------------

_test_script_result_store: dict = {}
_test_script_result_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helper functions (only used in test_scripts routes)
# ---------------------------------------------------------------------------

def _slugify_fixture_name(value: str, *, fallback: str = "fixture") -> str:
    import re
    slug = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return slug or fallback


def _build_primary_fixture_rel_path(*, test_name: str, source_url: str, canonical_fixture_mode: bool) -> str:
    import re
    if canonical_fixture_mode:
        return f"fixtures/{_slugify_fixture_name(test_name, fallback='test_script')}.html"
    safe_page = re.sub(r"[^a-zA-Z0-9._-]+", "_", (source_url.split("/")[-1] or "wiki_page"))
    return f"{uuid.uuid4().hex}_{safe_page}.html"


def _snapshot_member_pages_for_test(
    *,
    source_url: str,
    config_json: dict,
    html_content: str,
    fixture_stem: str,
    canonical_fixture_mode: bool,
) -> tuple[dict, list[str], list[str], list[dict]]:
    """Return (config_with_fixtures, fetched_urls, saved_files, actual_rows) for infobox-enabled table tests."""
    import re
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
    return templates.TemplateResponse(request, "test_script_result.html", {"payload": payload, "result_id": result_id})


@router.get("/test-scripts", response_class=HTMLResponse)
async def test_scripts_page(request: Request):
    tests = db_test_scripts.list_tests()
    return templates.TemplateResponse(
        request, "test_scripts.html",
        {
            "tests": tests,
            "can_use_office_templates": db_offices.use_hierarchy(),
            "infobox_role_key_filters": db_infobox_role_key_filter.list_infobox_role_key_filters(),
        },
    )


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
