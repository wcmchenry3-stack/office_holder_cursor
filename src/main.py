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

import httpx
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
from starlette.middleware.sessions import SessionMiddleware
from authlib.integrations.starlette_client import OAuth

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
from src.scraper.runner import run_with_db, preview_with_config, parse_full_table_for_export, find_best_matching_table_for_existing_terms
from src.scraper.config_test import test_office_config, get_raw_table_preview, get_all_tables_preview, get_table_html, get_table_header_from_html
from src.scraper.test_script_runner import run_test_script, run_test_script_from_html
from src.scraper.wiki_fetch import WIKIPEDIA_REQUEST_HEADERS, wiki_url_to_rest_html_url, normalize_wiki_url
from src.routers import refs as refs_router
from src.routers import parties as parties_router
from src.routers import data as data_router
from src.routers import run_scraper as run_scraper_router
from src.routers import test_scripts as test_scripts_router
from src.routers import ui_tests as ui_tests_router
from src.routers import preview as preview_router
from src.routers import offices as offices_router

app = FastAPI(title="Office Holder")

# Google OAuth setup
_oauth = OAuth()
_oauth.register(
    name="google",
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

_ALLOWED_EMAIL = os.environ.get("ALLOWED_EMAIL", "")
_APP_BASE_URL = os.environ.get("APP_BASE_URL", "")
_AUTH_ENABLED = bool(os.environ.get("GOOGLE_CLIENT_ID"))

# Auth middleware — skips public paths and dev mode (no GOOGLE_CLIENT_ID set)
_PUBLIC_PATHS = {"/login", "/auth/google", "/auth/google/callback"}


@app.middleware("http")
async def require_login(request: Request, call_next):
    if not _AUTH_ENABLED:
        return await call_next(request)
    if request.url.path in _PUBLIC_PATHS or request.url.path.startswith("/static"):
        return await call_next(request)
    if not request.session.get("user_email"):
        return RedirectResponse("/login")
    return await call_next(request)


# SessionMiddleware must be added AFTER require_login so it is outermost and
# populates request.session before require_login runs.
app.add_middleware(SessionMiddleware, secret_key=os.environ.get("SECRET_KEY", "dev-only-insecure-key"))


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/auth/google")
async def auth_google(request: Request):
    redirect_uri = (_APP_BASE_URL.rstrip("/") + "/auth/google/callback") if _APP_BASE_URL else str(request.url_for("auth_google_callback"))
    return await _oauth.google.authorize_redirect(request, redirect_uri)


@app.get("/auth/google/callback", name="auth_google_callback")
async def auth_google_callback(request: Request):
    try:
        token = await _oauth.google.authorize_access_token(request)
    except Exception:
        return HTMLResponse("<h2>Authentication failed. <a href='/login'>Try again</a>.</h2>", status_code=400)
    user_info = token.get("userinfo") or {}
    email = user_info.get("email", "")
    if not email or email.lower() != _ALLOWED_EMAIL.lower():
        return HTMLResponse(
            "<h2>Access denied.</h2><p>This app is restricted to a single authorised account.</p>"
            "<p><a href='/login'>Back to login</a></p>",
            status_code=403,
        )
    request.session["user_email"] = email
    return RedirectResponse("/")


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")


# Resolve to absolute path so template dir is correct regardless of process cwd
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(refs_router.router)
app.include_router(parties_router.router)
app.include_router(data_router.router)
app.include_router(run_scraper_router.router)
app.include_router(test_scripts_router.router)
app.include_router(ui_tests_router.router)
app.include_router(preview_router.router)
app.include_router(offices_router.router)

# Stoppable process types: server-side (e.g. "run") have a cancel endpoint and job store with cancelled flag;
# client-side (e.g. "preview_all") use a Stop button and a running/stopped flag (optional AbortController).
# To add a new type: follow the same pattern (job store + cancel_check + cancel API for server-side;
# flag + Stop button for client-side) and append to this list.
PROCESS_TYPES = ["run", "preview_all"]


def _run_git_command(args: list[str]) -> str:
    """Run a git command at repo root and return stripped stdout (empty on error)."""
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
            text=True,
        )
        return (result.stdout or "").strip()
    except Exception:
        return ""


def _get_git_sync_status() -> dict:
    """Return git sync metadata for UI banner display."""
    inside_repo = _run_git_command(["rev-parse", "--is-inside-work-tree"]) == "true"
    if not inside_repo:
        return {"unsynced": False}

    branch = _run_git_command(["symbolic-ref", "--quiet", "--short", "HEAD"]) or "(detached HEAD)"
    upstream = _run_git_command(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    dirty = bool(_run_git_command(["status", "--porcelain"]))
    ahead = 0
    behind = 0

    if upstream:
        counts = _run_git_command(["rev-list", "--left-right", "--count", f"HEAD...{upstream}"])
        parts = counts.split()
        if len(parts) == 2:
            try:
                behind = int(parts[0])
                ahead = int(parts[1])
            except ValueError:
                ahead = 0
                behind = 0

    unsynced = dirty or ahead > 0 or not upstream
    if not unsynced:
        return {"unsynced": False}

    if not upstream:
        message = (
            "Local changes are not synced to a remote branch yet. "
            "Create/push a feature branch (for example: git push -u origin "
            f"{branch}) because direct pushes to dev are blocked."
        )
    elif dirty:
        message = (
            "You have local edits not yet committed. Commit to this feature branch, "
            "then push to sync with remote."
        )
    else:
        message = (
            f"You are {ahead} commit(s) ahead of {upstream}. "
            "Push this feature branch to sync remote."
        )

    return {
        "unsynced": True,
        "branch": branch,
        "upstream": upstream,
        "ahead": ahead,
        "behind": behind,
        "dirty": dirty,
        "message": message,
    }


templates.env.globals["git_sync_status"] = _get_git_sync_status


@app.on_event("startup")
def startup():
    try:
        init_db()
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise RuntimeError(f"Database startup failed: {e}") from e


# ---------- Datasette DB explorer (read-only proxy) ----------

_DATASETTE_PORT = 8001
_datasette_proc: "subprocess.Popen | None" = None

# Headers that must not be forwarded from the upstream proxy response
_PROXY_SKIP_HEADERS = {
    "transfer-encoding", "connection", "keep-alive",
    "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "upgrade",
    "content-encoding",  # httpx decompresses automatically; forwarding this would break content
}

# Dark mode CSS injected into every Datasette HTML page — matches the app's existing color palette
_DATASETTE_DARK_CSS = """
<style>
/* Office Holder dark mode override for Datasette */
:root {
  --bg: #1a1b22; --bg2: #23242d; --bg3: #2c2d38;
  --text: #e6e6ea; --text-muted: #9a9ba8;
  --accent: #5c7cfa; --border: #3d3e4a; --input-bg: #23242d;
}
*, *::before, *::after { box-sizing: border-box; }
body, .nav, nav, header, footer, .not-found, .page-header { background: var(--bg) !important; color: var(--text) !important; }
.nav, nav { border-bottom: 1px solid var(--border) !important; }
a, a:visited { color: var(--accent) !important; }
a:hover { color: #748ffc !important; }
table, .rows-and-columns table { width: 100%; border-collapse: collapse; }
th, td { border: 1px solid var(--border) !important; padding: 0.4rem 0.6rem; background: var(--bg) !important; color: var(--text) !important; }
th { background: var(--bg2) !important; color: var(--text-muted) !important; }
tr:nth-child(even) td { background: var(--bg2) !important; }
tr:hover td { background: var(--bg3) !important; }
input, select, textarea, .select2-container .select2-choice, .CodeMirror {
  background: var(--input-bg) !important; color: var(--text) !important;
  border-color: var(--border) !important; border-radius: 4px;
}
.CodeMirror-gutters { background: var(--bg2) !important; border-right: 1px solid var(--border) !important; }
.CodeMirror-linenumber { color: var(--text-muted) !important; }
pre, code, .CodeMirror pre { background: var(--bg2) !important; color: var(--text) !important; }
.message, .message-info { background: var(--bg3) !important; color: var(--text) !important; border-color: var(--border) !important; }
.message-error { background: #3b1111 !important; color: #ff8080 !important; border-color: #8b2222 !important; }
button, .button, input[type=submit] {
  background: #2a2b38 !important; color: var(--text) !important;
  border: 1px solid var(--border) !important; cursor: pointer;
}
button:hover, .button:hover { background: var(--bg3) !important; }
.label-green { background: #1a3a1a !important; color: #69db7c !important; }
.label-orange { background: #3a2a10 !important; color: #ffa94d !important; }
.label-red { background: #3a1010 !important; color: #ff6b6b !important; }
.dropdown-menu, .select2-drop, .select2-results { background: var(--bg2) !important; border-color: var(--border) !important; }
.select2-results li { color: var(--text) !important; }
.select2-results li.select2-highlighted { background: var(--bg3) !important; }
</style>
"""


def _start_datasette() -> None:
    """Start Datasette as a read-only subprocess bound to localhost only."""
    global _datasette_proc
    from src.db.connection import get_db_path
    db_path = get_db_path()
    try:
        _datasette_proc = subprocess.Popen(
            [
                sys.executable, "-m", "datasette", "serve",
                "--host", "127.0.0.1",
                "--port", str(_DATASETTE_PORT),
                "--immutable", str(db_path),
                "--setting", "base_url", "/db/",
                "--setting", "sql_time_limit_ms", "600000",  # 10-minute query hard limit
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        print(f"[datasette] Failed to start: {exc}")


def _stop_datasette() -> None:
    global _datasette_proc
    if _datasette_proc is not None:
        _datasette_proc.terminate()
        _datasette_proc = None


@app.on_event("startup")
def startup_datasette():
    _start_datasette()


@app.on_event("shutdown")
def shutdown_datasette():
    _stop_datasette()


def _apply_datasette_dark_css(content: bytes, content_type: str) -> bytes:
    """Inject dark mode CSS into Datasette HTML responses. Non-HTML content is returned unchanged."""
    if "text/html" not in content_type:
        return content
    return content.replace(b"</head>", (_DATASETTE_DARK_CSS + "</head>").encode(), 1)


@app.get("/db", include_in_schema=False)
async def db_explorer_redirect():
    return RedirectResponse("/db/")


@app.get("/db/{path:path}", include_in_schema=False)
async def db_explorer_proxy(request: Request, path: str):
    """Proxy authenticated requests to the internal Datasette instance."""
    url = f"http://127.0.0.1:{_DATASETTE_PORT}/{path}"
    query = str(request.url.query)
    if query:
        url = f"{url}?{query}"
    try:
        async with httpx.AsyncClient(timeout=600.0) as client:
            resp = await client.get(url)
    except httpx.TimeoutException:
        return HTMLResponse(
            "<h2>Query timed out</h2>"
            "<p>The query exceeded the 10-minute limit and was stopped.</p>",
            status_code=504,
        )
    except httpx.ConnectError:
        return HTMLResponse(
            "<h2>DB Explorer unavailable</h2>"
            "<p>Datasette is starting up — wait a moment and refresh, "
            "or restart the app if this persists.</p>",
            status_code=503,
        )
    content_type = resp.headers.get("content-type", "")
    content = _apply_datasette_dark_css(resp.content, content_type)
    headers = {k: v for k, v in resp.headers.items() if k.lower() not in _PROXY_SKIP_HEADERS}
    return Response(
        content=content,
        status_code=resp.status_code,
        headers=headers,
        media_type=content_type or None,
    )


# ---------- Favicon (avoid 404 in console) ----------
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Return 204 so the browser's automatic favicon request doesn't 404."""
    return Response(status_code=204)


# ---------- Preview (single office) — see src/routers/preview.py ----------
