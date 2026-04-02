# -*- coding: utf-8 -*-
"""
Office Holder app: local UI and API for Wikipedia office/bio scraper.
Run: uvicorn src.main:app --reload
From project root: office_holder/
"""

import json
import os
import re
import tempfile
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
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
from starlette.middleware.sessions import SessionMiddleware
from authlib.integrations.starlette_client import OAuth
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

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
from src.scraper.runner import (
    run_with_db,
    preview_with_config,
    parse_full_table_for_export,
    find_best_matching_table_for_existing_terms,
)
from src.scraper.config_test import (
    test_office_config,
    get_raw_table_preview,
    get_all_tables_preview,
    get_table_html,
    get_table_header_from_html,
)
from src.scraper.test_script_runner import run_test_script, run_test_script_from_html
from src.scraper.wiki_fetch import (
    WIKIPEDIA_REQUEST_HEADERS,
    wiki_url_to_rest_html_url,
    normalize_wiki_url,
)
from dotenv import load_dotenv

load_dotenv(".env.local")  # loads OPENAI_API_KEY (and others) in dev; no-op if file absent
# OpenAI RateLimitError (HTTP 429) and retry backoff are handled by AIOfficeBuilder
# (src/services/ai_office_builder.py). The router (_batch_job_worker) adds an additional
# 30-second backoff sleep when a rate-limit failure is detected before continuing.
# max_completion_tokens=4096 is set on every API call to cap response size and avoid cost spikes.

from src.routers import refs as refs_router
from src.routers import parties as parties_router
from src.routers import data as data_router
from src.routers import run_scraper as run_scraper_router
from src.routers import test_scripts as test_scripts_router
from src.routers import ui_tests as ui_tests_router
from src.routers import preview as preview_router
from src.routers import offices as offices_router
from src.routers import ai_offices as ai_offices_router
from src.routers import db_explorer as db_explorer_router
from src.routers import gemini_research as gemini_research_router
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from src.routers._deps import templates, limiter
from src.scheduled_tasks import (
    is_daily_delta_enabled,
    run_daily_delta,
    run_daily_insufficient_vitals,
    run_daily_gemini_research,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_db()
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise RuntimeError(f"Database startup failed: {e}") from e
    scheduler = AsyncIOScheduler(timezone="UTC")
    if is_daily_delta_enabled():
        scheduler.add_job(run_daily_delta, "cron", hour=6, minute=0, id="daily_delta")
        print("[scheduler] Daily delta run scheduled at 06:00 UTC")
        scheduler.add_job(
            run_daily_insufficient_vitals, "cron", hour=7, minute=0, id="daily_insufficient_vitals"
        )
        print("[scheduler] Insufficient vitals recheck scheduled at 07:00 UTC")
        scheduler.add_job(
            run_daily_gemini_research, "cron", hour=8, minute=0, id="daily_gemini_research"
        )
        print("[scheduler] Gemini deep research scheduled at 08:00 UTC")
    else:
        print("[scheduler] Daily delta job is paused (DAILY_DELTA_ENABLED is disabled)")
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Office Holder", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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
        if request.url.path.startswith("/api/"):
            return JSONResponse({"detail": "Not authenticated"}, status_code=401)
        return RedirectResponse("/login")
    return await call_next(request)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; img-src 'self' data:",
    )
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    if request.url.scheme == "https":
        response.headers.setdefault(
            "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
        )
    return response


_MAX_BODY_SIZE = 1_048_576  # 1 MB


@app.middleware("http")
async def limit_request_body_size(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > _MAX_BODY_SIZE:
        return JSONResponse({"detail": "Request body too large (max 1 MB)"}, status_code=413)
    return await call_next(request)


# Middleware ordering (Starlette LIFO — last add_middleware call = outermost = runs first):
#   SessionMiddleware (outermost) → populates request.session first
#   SlowAPIMiddleware (inner) → rate-limit key function reads session email after session is set
app.add_middleware(SlowAPIMiddleware)
# SessionMiddleware must be added AFTER SlowAPIMiddleware (above) so it is outermost and
# populates request.session before the rate-limit key function runs.
app.add_middleware(
    SessionMiddleware, secret_key=os.environ.get("SECRET_KEY", "dev-only-insecure-key")
)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html")


@app.get("/auth/google")
@limiter.limit("10/minute")
async def auth_google(request: Request):
    redirect_uri = (
        (_APP_BASE_URL.rstrip("/") + "/auth/google/callback")
        if _APP_BASE_URL
        else str(request.url_for("auth_google_callback"))
    )
    return await _oauth.google.authorize_redirect(request, redirect_uri)


@app.get("/auth/google/callback", name="auth_google_callback")
@limiter.limit("10/minute")
async def auth_google_callback(request: Request):
    try:
        token = await _oauth.google.authorize_access_token(request)
    except Exception:
        return HTMLResponse(
            "<h2>Authentication failed. <a href='/login'>Try again</a>.</h2>", status_code=400
        )
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


STATIC_DIR = Path(__file__).resolve().parent / "static"
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
app.include_router(ai_offices_router.router)
app.include_router(db_explorer_router.router)
app.include_router(gemini_research_router.router)

# Stoppable process types: server-side (e.g. "run") have a cancel endpoint and job store with cancelled flag;
# client-side (e.g. "preview_all") use a Stop button and a running/stopped flag (optional AbortController).
# To add a new type: follow the same pattern (job store + cancel_check + cancel API for server-side;
# flag + Stop button for client-side) and append to this list.
PROCESS_TYPES = ["run", "preview_all"]


# ---------- Favicon (avoid 404 in console) ----------
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Return 204 so the browser's automatic favicon request doesn't 404."""
    return Response(status_code=204)


# ---------- Preview (single office) — see src/routers/preview.py ----------
