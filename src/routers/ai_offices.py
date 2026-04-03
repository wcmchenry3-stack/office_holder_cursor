# -*- coding: utf-8 -*-
"""Router: AI-assisted batch office creation."""

import logging
import os
import threading
import time
import uuid

import openai
import sentry_sdk
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.db import refs as db_refs
from src.routers._deps import templates, limiter
from src.services.orchestrator import (
    validate_and_normalize_wiki_url,
    get_ai_builder,
    reset_ai_builder,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# In-memory batch job store (same pattern as preview.py)
# ---------------------------------------------------------------------------

_batch_job_store: dict = {}
_batch_job_lock = threading.Lock()
_JOB_MAX_AGE_SECONDS = 4 * 3600  # 4 hours — batch jobs run longer


def _evict_old_batch_jobs() -> None:
    cutoff = time.monotonic() - _JOB_MAX_AGE_SECONDS
    with _batch_job_lock:
        stale = [
            jid
            for jid, job in _batch_job_store.items()
            if job.get("status") not in ("running",) and job.get("_created_at", 0) < cutoff
        ]
        for jid in stale:
            del _batch_job_store[jid]


# ---------------------------------------------------------------------------
# Worker thread
# ---------------------------------------------------------------------------


def _batch_job_worker(job_id: str, urls: list[str], batch_defaults: dict) -> None:
    """Processes URLs sequentially to avoid simultaneous Wikipedia + OpenAI rate limits."""
    sentry_sdk.set_context("batch_job", {"job_id": job_id, "total_urls": len(urls)})

    def cancel_check() -> bool:
        with _batch_job_lock:
            return _batch_job_store.get(job_id, {}).get("cancelled", False)

    try:
        builder = get_ai_builder()
    except RuntimeError as e:
        with _batch_job_lock:
            if job_id in _batch_job_store:
                _batch_job_store[job_id]["status"] = "failed"
                _batch_job_store[job_id]["error"] = str(e)
        return

    for i, url in enumerate(urls):
        if cancel_check():
            with _batch_job_lock:
                if job_id in _batch_job_store:
                    _batch_job_store[job_id]["results"][i]["status"] = "cancelled"
                    _batch_job_store[job_id]["status"] = "cancelled"
            return

        with _batch_job_lock:
            if job_id in _batch_job_store:
                _batch_job_store[job_id]["results"][i]["status"] = "running"
                _batch_job_store[job_id]["current_url_index"] = i

        try:
            url_result = builder.process_url_with_retries(
                url,
                batch_defaults,
                max_retries=5,
                cancel_check=cancel_check,
            )
        except openai.AuthenticationError as e:
            # Invalid API key — fail the entire batch immediately
            sentry_sdk.capture_exception(e)
            with _batch_job_lock:
                if job_id in _batch_job_store:
                    _batch_job_store[job_id]["results"][i]["status"] = "failed"
                    _batch_job_store[job_id]["results"][i][
                        "error"
                    ] = "OpenAI authentication failed — check OPENAI_API_KEY"
                    _batch_job_store[job_id]["status"] = "failed"
            return
        except Exception as e:
            sentry_sdk.capture_exception(e)
            url_result = {
                "url": url,
                "status": "failed",
                "offices_created": [],
                "error": str(e),
                "attempts": 0,
            }

        # Back off before the next URL if OpenAI returned a RateLimitError (HTTP 429).
        # max_completion_tokens=4096 is set on every API call to cap response size.
        error_msg = url_result.get("error") or ""
        if "RateLimitError" in error_msg or "rate limit" in error_msg.lower():
            logger.warning("OpenAI rate limit hit for %s; backing off 30 s before next URL", url)
            time.sleep(30)

        with _batch_job_lock:
            if job_id in _batch_job_store:
                _batch_job_store[job_id]["results"][i].update(url_result)

    with _batch_job_lock:
        if job_id in _batch_job_store:
            _batch_job_store[job_id]["status"] = "complete"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/ai-offices", response_class=HTMLResponse)
async def ai_offices_page(request: Request):
    """Render the AI batch office builder page."""
    _evict_old_batch_jobs()
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    states = db_refs.list_states_with_country()
    api_key_set = bool(os.environ.get("OPENAI_API_KEY", ""))
    return templates.TemplateResponse(
        request,
        "ai_offices.html",
        {
            "countries": countries,
            "levels": levels,
            "branches": branches,
            "states": states,
            "api_key_set": api_key_set,
        },
    )


@router.post("/api/ai-offices/batch")
@limiter.limit("10/minute")
async def api_ai_offices_batch_start(request: Request):
    """
    Start a batch AI office creation job.

    Body: {
      "urls": ["https://..."],
      "defaults": {
        "country_id": int,
        "level_id": int,
        "branch_id": int,
        "state_id": int | null,
        "city_id": int | null
      }
    }
    Returns: {"job_id": str, "status": "running"}
    """
    _evict_old_batch_jobs()

    try:
        get_ai_builder()  # validate key is configured before creating the job
    except RuntimeError:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY not configured")

    body = await request.json()
    urls_raw: list = body.get("urls") or []
    defaults: dict = body.get("defaults") or {}

    # Validate required fields
    urls = [u.strip() for u in urls_raw if (u or "").strip()]
    if not urls:
        raise HTTPException(status_code=400, detail="At least one URL is required")

    # Enforce URL count and SSRF limits
    _MAX_BATCH_URLS = 20
    if len(urls) > _MAX_BATCH_URLS:
        raise HTTPException(
            status_code=400, detail=f"Batch limited to {_MAX_BATCH_URLS} URLs per request"
        )
    _MAX_URL_LEN = 500
    validated_urls = []
    for u in urls:
        if len(u) > _MAX_URL_LEN:
            raise HTTPException(
                status_code=400,
                detail=f"URL exceeds maximum length of {_MAX_URL_LEN} characters",
            )
        try:
            validated_urls.append(validate_and_normalize_wiki_url(u))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    urls = validated_urls
    if not int(defaults.get("country_id") or 0):
        raise HTTPException(status_code=400, detail="defaults.country_id is required")
    if not int(defaults.get("level_id") or 0):
        raise HTTPException(status_code=400, detail="defaults.level_id is required")
    if not int(defaults.get("branch_id") or 0):
        raise HTTPException(status_code=400, detail="defaults.branch_id is required")

    batch_defaults = {
        "country_id": int(defaults["country_id"]),
        "level_id": int(defaults["level_id"]),
        "branch_id": int(defaults["branch_id"]),
        "state_id": int(defaults.get("state_id") or 0) or None,
        "city_id": int(defaults.get("city_id") or 0) or None,
    }

    job_id = str(uuid.uuid4())
    with _batch_job_lock:
        _batch_job_store[job_id] = {
            "status": "running",
            "_created_at": time.monotonic(),
            "cancelled": False,
            "current_url_index": 0,
            "total_urls": len(urls),
            "results": [
                {
                    "url": url,
                    "status": "pending",
                    "offices_created": [],
                    "error": None,
                    "attempts": 0,
                }
                for url in urls
            ],
        }

    t = threading.Thread(
        target=_batch_job_worker,
        args=(job_id, urls, batch_defaults),
        daemon=True,
    )
    t.start()

    return JSONResponse({"job_id": job_id, "status": "running"}, status_code=202)


@router.get("/api/ai-offices/batch/{job_id}/status")
async def api_ai_offices_batch_status(job_id: str):
    """Poll batch job status and per-URL results."""
    with _batch_job_lock:
        job = _batch_job_store.get(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return JSONResponse(
        {
            "status": job["status"],
            "current_url_index": job.get("current_url_index", 0),
            "total_urls": job.get("total_urls", 0),
            "results": job.get("results", []),
        }
    )


@router.post("/api/ai-offices/batch/{job_id}/cancel")
async def api_ai_offices_batch_cancel(job_id: str):
    """Cancel a running batch job."""
    with _batch_job_lock:
        job = _batch_job_store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.get("status") != "running":
            raise HTTPException(status_code=409, detail="Job is not running")
        job["cancelled"] = True

    return JSONResponse({"ok": True})
