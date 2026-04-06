"""Data view routes (individuals, office terms, milestones, wiki drafts)."""

import os

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.db import individuals as db_individuals
from src.db import offices as db_offices
from src.db import office_terms as db_office_terms
from src.db import reports as db_reports
from src.db import individual_research_sources as db_research
from src.db import app_settings as db_app_settings
from src.db import scheduled_job_runs as db_job_runs
from src.db import scraper_jobs as db_scraper_jobs
from src.db import scheduler_settings as db_scheduler_settings
from src.db.runner_registry import RUNNER_REGISTRY
from src.routers._deps import templates

router = APIRouter()


@router.get("/reports", response_class=HTMLResponse)
async def reports_landing(request: Request):
    return templates.TemplateResponse(request, "reports.html", {})


@router.get("/operations", response_class=HTMLResponse)
async def operations_landing(request: Request):
    return templates.TemplateResponse(request, "operations.html", {})


@router.get("/data/individuals", response_class=HTMLResponse)
async def data_individuals(
    request: Request,
    limit: int = Query(100, le=500),
    offset: int = Query(0),
    q: str | None = Query(None),
    is_living: int | None = Query(None),
    is_dead_link: int | None = Query(None),
):
    individuals = db_individuals.list_individuals(
        limit=limit, offset=offset, q=q, is_living=is_living, is_dead_link=is_dead_link
    )
    return templates.TemplateResponse(
        request,
        "individuals.html",
        {
            "individuals": individuals,
            "q": q or "",
            "is_living": is_living,
            "is_dead_link": is_dead_link,
            "limit": limit,
            "offset": offset,
        },
    )


@router.get("/data/office-terms", response_class=HTMLResponse)
async def data_office_terms(
    request: Request,
    limit: int = Query(100, le=500),
    offset: int = Query(0),
    office_id: int = Query(None),
):
    terms = db_office_terms.list_office_terms(limit=limit, offset=offset, office_id=office_id)
    offices = db_offices.list_offices()
    return templates.TemplateResponse(
        request, "office_terms.html", {"terms": terms, "offices": offices}
    )


@router.get("/data/wiki-drafts", response_class=HTMLResponse)
async def data_wiki_drafts(
    request: Request,
    status: str = Query(None),
):
    drafts = db_research.list_wiki_draft_proposals(status=status)
    return templates.TemplateResponse(
        request, "wiki_drafts.html", {"drafts": drafts, "status_filter": status}
    )


@router.get("/data/wiki-drafts/{proposal_id}", response_class=HTMLResponse)
async def data_wiki_draft_detail(request: Request, proposal_id: int):
    draft = db_research.get_wiki_draft_proposal(proposal_id)
    if draft is None:
        from fastapi.responses import RedirectResponse

        return RedirectResponse("/data/wiki-drafts")
    sources = db_research.list_sources_for_individual(draft["individual_id"])
    return templates.TemplateResponse(
        request, "wiki_draft_detail.html", {"draft": draft, "sources": sources}
    )


_VALID_DRAFT_STATUSES = {"pending", "submitted", "published", "rejected"}


@router.post("/api/wiki-drafts/{proposal_id}/status")
async def api_update_draft_status(request: Request, proposal_id: int):
    """Update the status of a wiki draft proposal."""
    body = await request.json()
    new_status = (body.get("status") or "").strip().lower()
    if new_status not in _VALID_DRAFT_STATUSES:
        raise HTTPException(400, f"Invalid status. Must be one of: {_VALID_DRAFT_STATUSES}")
    draft = db_research.get_wiki_draft_proposal(proposal_id)
    if draft is None:
        raise HTTPException(404, "Draft not found")
    db_research.update_wiki_draft_proposal_status(proposal_id, new_status)
    return JSONResponse({"ok": True, "status": new_status})


@router.get("/api/research/drafts")
async def api_research_drafts(status: str = Query(None)):
    """List wiki draft proposals as JSON, optionally filtered by status."""
    drafts = db_research.list_wiki_draft_proposals(status=status)
    return JSONResponse([dict(d) for d in drafts])


@router.post("/api/research/submit/{individual_id}")
async def api_research_submit(individual_id: int):
    """Submit a reviewed wiki draft to Wikipedia via the MediaWiki Action API.

    Requires WIKIPEDIA_BOT_USERNAME + WIKIPEDIA_BOT_PASSWORD env vars.
    Returns 503 if credentials are not configured.
    """
    from src.services.wikipedia_submit import get_submitter, WikipediaSubmitError

    submitter = get_submitter()
    if submitter is None:
        raise HTTPException(503, "Wikipedia submit disabled — bot credentials not configured")

    # Find the latest pending draft for this individual
    drafts = db_research.list_wiki_draft_proposals(status="pending")
    draft = next((d for d in drafts if d["individual_id"] == individual_id), None)
    if draft is None:
        raise HTTPException(404, "No pending draft found for this individual")

    # Get the full draft text
    full_draft = db_research.get_wiki_draft_proposal(draft["id"])
    if full_draft is None:
        raise HTTPException(404, "Draft not found")

    # Derive article title from individual name
    title = full_draft.get("full_name", "")
    if not title:
        raise HTTPException(400, "Cannot determine article title — individual has no name")

    try:
        result = submitter.submit_article(
            title=title,
            wikitext=full_draft["proposal_text"],
            summary=f"New article: {title} — created from researched sources",
        )
        db_research.update_wiki_draft_proposal_status(draft["id"], "submitted")
        return JSONResponse({"ok": True, "status": "submitted", "edit": result})
    except WikipediaSubmitError as exc:
        db_research.update_wiki_draft_proposal_status(draft["id"], "rejected")
        raise HTTPException(502, f"Wikipedia submission failed: {exc}")


@router.get("/data/scheduled-job-runs", response_class=HTMLResponse)
async def data_scheduled_job_runs(
    request: Request,
    days: int = Query(90, ge=1, le=365),
):
    runs = db_job_runs.list_recent_runs(days=days)
    return templates.TemplateResponse(
        request, "scheduled_job_runs.html", {"runs": runs, "days": days}
    )


@router.get("/data/scraper-jobs", response_class=HTMLResponse)
async def data_scraper_jobs(
    request: Request,
    limit: int = Query(50, le=200),
):
    jobs = db_scraper_jobs.list_recent_jobs(limit=limit)
    return templates.TemplateResponse(request, "scraper_jobs.html", {"jobs": jobs, "limit": limit})


@router.get("/data/runner-registry", response_class=HTMLResponse)
async def data_runner_registry(request: Request):
    return templates.TemplateResponse(request, "runner_registry.html", {"runners": RUNNER_REGISTRY})


@router.get("/data/scheduled-jobs", response_class=HTMLResponse)
async def data_scheduled_jobs(request: Request):
    from src.scheduled_tasks import SCHEDULED_JOBS

    settings_map = {s["job_id"]: s for s in db_scheduler_settings.list_all_settings()}
    jobs = []
    for job in SCHEDULED_JOBS:
        job_id = job["job_id"]
        last_run = db_job_runs.get_last_run_for_job(job_id)
        paused = settings_map.get(job_id, {}).get("paused", False)
        jobs.append(
            {
                **job,
                "paused": paused,
                "last_run": last_run,
            }
        )
    runners_enabled = os.environ.get("RUNNERS_ENABLED", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    app_settings = {s["key"]: s for s in db_app_settings.list_all_settings()}
    return templates.TemplateResponse(
        request,
        "scheduled_jobs.html",
        {"jobs": jobs, "runners_enabled": runners_enabled, "app_settings": app_settings},
    )


@router.post("/api/scheduler-settings/{job_id}/pause")
async def api_pause_job(job_id: str):
    from src.db.scheduler_settings import PAUSEABLE_JOB_IDS

    if job_id not in PAUSEABLE_JOB_IDS:
        raise HTTPException(status_code=400, detail=f"Job '{job_id}' is not pauseable")
    db_scheduler_settings.set_job_paused(job_id, True)
    return JSONResponse({"job_id": job_id, "paused": True})


@router.post("/api/scheduler-settings/{job_id}/resume")
async def api_resume_job(job_id: str):
    from src.db.scheduler_settings import PAUSEABLE_JOB_IDS

    if job_id not in PAUSEABLE_JOB_IDS:
        raise HTTPException(status_code=400, detail=f"Job '{job_id}' is not pauseable")
    db_scheduler_settings.set_job_paused(job_id, False)
    return JSONResponse({"job_id": job_id, "paused": False})


_APP_SETTINGS_RANGES: dict[str, tuple[int, int]] = {
    "expiry_hours_queued": (1, 168),
    "expiry_hours_running_full": (1, 168),
    "expiry_hours_running_other": (1, 168),
    "max_queued_jobs": (1, 10),
    "cron_daily_maintenance_hour": (0, 23),
    "cron_daily_maintenance_minute": (0, 59),
    "cron_daily_delta_hour": (0, 23),
    "cron_daily_delta_minute": (0, 59),
    "cron_daily_insufficient_vitals_hour": (0, 23),
    "cron_daily_insufficient_vitals_minute": (0, 59),
    "cron_daily_gemini_research_hour": (0, 23),
    "cron_daily_gemini_research_minute": (0, 59),
    "cron_daily_page_quality_hour": (0, 23),
    "cron_daily_page_quality_minute": (0, 59),
}


@router.post("/api/app-settings/{key}")
async def api_update_app_setting(key: str, request: Request):
    from src.db.app_settings import APP_SETTINGS_DEFAULTS, set_setting

    known_keys = {row["key"] for row in APP_SETTINGS_DEFAULTS}
    if key not in known_keys:
        raise HTTPException(status_code=400, detail=f"Unknown setting key: '{key}'")

    body = await request.json()
    raw_value = body.get("value")
    if raw_value is None:
        raise HTTPException(status_code=400, detail="Missing 'value' in request body")

    # All current settings are int — coerce and validate range.
    try:
        int_value = int(raw_value)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail=f"Value must be an integer, got: {raw_value!r}")

    lo, hi = _APP_SETTINGS_RANGES.get(key, (None, None))
    if lo is not None and not (lo <= int_value <= hi):
        raise HTTPException(
            status_code=400, detail=f"Value {int_value} out of range [{lo}, {hi}] for '{key}'"
        )

    set_setting(key, str(int_value))
    row = db_app_settings.list_all_settings()
    updated = next(
        (r for r in row if r["key"] == key),
        {"key": key, "value": str(int_value), "updated_at": None},
    )
    return JSONResponse(
        {"key": updated["key"], "value": updated["value"], "updated_at": updated["updated_at"]}
    )


@router.get("/report/milestones", response_class=HTMLResponse)
async def report_milestones(request: Request):
    recent_deaths = db_reports.get_recent_deaths()
    recent_term_ends = db_reports.get_recent_term_ends()
    recent_term_starts = db_reports.get_recent_term_starts()
    return templates.TemplateResponse(
        request,
        "milestone_report.html",
        {
            "recent_deaths": recent_deaths,
            "recent_term_ends": recent_term_ends,
            "recent_term_starts": recent_term_starts,
        },
    )
