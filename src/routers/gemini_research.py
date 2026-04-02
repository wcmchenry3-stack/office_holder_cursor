# -*- coding: utf-8 -*-
"""Router: Interactive Gemini vitals research testing page."""

import json
import logging
import threading
import time
import uuid

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.db import individuals as db_individuals
from src.db import individual_research_sources as db_research
from src.db import reference_documents as db_ref_docs
from src.routers._deps import templates, limiter

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# In-memory job store for research requests
# ---------------------------------------------------------------------------

_job_store: dict = {}
_job_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------


@router.get("/gemini-research", response_class=HTMLResponse)
async def gemini_research_page(request: Request):
    return templates.TemplateResponse(request, "gemini_research.html", {})


# ---------------------------------------------------------------------------
# API: search individuals
# ---------------------------------------------------------------------------


@router.get("/api/gemini-research/search")
async def api_search_individuals(q: str = ""):
    """Search individuals by name or wiki_url. Returns up to 20 matches."""
    if not q or len(q) < 2:
        return JSONResponse([])
    from src.db.connection import get_connection

    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, wiki_url, full_name, birth_date, death_date, is_living"
            " FROM individuals"
            " WHERE full_name LIKE %s OR wiki_url LIKE %s"
            " ORDER BY id"
            " LIMIT 20",
            (f"%{q}%", f"%{q}%"),
        )
        keys = ["id", "wiki_url", "full_name", "birth_date", "death_date", "is_living"]
        return JSONResponse([dict(zip(keys, row)) for row in cur.fetchall()])
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# API: start research job
# ---------------------------------------------------------------------------


@router.post("/api/gemini-research/run")
@limiter.limit("10/minute")
async def api_start_research(request: Request):
    """Start a Gemini research job for one individual."""
    body = await request.json()
    individual_id = body.get("individual_id")
    if not individual_id:
        raise HTTPException(400, "individual_id required")

    job_id = str(uuid.uuid4())
    with _job_lock:
        _job_store[job_id] = {
            "status": "running",
            "individual_id": individual_id,
            "phase": "starting",
            "gemini_result": None,
            "article": None,
            "error": None,
        }

    t = threading.Thread(
        target=_research_worker,
        args=(job_id, int(individual_id)),
        daemon=True,
    )
    t.start()
    return JSONResponse({"job_id": job_id}, status_code=202)


@router.get("/api/gemini-research/status/{job_id}")
async def api_research_status(job_id: str):
    """Poll research job status."""
    with _job_lock:
        job = _job_store.get(job_id)
    if job is None:
        raise HTTPException(404, "Job not found")
    return JSONResponse(job)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


def _research_worker(job_id: str, individual_id: int) -> None:
    """Run Gemini research + OpenAI polish for one individual."""
    try:
        _update_job(job_id, phase="loading context")

        # Get individual info
        from src.db.connection import get_connection

        conn = get_connection()
        try:
            cur = conn.execute(
                "SELECT id, wiki_url, full_name, birth_date, death_date,"
                " birth_place, death_place, is_living"
                " FROM individuals WHERE id = %s",
                (individual_id,),
            )
            row = cur.fetchone()
        finally:
            conn.close()

        if row is None:
            _update_job(job_id, status="error", error=f"Individual {individual_id} not found")
            return

        keys = ["id", "wiki_url", "full_name", "birth_date", "death_date",
                "birth_place", "death_place", "is_living"]
        individual = dict(zip(keys, row))

        # Get office context
        from src.scraper.runner import _get_office_context_for_individual

        office_ctx = _get_office_context_for_individual(individual_id)

        # Step 1: Gemini research
        _update_job(job_id, phase="gemini research")

        from src.services.gemini_vitals_researcher import get_gemini_researcher

        researcher = get_gemini_researcher()
        if researcher is None:
            _update_job(job_id, status="error",
                        error="GEMINI_OFFICE_HOLDER not set — Gemini research disabled")
            return

        result = researcher.research_individual(
            individual_id=individual_id,
            full_name=individual.get("full_name") or "",
            office_name=office_ctx.get("office_name", ""),
            term_dates=office_ctx.get("term_dates", ""),
            party=office_ctx.get("party", ""),
            district=office_ctx.get("district", ""),
            location=office_ctx.get("location", ""),
            level=office_ctx.get("level", ""),
            branch=office_ctx.get("branch", ""),
            wiki_url=individual.get("wiki_url") or "",
            known_birth_date=individual.get("birth_date") or "",
            known_death_date=individual.get("death_date") or "",
            known_birth_place=individual.get("birth_place") or "",
            known_death_place=individual.get("death_place") or "",
        )

        gemini_data = {
            "birth_date": result.birth_date,
            "death_date": result.death_date,
            "birth_place": result.birth_place,
            "death_place": result.death_place,
            "confidence": result.confidence,
            "biographical_notes": result.biographical_notes,
            "sources": [
                {"url": s.url, "source_type": s.source_type, "notes": s.notes}
                for s in result.sources
            ],
        }
        _update_job(job_id, phase="gemini complete", gemini_result=gemini_data)

        # Step 2: Save vitals if found
        if result.birth_date or result.death_date:
            update_data = {"wiki_url": individual["wiki_url"]}
            if result.birth_date:
                update_data["birth_date"] = result.birth_date
            if result.death_date:
                update_data["death_date"] = result.death_date
            if result.birth_place:
                update_data["birth_place"] = result.birth_place
            if result.death_place:
                update_data["death_place"] = result.death_place
            db_individuals.upsert_individual(update_data)

        # Save sources
        for src in result.sources:
            db_research.insert_research_source(
                individual_id=individual_id,
                source_url=src.url,
                source_type=src.source_type,
                found_data_json=json.dumps({
                    "birth_date": result.birth_date,
                    "death_date": result.death_date,
                    "notes": src.notes,
                }),
            )

        # Step 3: OpenAI polish
        if result.biographical_notes or result.birth_date:
            _update_job(job_id, phase="openai polish")
            try:
                from src.services.orchestrator import get_ai_builder

                ref_doc = db_ref_docs.get_reference_document("wikipedia_mos")
                guidelines = ref_doc["content"] if ref_doc else ""

                builder = get_ai_builder()
                article = builder.polish_wiki_article(
                    full_name=individual.get("full_name") or "",
                    office_name=office_ctx.get("office_name", ""),
                    term_dates=office_ctx.get("term_dates", ""),
                    party=office_ctx.get("party", ""),
                    location=office_ctx.get("location", ""),
                    research_result=result,
                    formatting_guidelines=guidelines,
                )
                if article:
                    db_research.insert_wiki_draft_proposal(
                        individual_id=individual_id,
                        proposal_text=article,
                    )
                    _update_job(job_id, article=article)
            except Exception as exc:
                logger.exception("OpenAI polish failed")
                _update_job(job_id, article=f"OpenAI polish failed: {exc}")

        # Mark checked
        db_individuals.mark_gemini_research_checked(individual_id)

        _update_job(job_id, status="complete", phase="done")

    except Exception as exc:
        logger.exception("Research worker failed for individual %d", individual_id)
        _update_job(job_id, status="error", error=str(exc))


def _update_job(job_id: str, **kwargs) -> None:
    with _job_lock:
        if job_id in _job_store:
            _job_store[job_id].update(kwargs)
