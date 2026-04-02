"""Data view routes (individuals, office terms, milestones, wiki drafts)."""

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from src.db import individuals as db_individuals
from src.db import offices as db_offices
from src.db import office_terms as db_office_terms
from src.db import reports as db_reports
from src.db import individual_research_sources as db_research
from src.routers._deps import templates

router = APIRouter()


@router.get("/data/individuals", response_class=HTMLResponse)
async def data_individuals(
    request: Request, limit: int = Query(100, le=500), offset: int = Query(0)
):
    individuals = db_individuals.list_individuals(limit=limit, offset=offset)
    return templates.TemplateResponse(request, "individuals.html", {"individuals": individuals})


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
