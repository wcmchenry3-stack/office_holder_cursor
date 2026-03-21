"""Data view routes (individuals, office terms, milestones)."""

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from src.db import individuals as db_individuals
from src.db import offices as db_offices
from src.db import office_terms as db_office_terms
from src.db import reports as db_reports
from src.routers._deps import templates

router = APIRouter()


@router.get("/data/individuals", response_class=HTMLResponse)
async def data_individuals(request: Request, limit: int = Query(100, le=500), offset: int = Query(0)):
    individuals = db_individuals.list_individuals(limit=limit, offset=offset)
    return templates.TemplateResponse("individuals.html", {"request": request, "individuals": individuals})


@router.get("/data/office-terms", response_class=HTMLResponse)
async def data_office_terms(request: Request, limit: int = Query(100, le=500), offset: int = Query(0), office_id: int = Query(None)):
    terms = db_office_terms.list_office_terms(limit=limit, offset=offset, office_id=office_id)
    offices = db_offices.list_offices()
    return templates.TemplateResponse("office_terms.html", {"request": request, "terms": terms, "offices": offices})


@router.get("/report/milestones", response_class=HTMLResponse)
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
