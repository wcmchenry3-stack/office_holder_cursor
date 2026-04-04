# -*- coding: utf-8 -*-
"""AI decisions dashboard router (Issue #221).

Read-only page at /data/ai-decisions showing a unified view of all AI-driven
actions across data_quality_reports, parse_error_reports, page_quality_checks,
and suspect_record_flags.
"""

from __future__ import annotations

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from src.db import ai_decisions as db_ai
from src.routers._deps import templates

router = APIRouter()

_PAGE_SIZE = 100

_VALID_TYPES = {"data_quality", "parse_error", "page_quality", "suspect_flag"}
_VALID_RESULTS = {
    "ok",
    "reparse_ok",
    "gh_issue",
    "manual_review",
    "allowed",
    "skipped",
}


@router.get("/data/ai-decisions", response_class=HTMLResponse)
async def data_ai_decisions(
    request: Request,
    type: str = Query(None),
    result: str = Query(None),
    offset: int = Query(0, ge=0),
):
    decision_type = type if type in _VALID_TYPES else None
    result_filter = result if result in _VALID_RESULTS else None

    rows = db_ai.list_ai_decisions(
        decision_type=decision_type,
        result=result_filter,
        offset=offset,
        limit=_PAGE_SIZE,
    )
    total = db_ai.count_ai_decisions(
        decision_type=decision_type,
        result=result_filter,
    )

    prev_offset = max(0, offset - _PAGE_SIZE) if offset > 0 else None
    next_offset = offset + _PAGE_SIZE if offset + _PAGE_SIZE < total else None

    return templates.TemplateResponse(
        request,
        "ai_decisions.html",
        {
            "rows": rows,
            "total": total,
            "offset": offset,
            "page_size": _PAGE_SIZE,
            "type_filter": decision_type,
            "result_filter": result_filter,
            "prev_offset": prev_offset,
            "next_offset": next_offset,
            "valid_types": sorted(_VALID_TYPES),
            "valid_results": sorted(_VALID_RESULTS),
        },
    )
