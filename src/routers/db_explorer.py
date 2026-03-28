"""Read-only SQL query interface for the database."""

import re
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.db.connection import get_connection
from src.routers._deps import templates

router = APIRouter()

# Only allow SELECT and WITH (CTEs) — block anything else
_ALLOWED_STMT = re.compile(r"^\s*(select|with)\s", re.IGNORECASE | re.DOTALL)


def _get_table_names() -> list[str]:
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


@router.get("/db", response_class=HTMLResponse, include_in_schema=False)
async def db_explorer(request: Request):
    tables = _get_table_names()
    return templates.TemplateResponse(
        request, "db_explorer.html", {"tables": tables, "db_type": "PostgreSQL"}
    )


@router.post("/db/query", include_in_schema=False)
async def db_query(request: Request):
    body = await request.json()
    sql: str = (body.get("sql") or "").strip()

    if not sql:
        return JSONResponse({"error": "No query provided."}, status_code=400)

    if not _ALLOWED_STMT.match(sql):
        return JSONResponse(
            {"error": "Only SELECT (and WITH … SELECT) queries are allowed."}, status_code=400
        )

    conn = get_connection()
    try:
        t0 = time.monotonic()
        cur = conn.execute(sql)
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = [list(row) for row in cur.fetchall()]
        return JSONResponse(
            {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "elapsed_ms": elapsed_ms,
            }
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    finally:
        conn.close()
