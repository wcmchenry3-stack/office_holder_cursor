"""Read-only database explorer: list tables and browse rows."""

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from src.db.connection import get_connection, is_postgres
from src.routers._deps import templates

router = APIRouter()


def _get_table_names() -> list[str]:
    """Return all table names from the public schema."""
    conn = get_connection()
    try:
        if is_postgres():
            cur = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
        else:
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


@router.get("/db", response_class=HTMLResponse, include_in_schema=False)
async def db_explorer(request: Request):
    table_names = _get_table_names()
    tables = []
    conn = get_connection()
    try:
        for name in table_names:
            cur = conn.execute(f"SELECT COUNT(*) FROM {name}")
            count = cur.fetchone()[0]
            tables.append({"name": name, "count": count})
    finally:
        conn.close()
    return templates.TemplateResponse(request, "db_explorer.html", {"tables": tables})


@router.get("/db/{table}", response_class=HTMLResponse, include_in_schema=False)
async def db_table(
    request: Request,
    table: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    valid_tables = _get_table_names()
    if table not in valid_tables:
        raise HTTPException(status_code=404, detail="Table not found")

    conn = get_connection()
    try:
        if is_postgres():
            cur = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position",
                (table,),
            )
            columns = [row[0] for row in cur.fetchall()]
        else:
            cur = conn.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cur.fetchall()]

        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        total = cur.fetchone()[0]

        cur = conn.execute(f"SELECT * FROM {table} LIMIT %s OFFSET %s", (limit, offset))
        rows = [list(row) for row in cur.fetchall()]
    finally:
        conn.close()

    return templates.TemplateResponse(
        request,
        "db_table.html",
        {
            "table": table,
            "columns": columns,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
            "prev_offset": max(0, offset - limit),
            "next_offset": offset + limit,
            "has_prev": offset > 0,
            "has_next": offset + limit < total,
        },
    )
