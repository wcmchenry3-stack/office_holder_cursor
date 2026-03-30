"""Party management routes."""

import tempfile
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from src.db import parties as db_parties
from src.db import refs as db_refs
from src.db.bulk_import import bulk_import_parties_from_csv
from src.routers._deps import templates

router = APIRouter()


@router.get("/parties", response_class=HTMLResponse)
async def parties_list(request: Request):
    parties = db_parties.list_parties()
    saved = request.query_params.get("saved") == "1"
    imported_count = request.query_params.get("count")
    imported_errors = request.query_params.get("errors")
    imported = request.query_params.get("imported") == "1"
    return templates.TemplateResponse(
        request,
        "parties.html",
        {
            "parties": parties,
            "saved": saved,
            "imported": imported,
            "imported_count": imported_count,
            "imported_errors": imported_errors,
        },
    )


@router.get("/parties/import", response_class=HTMLResponse)
async def parties_import_page(request: Request):
    return templates.TemplateResponse(request, "import_parties.html")


@router.post("/parties/import")
async def parties_import(
    request: Request,
    mode: str = Form("append"),
    csv_file: UploadFile = File(None),
):
    if not csv_file or not csv_file.filename:
        return templates.TemplateResponse(
            request,
            "import_parties.html",
            {"error": "Please choose a CSV file to upload.", "mode": mode},
        )
    if not csv_file.filename.lower().endswith(".csv"):
        return templates.TemplateResponse(
            request,
            "import_parties.html",
            {"error": "File must be a .csv file.", "mode": mode},
        )
    _ALLOWED_CSV_MIME = {"text/csv", "application/csv", "text/plain"}
    content_type = (csv_file.content_type or "").split(";")[0].strip().lower()
    if content_type and content_type not in _ALLOWED_CSV_MIME:
        return templates.TemplateResponse(
            request,
            "import_parties.html",
            {
                "error": (
                    f"Invalid file type '{content_type}'. "
                    "Please upload a CSV file (accepted types: text/csv, text/plain)."
                ),
                "mode": mode,
            },
        )
    try:
        content = await csv_file.read()
    except Exception as e:
        return templates.TemplateResponse(
            request,
            "import_parties.html",
            {"error": f"Could not read file: {e}", "mode": mode},
        )
    try:
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        try:
            overwrite = mode == "overwrite"
            imported, errors = bulk_import_parties_from_csv(tmp_path, overwrite=overwrite)
            return RedirectResponse(
                f"/parties?imported=1&count={imported}&errors={errors}", status_code=302
            )
        finally:
            tmp_path.unlink(missing_ok=True)
    except Exception as e:
        return templates.TemplateResponse(
            request,
            "import_parties.html",
            {"error": str(e), "mode": mode},
        )


@router.get("/parties/new", response_class=HTMLResponse)
async def party_new(request: Request):
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        request, "party_form.html", {"party": None, "countries": countries}
    )


@router.post("/parties/new")
async def party_create(
    country_id: int = Form(0),
    party_name: str = Form(""),
    party_link: str = Form(""),
):
    db_parties.create_party(
        {"country_id": country_id, "party_name": party_name, "party_link": party_link}
    )
    return RedirectResponse("/parties?saved=1", status_code=302)


@router.get("/parties/{party_id}", response_class=HTMLResponse)
async def party_edit_page(request: Request, party_id: int):
    party = db_parties.get_party(party_id)
    if not party:
        raise HTTPException(status_code=404)
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        request, "party_form.html", {"party": party, "countries": countries}
    )


@router.post("/parties/{party_id}")
async def party_update(
    party_id: int,
    country_id: int = Form(0),
    party_name: str = Form(""),
    party_link: str = Form(""),
):
    db_parties.update_party(
        party_id, {"country_id": country_id, "party_name": party_name, "party_link": party_link}
    )
    return RedirectResponse("/parties?saved=1", status_code=302)


@router.post("/parties/{party_id}/delete")
async def party_delete(party_id: int):
    db_parties.delete_party(party_id)
    return RedirectResponse("/parties", status_code=302)
