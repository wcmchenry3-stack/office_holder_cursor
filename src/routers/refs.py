"""Reference data routes: countries, states, cities, levels, branches, office categories,
infobox role key filters, and dropdown API endpoints."""

from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.db import infobox_role_key_filter as db_infobox_role_key_filter
from src.db import office_category as db_office_category
from src.db import refs as db_refs
from src.routers._deps import templates

router = APIRouter()


def _form_ids(form, key: str) -> list[int]:
    """Return list of int ids from form getlist(key), ignoring empty/zero."""
    raw = form.getlist(key) if hasattr(form, "getlist") else []
    ids = []
    for v in raw:
        try:
            n = int(v) if v else 0
            if n:
                ids.append(n)
        except (TypeError, ValueError):
            pass
    return ids


# ---------- Index ----------


@router.get("/refs", response_class=HTMLResponse)
async def refs_index(request: Request):
    return templates.TemplateResponse(request, "refs.html")


# ---------- Countries ----------


@router.get("/refs/countries", response_class=HTMLResponse)
async def refs_countries_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        request,
        "refs_countries.html",
        {"countries": countries, "saved": saved, "error": error},
    )


@router.get("/refs/countries/new", response_class=HTMLResponse)
async def refs_country_new(request: Request):
    return templates.TemplateResponse(request, "refs_country_form.html", {"country": None})


@router.post("/refs/countries/new")
async def refs_country_create(request: Request, name: str = Form("")):
    try:
        db_refs.create_country(name)
        return RedirectResponse("/refs/countries?saved=1", status_code=302)
    except ValueError as e:
        return templates.TemplateResponse(
            request,
            "refs_country_form.html",
            {"country": {"name": name}, "validation_error": str(e)},
        )


@router.get("/refs/countries/{country_id}", response_class=HTMLResponse)
async def refs_country_edit(request: Request, country_id: int):
    country = db_refs.get_country(country_id)
    if not country:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(request, "refs_country_form.html", {"country": country})


@router.post("/refs/countries/{country_id}")
async def refs_country_update(request: Request, country_id: int, name: str = Form("")):
    try:
        db_refs.update_country(country_id, name)
        return RedirectResponse("/refs/countries?saved=1", status_code=302)
    except ValueError as e:
        country = db_refs.get_country(country_id)
        if not country:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse(
            request,
            "refs_country_form.html",
            {"country": {**country, "name": name}, "validation_error": str(e)},
        )


@router.post("/refs/countries/{country_id}/delete")
async def refs_country_delete(country_id: int):
    try:
        db_refs.delete_country(country_id)
        return RedirectResponse("/refs/countries", status_code=302)
    except ValueError as e:
        return RedirectResponse("/refs/countries?error=" + quote(str(e)), status_code=302)


# ---------- States ----------


@router.get("/refs/states", response_class=HTMLResponse)
async def refs_states_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    states = db_refs.list_states_with_country()
    return templates.TemplateResponse(
        request,
        "refs_states.html",
        {"states": states, "saved": saved, "error": error},
    )


@router.get("/refs/states/new", response_class=HTMLResponse)
async def refs_state_new(request: Request):
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        request, "refs_state_form.html", {"state": None, "countries": countries}
    )


@router.post("/refs/states/new")
async def refs_state_create(request: Request, country_id: int = Form(0), name: str = Form("")):
    try:
        db_refs.create_state(country_id, name)
        return RedirectResponse("/refs/states?saved=1", status_code=302)
    except ValueError as e:
        countries = db_refs.list_countries()
        return templates.TemplateResponse(
            request,
            "refs_state_form.html",
            {
                "state": None,
                "countries": countries,
                "validation_error": str(e),
                "form_country_id": country_id,
                "form_name": name,
            },
        )


@router.get("/refs/states/{state_id}", response_class=HTMLResponse)
async def refs_state_edit(request: Request, state_id: int):
    state = db_refs.get_state(state_id)
    if not state:
        raise HTTPException(status_code=404)
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        request, "refs_state_form.html", {"state": state, "countries": countries}
    )


@router.post("/refs/states/{state_id}")
async def refs_state_update(
    request: Request, state_id: int, country_id: int = Form(0), name: str = Form("")
):
    try:
        db_refs.update_state(state_id, country_id, name)
        return RedirectResponse("/refs/states?saved=1", status_code=302)
    except ValueError as e:
        state = db_refs.get_state(state_id)
        if not state:
            raise HTTPException(status_code=404)
        countries = db_refs.list_countries()
        return templates.TemplateResponse(
            request,
            "refs_state_form.html",
            {
                "state": {**state, "country_id": country_id, "name": name},
                "countries": countries,
                "validation_error": str(e),
            },
        )


@router.post("/refs/states/{state_id}/delete")
async def refs_state_delete(state_id: int):
    try:
        db_refs.delete_state(state_id)
        return RedirectResponse("/refs/states", status_code=302)
    except ValueError as e:
        return RedirectResponse("/refs/states?error=" + quote(str(e)), status_code=302)


# ---------- Levels ----------


@router.get("/refs/levels", response_class=HTMLResponse)
async def refs_levels_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    levels = db_refs.list_levels()
    return templates.TemplateResponse(
        request,
        "refs_levels.html",
        {"levels": levels, "saved": saved, "error": error},
    )


@router.get("/refs/levels/new", response_class=HTMLResponse)
async def refs_level_new(request: Request):
    return templates.TemplateResponse(request, "refs_level_form.html", {"level": None})


@router.post("/refs/levels/new")
async def refs_level_create(request: Request, name: str = Form("")):
    try:
        db_refs.create_level(name)
        return RedirectResponse("/refs/levels?saved=1", status_code=302)
    except ValueError as e:
        return templates.TemplateResponse(
            request,
            "refs_level_form.html",
            {"level": {"name": name}, "validation_error": str(e)},
        )


@router.get("/refs/levels/{level_id}", response_class=HTMLResponse)
async def refs_level_edit(request: Request, level_id: int):
    level = db_refs.get_level(level_id)
    if not level:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(request, "refs_level_form.html", {"level": level})


@router.post("/refs/levels/{level_id}")
async def refs_level_update(request: Request, level_id: int, name: str = Form("")):
    try:
        db_refs.update_level(level_id, name)
        return RedirectResponse("/refs/levels?saved=1", status_code=302)
    except ValueError as e:
        level = db_refs.get_level(level_id)
        if not level:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse(
            request,
            "refs_level_form.html",
            {"level": {**level, "name": name}, "validation_error": str(e)},
        )


@router.post("/refs/levels/{level_id}/delete")
async def refs_level_delete(level_id: int):
    try:
        db_refs.delete_level(level_id)
        return RedirectResponse("/refs/levels", status_code=302)
    except ValueError as e:
        return RedirectResponse("/refs/levels?error=" + quote(str(e)), status_code=302)


# ---------- Branches ----------


@router.get("/refs/branches", response_class=HTMLResponse)
async def refs_branches_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        request,
        "refs_branches.html",
        {"branches": branches, "saved": saved, "error": error},
    )


@router.get("/refs/branches/new", response_class=HTMLResponse)
async def refs_branch_new(request: Request):
    return templates.TemplateResponse(request, "refs_branch_form.html", {"branch": None})


@router.post("/refs/branches/new")
async def refs_branch_create(request: Request, name: str = Form("")):
    try:
        db_refs.create_branch(name)
        return RedirectResponse("/refs/branches?saved=1", status_code=302)
    except ValueError as e:
        return templates.TemplateResponse(
            request,
            "refs_branch_form.html",
            {"branch": {"name": name}, "validation_error": str(e)},
        )


@router.get("/refs/branches/{branch_id}", response_class=HTMLResponse)
async def refs_branch_edit(request: Request, branch_id: int):
    branch = db_refs.get_branch(branch_id)
    if not branch:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(request, "refs_branch_form.html", {"branch": branch})


@router.post("/refs/branches/{branch_id}")
async def refs_branch_update(request: Request, branch_id: int, name: str = Form("")):
    try:
        db_refs.update_branch(branch_id, name)
        return RedirectResponse("/refs/branches?saved=1", status_code=302)
    except ValueError as e:
        branch = db_refs.get_branch(branch_id)
        if not branch:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse(
            request,
            "refs_branch_form.html",
            {"branch": {**branch, "name": name}, "validation_error": str(e)},
        )


@router.post("/refs/branches/{branch_id}/delete")
async def refs_branch_delete(branch_id: int):
    try:
        db_refs.delete_branch(branch_id)
        return RedirectResponse("/refs/branches", status_code=302)
    except ValueError as e:
        return RedirectResponse("/refs/branches?error=" + quote(str(e)), status_code=302)


# ---------- Cities ----------


@router.get("/refs/cities", response_class=HTMLResponse)
async def refs_cities_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    cities = db_refs.list_cities_with_country_state()
    return templates.TemplateResponse(
        request,
        "refs_cities.html",
        {"cities": cities, "saved": saved, "error": error},
    )


@router.get("/refs/cities/new", response_class=HTMLResponse)
async def refs_city_new(request: Request):
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        request, "refs_city_form.html", {"city": None, "states": [], "countries": countries}
    )


@router.post("/refs/cities/new")
async def refs_city_create(request: Request, state_id: int = Form(0), name: str = Form("")):
    try:
        db_refs.create_city(state_id, name)
        return RedirectResponse("/refs/cities?saved=1", status_code=302)
    except ValueError as e:
        countries = db_refs.list_countries()
        state_row = db_refs.get_state(state_id) if state_id else None
        form_country_id = state_row.get("country_id") if state_row else None
        states = db_refs.list_states(form_country_id) if form_country_id else []
        return templates.TemplateResponse(
            request,
            "refs_city_form.html",
            {
                "city": None,
                "states": states,
                "countries": countries,
                "validation_error": str(e),
                "form_state_id": state_id,
                "form_name": name,
                "form_country_id": form_country_id,
            },
        )


@router.get("/refs/cities/{city_id}", response_class=HTMLResponse)
async def refs_city_edit(request: Request, city_id: int):
    city = db_refs.get_city(city_id)
    if not city:
        raise HTTPException(status_code=404)
    state_row = db_refs.get_state(city["state_id"]) if city.get("state_id") else None
    form_country_id = state_row.get("country_id") if state_row else None
    states = db_refs.list_states(form_country_id) if form_country_id else []
    countries = db_refs.list_countries()
    return templates.TemplateResponse(
        request,
        "refs_city_form.html",
        {
            "city": city,
            "states": states,
            "countries": countries,
            "form_country_id": form_country_id,
        },
    )


@router.post("/refs/cities/{city_id}")
async def refs_city_update(
    request: Request, city_id: int, state_id: int = Form(0), name: str = Form("")
):
    try:
        db_refs.update_city(city_id, state_id, name)
        return RedirectResponse("/refs/cities?saved=1", status_code=302)
    except ValueError as e:
        city = db_refs.get_city(city_id)
        if not city:
            raise HTTPException(status_code=404)
        state_row = db_refs.get_state(state_id) if state_id else None
        form_country_id = state_row.get("country_id") if state_row else None
        states = db_refs.list_states(form_country_id) if form_country_id else []
        countries = db_refs.list_countries()
        return templates.TemplateResponse(
            request,
            "refs_city_form.html",
            {
                "city": {**city, "state_id": state_id, "name": name},
                "states": states,
                "countries": countries,
                "form_country_id": form_country_id,
                "validation_error": str(e),
            },
        )


@router.post("/refs/cities/{city_id}/delete")
async def refs_city_delete(city_id: int):
    try:
        db_refs.delete_city(city_id)
        return RedirectResponse("/refs/cities", status_code=302)
    except ValueError as e:
        return RedirectResponse("/refs/cities?error=" + quote(str(e)), status_code=302)


# ---------- Office categories ----------


@router.get("/refs/office-categories", response_class=HTMLResponse)
async def refs_office_categories_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    categories = db_office_category.list_office_categories()
    return templates.TemplateResponse(
        request,
        "refs_office_categories.html",
        {"categories": categories, "saved": saved, "error": error},
    )


@router.get("/refs/office-categories/new", response_class=HTMLResponse)
async def refs_office_category_new(request: Request):
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        request,
        "refs_office_category_form.html",
        {"category": None, "countries": countries, "levels": levels, "branches": branches},
    )


@router.post("/refs/office-categories/new")
async def refs_office_category_create(request: Request):
    form = await request.form()
    name = (form.get("name") or "").strip()
    country_ids = _form_ids(form, "country_ids")
    level_ids = _form_ids(form, "level_ids")
    branch_ids = _form_ids(form, "branch_ids")
    try:
        db_office_category.create_office_category(name, country_ids, level_ids, branch_ids)
        return RedirectResponse("/refs/office-categories?saved=1", status_code=302)
    except ValueError as e:
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        return templates.TemplateResponse(
            request,
            "refs_office_category_form.html",
            {
                "category": None,
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "validation_error": str(e),
                "form_name": name,
                "form_country_ids": country_ids,
                "form_level_ids": level_ids,
                "form_branch_ids": branch_ids,
            },
        )


@router.get("/refs/office-categories/{category_id}", response_class=HTMLResponse)
async def refs_office_category_edit(request: Request, category_id: int):
    category = db_office_category.get_office_category(category_id)
    if not category:
        raise HTTPException(status_code=404)
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        request,
        "refs_office_category_form.html",
        {"category": category, "countries": countries, "levels": levels, "branches": branches},
    )


@router.post("/refs/office-categories/{category_id}")
async def refs_office_category_update(request: Request, category_id: int):
    form = await request.form()
    name = (form.get("name") or "").strip()
    country_ids = _form_ids(form, "country_ids")
    level_ids = _form_ids(form, "level_ids")
    branch_ids = _form_ids(form, "branch_ids")
    try:
        updated = db_office_category.update_office_category(
            category_id, name, country_ids, level_ids, branch_ids
        )
        if not updated:
            raise HTTPException(status_code=404)
        return RedirectResponse("/refs/office-categories?saved=1", status_code=302)
    except ValueError as e:
        category = db_office_category.get_office_category(category_id)
        if not category:
            raise HTTPException(status_code=404)
        category = {
            **category,
            "name": name,
            "country_ids": country_ids,
            "level_ids": level_ids,
            "branch_ids": branch_ids,
        }
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        return templates.TemplateResponse(
            request,
            "refs_office_category_form.html",
            {
                "category": category,
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "validation_error": str(e),
            },
        )


@router.post("/refs/office-categories/{category_id}/delete")
async def refs_office_category_delete(category_id: int):
    try:
        db_office_category.delete_office_category(category_id)
        return RedirectResponse("/refs/office-categories", status_code=302)
    except ValueError as e:
        return RedirectResponse("/refs/office-categories?error=" + quote(str(e)), status_code=302)


# ---------- Infobox role key filters ----------


@router.get("/refs/infobox-role-key-filters", response_class=HTMLResponse)
async def refs_infobox_role_key_filters_list(request: Request):
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    filters = db_infobox_role_key_filter.list_infobox_role_key_filters()
    return templates.TemplateResponse(
        request,
        "refs_infobox_role_key_filters.html",
        {"filters": filters, "saved": saved, "error": error},
    )


@router.get("/refs/infobox-role-key-filters/new", response_class=HTMLResponse)
async def refs_infobox_role_key_filter_new(request: Request):
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        request,
        "refs_infobox_role_key_filter_form.html",
        {"filter_obj": None, "countries": countries, "levels": levels, "branches": branches},
    )


@router.post("/refs/infobox-role-key-filters/new")
async def refs_infobox_role_key_filter_create(request: Request):
    form = await request.form()
    name = (form.get("name") or "").strip()
    role_key = (form.get("role_key") or "").strip()
    country_ids = _form_ids(form, "country_ids")
    level_ids = _form_ids(form, "level_ids")
    branch_ids = _form_ids(form, "branch_ids")
    try:
        db_infobox_role_key_filter.create_infobox_role_key_filter(
            name, role_key, country_ids, level_ids, branch_ids
        )
        return RedirectResponse("/refs/infobox-role-key-filters?saved=1", status_code=302)
    except ValueError as e:
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        return templates.TemplateResponse(
            request,
            "refs_infobox_role_key_filter_form.html",
            {
                "filter_obj": None,
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "validation_error": str(e),
                "form_name": name,
                "form_role_key": role_key,
                "form_country_ids": country_ids,
                "form_level_ids": level_ids,
                "form_branch_ids": branch_ids,
            },
        )


@router.get("/refs/infobox-role-key-filters/{filter_id}", response_class=HTMLResponse)
async def refs_infobox_role_key_filter_edit(request: Request, filter_id: int):
    filter_obj = db_infobox_role_key_filter.get_infobox_role_key_filter(filter_id)
    if not filter_obj:
        raise HTTPException(status_code=404)
    countries = db_refs.list_countries()
    levels = db_refs.list_levels()
    branches = db_refs.list_branches()
    return templates.TemplateResponse(
        request,
        "refs_infobox_role_key_filter_form.html",
        {"filter_obj": filter_obj, "countries": countries, "levels": levels, "branches": branches},
    )


@router.post("/refs/infobox-role-key-filters/{filter_id}")
async def refs_infobox_role_key_filter_update(request: Request, filter_id: int):
    form = await request.form()
    name = (form.get("name") or "").strip()
    role_key = (form.get("role_key") or "").strip()
    country_ids = _form_ids(form, "country_ids")
    level_ids = _form_ids(form, "level_ids")
    branch_ids = _form_ids(form, "branch_ids")
    try:
        updated = db_infobox_role_key_filter.update_infobox_role_key_filter(
            filter_id, name, role_key, country_ids, level_ids, branch_ids
        )
        if not updated:
            raise HTTPException(status_code=404)
        return RedirectResponse("/refs/infobox-role-key-filters?saved=1", status_code=302)
    except ValueError as e:
        filter_obj = db_infobox_role_key_filter.get_infobox_role_key_filter(filter_id)
        if not filter_obj:
            raise HTTPException(status_code=404)
        filter_obj = {
            **filter_obj,
            "name": name,
            "role_key": role_key,
            "country_ids": country_ids,
            "level_ids": level_ids,
            "branch_ids": branch_ids,
        }
        countries = db_refs.list_countries()
        levels = db_refs.list_levels()
        branches = db_refs.list_branches()
        return templates.TemplateResponse(
            request,
            "refs_infobox_role_key_filter_form.html",
            {
                "filter_obj": filter_obj,
                "countries": countries,
                "levels": levels,
                "branches": branches,
                "validation_error": str(e),
            },
        )


@router.post("/refs/infobox-role-key-filters/{filter_id}/delete")
async def refs_infobox_role_key_filter_delete(filter_id: int):
    db_infobox_role_key_filter.delete_infobox_role_key_filter(filter_id)
    return RedirectResponse("/refs/infobox-role-key-filters", status_code=302)


# ---------- Reference data dropdown API endpoints ----------


@router.get("/api/countries")
async def api_countries():
    return JSONResponse(db_refs.list_countries())


@router.get("/api/states")
async def api_states(country_id: int = Query(0)):
    if not country_id:
        return JSONResponse([])
    return JSONResponse(db_refs.list_states(country_id))


@router.get("/api/levels")
async def api_levels():
    return JSONResponse(db_refs.list_levels())


@router.get("/api/branches")
async def api_branches():
    return JSONResponse(db_refs.list_branches())


@router.get("/api/cities")
async def api_cities(state_id: int = Query(0)):
    if not state_id:
        return JSONResponse([])
    return JSONResponse(db_refs.list_cities(state_id))
