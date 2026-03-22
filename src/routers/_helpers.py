"""Shared helper functions used across multiple routers."""

from src.db import infobox_role_key_filter as db_infobox_role_key_filter
from src.db import refs as db_refs


def _validate_infobox_role_key_filter_id(filter_id) -> "int | None":
    """Normalize and validate optional infobox role-key filter id."""
    if filter_id is None:
        return None
    raw = str(filter_id).strip()
    if not raw:
        return None
    try:
        fid = int(raw)
    except (TypeError, ValueError) as e:
        raise ValueError("Infobox role key filter must be an integer") from e
    if fid <= 0:
        return None
    if not db_infobox_role_key_filter.get_infobox_role_key_filter(fid):
        raise ValueError(f"Infobox role key filter {fid} was not found")
    return fid


def _resolve_infobox_role_key_from_filter_id(filter_id) -> str:
    """Resolve filter id to role_key text; return empty string when missing/unset."""
    try:
        fid = _validate_infobox_role_key_filter_id(filter_id)
    except ValueError:
        return ""
    if not fid:
        return ""
    f = db_infobox_role_key_filter.get_infobox_role_key_filter(fid)
    if not f:
        return ""
    return (f.get("role_key") or "").strip()


def _parse_optional_int(value) -> "int | None":
    """Parse query param to int; treat None or empty string as None."""
    if value is None or not str(value).strip():
        return None
    try:
        n = int(str(value).strip())
        return n if n != 0 else None
    except ValueError:
        return None


def _office_draft_from_body(body: dict, *, include_ref_names: bool = False) -> dict:
    """Build office draft dict from JSON body. If include_ref_names, add country_name, level_name, branch_name, state_name from db_refs."""
    term_dates_merged = body.get("term_dates_merged") in (True, 1, "1", "true", "TRUE")
    party_ignore = body.get("party_ignore") in (True, 1, "1", "true", "TRUE")
    district_ignore = body.get("district_ignore") in (True, 1, "1", "true", "TRUE")
    district_at_large = body.get("district_at_large") in (True, 1, "1", "true", "TRUE")
    term_start = int(body.get("term_start_column") or 4)
    term_end = int(body.get("term_end_column") or 5) if not term_dates_merged else term_start
    draft = {
        "url": (body.get("url") or "").strip(),
        "name": (body.get("name") or "").strip(),
        "department": (body.get("department") or "").strip(),
        "notes": (body.get("notes") or "").strip(),
        "table_no": int(body.get("table_no") or 1),
        "table_rows": int(body.get("table_rows") or 4),
        "link_column": int(body.get("link_column") or 1),
        "party_column": int(body.get("party_column") or 0),
        "term_start_column": term_start,
        "term_end_column": term_end,
        "district_column": int(body.get("district_column") or 0),
        "filter_column": int(body.get("filter_column") or 0),
        "filter_criteria": (body.get("filter_criteria") or "").strip(),
        "dynamic_parse": body.get("dynamic_parse", True),
        "read_right_to_left": body.get("read_right_to_left", False),
        "find_date_in_infobox": body.get("find_date_in_infobox", False),
        "years_only": body.get("years_only", False),
        "parse_rowspan": body.get("parse_rowspan", False),
        "consolidate_rowspan_terms": body.get("consolidate_rowspan_terms", False),
        "rep_link": body.get("rep_link", False),
        "party_link": body.get("party_link", False),
        "alt_links": (
            body.get("alt_links")
            if isinstance(body.get("alt_links"), list)
            else (
                [(body.get("alt_link") or "").strip()]
                if (body.get("alt_link") or "").strip()
                else []
            )
        ),
        "alt_link_include_main": body.get("alt_link_include_main", False),
        "use_full_page_for_table": body.get("use_full_page_for_table", False),
        "term_dates_merged": term_dates_merged,
        "party_ignore": party_ignore,
        "district_ignore": district_ignore,
        "district_at_large": district_at_large,
        "ignore_non_links": body.get("ignore_non_links") in (True, 1, "1", "true", "TRUE"),
        "remove_duplicates": body.get("remove_duplicates") in (True, 1, "1", "true", "TRUE"),
        "infobox_role_key_filter_id": _validate_infobox_role_key_filter_id(
            body.get("infobox_role_key_filter_id")
        ),
        "office_table_config_id": int(body.get("office_table_config_id") or 0) or None,
    }
    draft["infobox_role_key"] = (
        body.get("infobox_role_key") or ""
    ).strip() or _resolve_infobox_role_key_from_filter_id(draft.get("infobox_role_key_filter_id"))
    if include_ref_names:
        country_id = int(body.get("country_id") or 0)
        draft["country_name"] = db_refs.get_country_name(country_id)
        draft["level_name"] = db_refs.get_level_name(int(body.get("level_id") or 0) or None)
        draft["branch_name"] = db_refs.get_branch_name(int(body.get("branch_id") or 0) or None)
        draft["state_name"] = db_refs.get_state_name(int(body.get("state_id") or 0) or None)
    return draft
