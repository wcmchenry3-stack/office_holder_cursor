from src.routers import offices as offices_router


def test_office_draft_resolves_infobox_role_key_from_filter_id(monkeypatch):
    monkeypatch.setattr(
        offices_router.db_infobox_role_key_filter,
        "get_infobox_role_key_filter",
        lambda fid: {"id": fid, "role_key": '"judge" -"chief judge"'} if int(fid) == 7 else None,
    )

    body = {
        "country_id": 1,
        "url": "https://en.wikipedia.org/wiki/Example",
        "name": "Judge",
        "infobox_role_key_filter_id": 7,
        "find_date_in_infobox": True,
    }

    draft = offices_router._office_draft_from_body(body, include_ref_names=False)
    assert draft["infobox_role_key_filter_id"] == 7
    assert draft["infobox_role_key"] == '"judge" -"chief judge"'


def test_office_draft_prefers_explicit_infobox_role_key_over_filter(monkeypatch):
    monkeypatch.setattr(
        offices_router.db_infobox_role_key_filter,
        "get_infobox_role_key_filter",
        lambda fid: {"id": fid, "role_key": '"judge" -"chief judge"'},
    )

    body = {
        "country_id": 1,
        "url": "https://en.wikipedia.org/wiki/Example",
        "name": "Judge",
        "infobox_role_key_filter_id": 7,
        "infobox_role_key": '"associate justice" -"chief justice"',
    }

    draft = offices_router._office_draft_from_body(body, include_ref_names=False)
    assert draft["infobox_role_key"] == '"associate justice" -"chief justice"'
