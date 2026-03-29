"""Unit tests for src/routers/_helpers.py.

Uses SQLite in-memory via init_db() — no PostgreSQL required.

Policy note: all Wikipedia HTTP requests in this application use wiki_session()
from src/scraper/wiki_fetch.py, which sets the User-Agent header and applies
retry/throttle logic (≤1 req/s, Retry-After respected) per Wikimedia policy.

Run: pytest src/test_helpers.py -v
"""

from __future__ import annotations

import importlib
import os

import pytest

from src.db.connection import get_connection, init_db
from src.db import infobox_role_key_filter as db_filter

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("helpers_db")
    path = tmp / "helpers_test.db"
    init_db(path=path)
    return path


@pytest.fixture(scope="module")
def filter_id(db_path):
    """Create a real infobox role key filter and return its id."""
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO infobox_role_key_filter (name, role_key) VALUES (%s, %s)",
            ("Test Filter", "member_of"),
        )
        row = conn.execute(
            "SELECT id FROM infobox_role_key_filter WHERE name = %s", ("Test Filter",)
        ).fetchone()
        conn.commit()
        return row[0]
    finally:
        conn.close()


@pytest.fixture(scope="module")
def helpers(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    import src.routers._helpers as h

    importlib.reload(h)
    return h


# ---------------------------------------------------------------------------
# _validate_infobox_role_key_filter_id
# ---------------------------------------------------------------------------


def test_validate_filter_id_none_returns_none(helpers):
    assert helpers._validate_infobox_role_key_filter_id(None) is None


def test_validate_filter_id_empty_string_returns_none(helpers):
    assert helpers._validate_infobox_role_key_filter_id("") is None


def test_validate_filter_id_whitespace_returns_none(helpers):
    assert helpers._validate_infobox_role_key_filter_id("   ") is None


def test_validate_filter_id_non_int_raises(helpers):
    with pytest.raises(ValueError, match="integer"):
        helpers._validate_infobox_role_key_filter_id("abc")


def test_validate_filter_id_zero_returns_none(helpers):
    assert helpers._validate_infobox_role_key_filter_id(0) is None


def test_validate_filter_id_negative_returns_none(helpers):
    assert helpers._validate_infobox_role_key_filter_id(-1) is None


def test_validate_filter_id_missing_from_db_raises(helpers):
    with pytest.raises(ValueError, match="not found"):
        helpers._validate_infobox_role_key_filter_id(999999)


def test_validate_filter_id_valid_returns_int(helpers, filter_id):
    result = helpers._validate_infobox_role_key_filter_id(filter_id)
    assert result == filter_id


def test_validate_filter_id_string_int_accepted(helpers, filter_id):
    result = helpers._validate_infobox_role_key_filter_id(str(filter_id))
    assert result == filter_id


# ---------------------------------------------------------------------------
# _resolve_infobox_role_key_from_filter_id
# ---------------------------------------------------------------------------


def test_resolve_filter_id_none_returns_empty(helpers):
    assert helpers._resolve_infobox_role_key_from_filter_id(None) == ""


def test_resolve_filter_id_zero_returns_empty(helpers):
    assert helpers._resolve_infobox_role_key_from_filter_id(0) == ""


def test_resolve_filter_id_nonexistent_returns_empty(helpers):
    assert helpers._resolve_infobox_role_key_from_filter_id(999999) == ""


def test_resolve_filter_id_invalid_string_returns_empty(helpers):
    # ValueError from validate is caught internally → empty string
    assert helpers._resolve_infobox_role_key_from_filter_id("not-an-int") == ""


def test_resolve_filter_id_valid_returns_role_key(helpers, filter_id):
    result = helpers._resolve_infobox_role_key_from_filter_id(filter_id)
    assert result == "member_of"


# ---------------------------------------------------------------------------
# _parse_optional_int
# ---------------------------------------------------------------------------


def test_parse_optional_int_none_returns_none(helpers):
    assert helpers._parse_optional_int(None) is None


def test_parse_optional_int_empty_returns_none(helpers):
    assert helpers._parse_optional_int("") is None


def test_parse_optional_int_whitespace_returns_none(helpers):
    assert helpers._parse_optional_int("  ") is None


def test_parse_optional_int_zero_returns_none(helpers):
    assert helpers._parse_optional_int(0) is None


def test_parse_optional_int_zero_string_returns_none(helpers):
    assert helpers._parse_optional_int("0") is None


def test_parse_optional_int_positive(helpers):
    assert helpers._parse_optional_int("5") == 5


def test_parse_optional_int_negative(helpers):
    assert helpers._parse_optional_int("-3") == -3


def test_parse_optional_int_invalid_string_returns_none(helpers):
    assert helpers._parse_optional_int("abc") is None


def test_parse_optional_int_int_arg(helpers):
    assert helpers._parse_optional_int(7) == 7


# ---------------------------------------------------------------------------
# _office_draft_from_body — basic structure
# ---------------------------------------------------------------------------


_MINIMAL_BODY = {
    "name": "Test Office",
    "url": "https://en.wikipedia.org/wiki/Test",
    "country_id": "1",
}


def test_office_draft_name_stripped(helpers):
    draft = helpers._office_draft_from_body({"name": "  My Office  ", "url": "https://x.com"})
    assert draft["name"] == "My Office"


def test_office_draft_url_stripped(helpers):
    draft = helpers._office_draft_from_body({"name": "x", "url": "  https://x.com  "})
    assert draft["url"] == "https://x.com"


def test_office_draft_defaults(helpers):
    draft = helpers._office_draft_from_body({})
    assert draft["table_no"] == 1
    assert draft["table_rows"] == 4
    assert draft["link_column"] == 1
    assert draft["party_column"] == 0
    assert draft["term_start_column"] == 4
    assert draft["term_end_column"] == 5


def test_office_draft_term_dates_merged_collapses_end_column(helpers):
    draft = helpers._office_draft_from_body({"term_dates_merged": True, "term_start_column": "3"})
    assert draft["term_dates_merged"] is True
    assert draft["term_end_column"] == draft["term_start_column"]


def test_office_draft_boolean_flag_truthy_variants(helpers):
    """All truthy variants (True, 1, "1", "true", "TRUE") must produce True."""
    for value in (True, 1, "1", "true", "TRUE"):
        draft = helpers._office_draft_from_body({"party_ignore": value})
        assert draft["party_ignore"] is True, f"Expected True for {value!r}"


def test_office_draft_boolean_flag_falsy(helpers):
    for value in (False, 0, "0", "false", "", None):
        draft = helpers._office_draft_from_body({"party_ignore": value})
        assert draft["party_ignore"] is False, f"Expected False for {value!r}"


def test_office_draft_district_ignore(helpers):
    draft = helpers._office_draft_from_body({"district_ignore": "1"})
    assert draft["district_ignore"] is True


def test_office_draft_district_at_large(helpers):
    draft = helpers._office_draft_from_body({"district_at_large": True})
    assert draft["district_at_large"] is True


def test_office_draft_alt_links_list_passthrough(helpers):
    links = ["https://a.com", "https://b.com"]
    draft = helpers._office_draft_from_body({"alt_links": links})
    assert draft["alt_links"] == links


def test_office_draft_alt_link_scalar_wrapped(helpers):
    draft = helpers._office_draft_from_body({"alt_link": "https://c.com"})
    assert draft["alt_links"] == ["https://c.com"]


def test_office_draft_alt_links_empty_when_missing(helpers):
    draft = helpers._office_draft_from_body({})
    assert draft["alt_links"] == []


def test_office_draft_infobox_role_key_explicit(helpers):
    draft = helpers._office_draft_from_body({"infobox_role_key": " senator "})
    assert draft["infobox_role_key"] == "senator"


def test_office_draft_infobox_role_key_from_filter(helpers, filter_id):
    draft = helpers._office_draft_from_body({"infobox_role_key_filter_id": filter_id})
    assert draft["infobox_role_key"] == "member_of"


def test_office_draft_infobox_role_key_explicit_overrides_filter(helpers, filter_id):
    draft = helpers._office_draft_from_body(
        {"infobox_role_key": "explicit_key", "infobox_role_key_filter_id": filter_id}
    )
    assert draft["infobox_role_key"] == "explicit_key"


def test_office_draft_include_ref_names(helpers, db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    draft = helpers._office_draft_from_body(
        {"country_id": "1", "level_id": "", "branch_id": "", "state_id": ""},
        include_ref_names=True,
    )
    assert "country_name" in draft
    assert "level_name" in draft
    assert "branch_name" in draft
    assert "state_name" in draft
