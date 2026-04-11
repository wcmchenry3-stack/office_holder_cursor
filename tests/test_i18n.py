"""
Tests for i18n foundation — story #460.

Covers:
  - resolve_locale()      Accept-Language parsing, exact/base/fallback matching
  - /set-locale endpoint  cookie persistence, redirect, invalid locale guard
  - Locale middleware      request.state.locale populated correctly
  - Template context       lang attr and _ function injected by I18nTemplates
"""

import importlib
import os

import pytest
from fastapi.testclient import TestClient

from src.i18n import (
    LOCALE_NAMES,
    RTL_LOCALES,
    SUPPORTED_LOCALES,
    _parse_accept_language,
    resolve_locale,
)

# ---------------------------------------------------------------------------
# resolve_locale()
# ---------------------------------------------------------------------------


class TestParseAcceptLanguage:
    def test_empty_header(self):
        assert _parse_accept_language("") == []

    def test_single_tag(self):
        assert _parse_accept_language("en") == ["en"]

    def test_quality_ordering(self):
        result = _parse_accept_language("fr-CA,fr;q=0.9,en;q=0.8")
        assert result == ["fr-CA", "fr", "en"]

    def test_equal_quality_preserved(self):
        result = _parse_accept_language("de,en")
        assert result[0] == "de"
        assert "en" in result

    def test_invalid_q_treated_as_1(self):
        result = _parse_accept_language("ko;q=bad,en")
        assert result[0] == "ko"


class TestResolveLocale:
    def test_exact_match(self):
        assert resolve_locale("es", SUPPORTED_LOCALES) == "es"

    def test_exact_match_with_region(self):
        assert resolve_locale("fr-CA", SUPPORTED_LOCALES) == "fr-CA"

    def test_base_language_fallback(self):
        # 'fr' is not in the list but 'fr-CA' is → should match
        assert resolve_locale("fr", SUPPORTED_LOCALES) == "fr-CA"

    def test_accept_language_quality_order(self):
        # First tag 'it' has no match; second tag 'de' matches exactly
        assert resolve_locale("it,de;q=0.9", SUPPORTED_LOCALES) == "de"

    def test_en_fallback(self):
        assert resolve_locale("xx-YY", SUPPORTED_LOCALES) == "en"

    def test_empty_header_returns_en(self):
        assert resolve_locale("", SUPPORTED_LOCALES) == "en"

    def test_default_supported_locales(self):
        assert resolve_locale("ja") == "ja"


# ---------------------------------------------------------------------------
# Constants sanity checks
# ---------------------------------------------------------------------------


def test_rtl_locales_subset_of_supported():
    assert RTL_LOCALES.issubset(set(SUPPORTED_LOCALES))


def test_locale_names_covers_all_supported():
    for loc in SUPPORTED_LOCALES:
        assert loc in LOCALE_NAMES, f"Missing LOCALE_NAMES entry for {loc!r}"


def test_thirteen_supported_locales():
    assert len(SUPPORTED_LOCALES) == 13


# ---------------------------------------------------------------------------
# /set-locale endpoint
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(tmp_db_path):
    """TestClient with a freshly reloaded app and a temp DB.

    Reloads src.main to get a clean app instance — same pattern as other router
    test fixtures. Also clears accumulated slowapi route-limit entries for
    src.main routes: without this cleanup, each reload extends
    `limiter._route_limits["src.main.*"]`, causing one HTTP request to be
    counted N times against the storage counter and triggering false 429s.
    """
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(tmp_db_path)
    import src.main as main_mod
    import src.routers._deps as deps_mod

    # Clear stale route-limit entries accumulated by previous reloads so each
    # request is counted exactly once.
    lim = deps_mod.limiter
    for key in list(lim._route_limits.keys()):
        if key.startswith("src.main."):
            del lim._route_limits[key]

    importlib.reload(main_mod)
    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c
    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)


class TestSetLocale:
    def test_valid_locale_stored_in_session(self, client):
        resp = client.post(
            "/set-locale",
            data={"locale": "es"},
            headers={"referer": "/offices"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert resp.headers["location"] == "/offices"

    def test_invalid_locale_does_not_crash(self, client):
        resp = client.post(
            "/set-locale",
            data={"locale": "xx-INVALID"},
            headers={"referer": "/"},
            follow_redirects=False,
        )
        assert resp.status_code == 303  # redirects but ignores bad locale

    def test_missing_referer_redirects_to_root(self, client):
        resp = client.post(
            "/set-locale",
            data={"locale": "de"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert resp.headers["location"] == "/"


# ---------------------------------------------------------------------------
# Template context — locale and lang attribute
# ---------------------------------------------------------------------------


class TestLocaleTemplateContext:
    def test_lang_attr_default_en(self, client):
        resp = client.get("/offices")
        assert resp.status_code == 200
        assert 'lang="en"' in resp.text

    def test_lang_attr_reflects_cookie(self, client):
        # Set locale via /set-locale then visit a page
        client.post("/set-locale", data={"locale": "de"}, follow_redirects=False)
        resp = client.get("/offices")
        assert 'lang="de"' in resp.text

    def test_ltr_dir_attribute(self, client):
        resp = client.get("/offices")
        assert 'dir="ltr"' in resp.text


# ---------------------------------------------------------------------------
# RTL layout — story #461
# ---------------------------------------------------------------------------


class TestRTLLayout:
    def test_ar_dir_rtl(self, client):
        client.post("/set-locale", data={"locale": "ar"}, follow_redirects=False)
        resp = client.get("/offices")
        assert resp.status_code == 200
        assert 'lang="ar"' in resp.text
        assert 'dir="rtl"' in resp.text

    def test_he_dir_rtl(self, client):
        client.post("/set-locale", data={"locale": "he"}, follow_redirects=False)
        resp = client.get("/offices")
        assert resp.status_code == 200
        assert 'lang="he"' in resp.text
        assert 'dir="rtl"' in resp.text

    def test_en_dir_ltr(self, client):
        client.post("/set-locale", data={"locale": "en"}, follow_redirects=False)
        resp = client.get("/offices")
        assert 'dir="ltr"' in resp.text

    def test_ar_in_language_switcher(self, client):
        """ar must appear in the language switcher dropdown (RTL gate removed)."""
        resp = client.get("/offices")
        assert resp.status_code == 200
        assert 'value="ar"' in resp.text

    def test_he_in_language_switcher(self, client):
        """he must appear in the language switcher dropdown (RTL gate removed)."""
        resp = client.get("/offices")
        assert resp.status_code == 200
        assert 'value="he"' in resp.text

    def test_ar_noto_font_loaded(self, client):
        """Noto Sans Arabic link tag appears only when locale is ar."""
        client.post("/set-locale", data={"locale": "ar"}, follow_redirects=False)
        resp = client.get("/offices")
        assert "Noto+Sans+Arabic" in resp.text

    def test_he_noto_font_loaded(self, client):
        """Noto Sans Hebrew link tag appears only when locale is he."""
        client.post("/set-locale", data={"locale": "he"}, follow_redirects=False)
        resp = client.get("/offices")
        assert "Noto+Sans+Hebrew" in resp.text

    def test_en_no_rtl_font(self, client):
        """No Noto RTL font link tag rendered for LTR locales."""
        resp = client.get("/offices")
        assert "Noto+Sans+Arabic" not in resp.text
        assert "Noto+Sans+Hebrew" not in resp.text

    @pytest.mark.parametrize("locale", ["ar", "he"])
    def test_rtl_locales_accessible_key_pages(self, client, locale):
        """HTTP 200 for RTL locales on key pages (locale smoke test)."""
        client.post("/set-locale", data={"locale": locale}, follow_redirects=False)
        for path in ["/offices", "/run", "/data/individuals", "/refs", "/reports"]:
            resp = client.get(path)
            assert resp.status_code == 200, f"Expected 200 for {locale} on {path}"
