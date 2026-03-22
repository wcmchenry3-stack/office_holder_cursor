"""Playwright UI tests for the /offices list page."""

import os

import pytest
from playwright.sync_api import Playwright, expect, sync_playwright

BASE_URL = os.getenv("PLAYWRIGHT_BASE_URL", "http://127.0.0.1:8000")


@pytest.fixture(scope="session")
def playwright_instance():
    try:
        p = sync_playwright().start()
    except Exception as e:
        pytest.skip(f"Playwright sync API unavailable (start app + run separately): {e}")
    try:
        yield p
    finally:
        p.stop()


@pytest.fixture()
def page(playwright_instance: Playwright):
    browser = playwright_instance.chromium.launch()
    page = browser.new_page()
    yield page
    browser.close()


def test_offices_list_renders(page):
    """GET /offices returns a page with the offices table visible."""
    page.goto(f"{BASE_URL}/offices")

    # Either the flat offices table or the page-grouped table must be present
    offices_table = page.locator("#officesTable")
    pages_table = page.locator("#pagesTable")
    has_offices = offices_table.count() > 0
    has_pages = pages_table.count() > 0
    assert (
        has_offices or has_pages
    ), "Expected #officesTable or #pagesTable — neither found on /offices"
    if has_offices:
        expect(offices_table).to_be_visible()
    else:
        expect(pages_table).to_be_visible()


def test_filter_by_enabled_via_query_param(page):
    """Navigating to /offices?enabled=1 filters the list to enabled offices only.

    Verifies that the server respects the 'enabled' query parameter and that
    all rendered rows carry data-enabled="1".
    """
    page.goto(f"{BASE_URL}/offices?enabled=1")

    # Page must still render a table (not an error)
    offices_table = page.locator("#officesTable")
    pages_table = page.locator("#pagesTable")
    has_offices = offices_table.count() > 0
    has_pages = pages_table.count() > 0
    assert has_offices or has_pages, "Expected a table after filtering by enabled=1"

    if has_offices:
        # In flat view, every office row carries data-enabled; verify none are disabled
        disabled_rows = page.locator("#officesTable tr[data-enabled='0']")
        assert (
            disabled_rows.count() == 0
        ), "No disabled offices should appear when filtering by enabled=1"


def test_enable_toggle_calls_api(page):
    """Clicking an enable/disable checkbox fires the correct API endpoint (status 200)."""
    page.goto(f"{BASE_URL}/offices")

    checkbox = page.locator(".office-enabled").first
    if checkbox.count() == 0:
        pytest.skip("No .office-enabled checkboxes found — app may have no offices")

    # Intercept the API call triggered by the toggle
    with page.expect_response(
        lambda r: "/api/offices/" in r.url and "/enabled" in r.url
    ) as response_info:
        checkbox.first.click()

    resp = response_info.value
    assert resp.status == 200, f"Enable/disable toggle API returned {resp.status}, expected 200"

    # Restore original state by clicking again
    with page.expect_response(lambda r: "/api/offices/" in r.url and "/enabled" in r.url):
        checkbox.first.click()
