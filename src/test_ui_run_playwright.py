"""Playwright UI tests for the /run (scraper run) page."""

import os

import pytest
from playwright.sync_api import Playwright, expect, sync_playwright

BASE_URL = os.getenv("PLAYWRIGHT_BASE_URL", "http://127.0.0.1:8000")


@pytest.fixture(scope="session")
def playwright_instance() -> Playwright:
    with sync_playwright() as p:
        yield p


@pytest.fixture()
def page(playwright_instance: Playwright):
    browser = playwright_instance.chromium.launch()
    page = browser.new_page()
    yield page
    browser.close()


def test_run_page_loads(page):
    """GET /run returns the run form with key elements present and running panel hidden."""
    page.goto(f"{BASE_URL}/run")

    expect(page.locator("#runForm")).to_be_visible()
    expect(page.locator("#runMode")).to_be_visible()
    expect(page.locator("#runBtn")).to_be_visible()
    expect(page.locator("#runningPanel")).to_be_hidden()


def test_run_mode_switching_shows_correct_fields(page):
    """Changing #runMode reveals/hides the correct conditional field groups."""
    page.goto(f"{BASE_URL}/run")

    run_mode = page.locator("#runMode")

    # single_bio: individual ref visible, office category hidden
    run_mode.select_option("single_bio")
    expect(page.locator("#individualRefGroup")).to_be_visible()
    expect(page.locator("#officeCategoryGroup")).to_be_hidden()

    # populate_category_terms: office category visible, individual ref hidden
    run_mode.select_option("populate_category_terms")
    expect(page.locator("#officeCategoryGroup")).to_be_visible()
    expect(page.locator("#individualRefGroup")).to_be_hidden()

    # delta: both groups hidden
    run_mode.select_option("delta")
    expect(page.locator("#individualRefGroup")).to_be_hidden()
    expect(page.locator("#officeCategoryGroup")).to_be_hidden()


def test_run_submission_shows_running_panel(page):
    """Submitting a delta run returns 202 and transitions the UI to the running state.

    The test triggers a real background job but does not wait for completion.
    The job is cancelled immediately after the UI transition is confirmed.
    """
    page.goto(f"{BASE_URL}/run")

    page.locator("#runMode").select_option("delta")

    with page.expect_response("**/api/run") as response_info:
        page.locator("#runBtn").click()

    resp = response_info.value
    assert resp.status == 202, f"Expected 202 Accepted from /api/run, got {resp.status}"

    expect(page.locator("#runningPanel")).to_be_visible()

    # Cancel the job immediately to avoid running a full scrape during tests
    job_id = resp.json().get("job_id")
    if job_id:
        page.request.post(f"{BASE_URL}/api/run/cancel/{job_id}")
