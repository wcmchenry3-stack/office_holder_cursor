import os
import re

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


def _goto_edit(page, office_id: str) -> None:
    page.goto(f"{BASE_URL}/offices/{office_id}")
    expect(page.locator("#officeForm")).to_be_visible()


def test_term_dates_merged_disables_term_end_and_matches_start(page):
    office_id = os.getenv("PLAYWRIGHT_EDIT_OFFICE_ID")
    if not office_id:
        pytest.skip("Set PLAYWRIGHT_EDIT_OFFICE_ID for edit-office UI tests")

    _goto_edit(page, office_id)

    term_start = page.locator("#termStartColumn")
    term_end = page.locator("#termEndColumn")
    merged = page.locator("#termDatesMerged")

    term_start.fill("6")
    merged.check()

    expect(term_end).to_be_disabled()
    expect(term_end).to_have_value("6")

    term_start.fill("8")
    expect(term_end).to_have_value("8")


def test_no_district_mode_disables_district_column(page):
    office_id = os.getenv("PLAYWRIGHT_EDIT_OFFICE_ID")
    if not office_id:
        pytest.skip("Set PLAYWRIGHT_EDIT_OFFICE_ID for edit-office UI tests")

    _goto_edit(page, office_id)

    district_mode = page.locator("#districtMode")
    district_column = page.locator("#districtColumn")

    district_column.fill("9")
    district_mode.select_option("no_district")

    expect(district_column).to_be_disabled()
    expect(district_column).to_have_value("0")


def test_ignore_party_disables_party_column(page):
    office_id = os.getenv("PLAYWRIGHT_EDIT_OFFICE_ID")
    if not office_id:
        pytest.skip("Set PLAYWRIGHT_EDIT_OFFICE_ID for edit-office UI tests")

    _goto_edit(page, office_id)

    party_ignore = page.locator("#partyIgnore")
    party_column = page.locator("#partyColumn")

    party_column.fill("7")
    party_ignore.check()

    expect(party_column).to_be_disabled()
    expect(party_column).to_have_value("0")


def test_unmerged_equal_term_columns_block_save(page):
    office_id = os.getenv("PLAYWRIGHT_EDIT_OFFICE_ID")
    if not office_id:
        pytest.skip("Set PLAYWRIGHT_EDIT_OFFICE_ID for edit-office UI tests")

    _goto_edit(page, office_id)

    merged = page.locator("#termDatesMerged")
    term_start = page.locator("#termStartColumn")
    term_end = page.locator("#termEndColumn")

    merged.uncheck()
    term_start.fill("5")
    term_end.fill("5")

    page.locator('#officeForm button[type="submit"]').first.click()

    expect(page).to_have_url(re.compile(r"/offices/\d+\?error="))
    expect(page.get_by_text("Term start column and term end column must be different")).to_be_visible()


def test_table_no_reuse_rules_across_page_and_same_office(page):
    """
    Preconditions (set with env vars):
      - PLAYWRIGHT_PAGE_EDIT_URL points to a page edit route containing >=2 office forms.
      - PLAYWRIGHT_OFFICE_A_ID and PLAYWRIGHT_OFFICE_B_ID are offices on the same source page.

    This test validates:
      1) When allow_reuse_tables is OFF, reusing table number across offices is rejected.
      2) When allow_reuse_tables is ON, cross-office reuse is allowed.
      3) Duplicate table numbers within the same office are still rejected.
    """
    page_url = os.getenv("PLAYWRIGHT_PAGE_EDIT_URL")
    office_a = os.getenv("PLAYWRIGHT_OFFICE_A_ID")
    office_b = os.getenv("PLAYWRIGHT_OFFICE_B_ID")
    if not (page_url and office_a and office_b):
        pytest.skip("Set PLAYWRIGHT_PAGE_EDIT_URL, PLAYWRIGHT_OFFICE_A_ID, PLAYWRIGHT_OFFICE_B_ID")

    page.goto(page_url)
    expect(page.locator("#pageForm")).to_be_visible()

    allow_reuse = page.locator("#allowReuseTablesInput")

    office_a_form = page.locator(f'#section-office-{office_a} form.office-form')
    office_b_form = page.locator(f'#section-office-{office_b} form.office-form')

    table_a = office_a_form.locator('input[name="tc_table_no"]').first
    table_b = office_b_form.locator('input[name="tc_table_no"]').first

    table_a.fill("3")
    table_b.fill("4")

    allow_reuse.uncheck()
    table_b.fill("3")
    office_b_form.locator('button[type="submit"]').first.click()

    expect(page.get_by_text("Table numbers must be unique per page")).to_be_visible()

    page.goto(page_url)
    allow_reuse = page.locator("#allowReuseTablesInput")
    allow_reuse.check()
    page.locator("#pageForm button[type='submit']").first.click()

    office_b_form = page.locator(f'#section-office-{office_b} form.office-form')
    table_b = office_b_form.locator('input[name="tc_table_no"]').first
    table_b.fill("3")
    office_b_form.locator('button[type="submit"]').first.click()

    expect(page.get_by_text("Table numbers must be unique per page")).to_have_count(0)

    office_a_form = page.locator(f'#section-office-{office_a} form.office-form')
    add_table_btn = office_a_form.locator('.add-table-btn').first
    add_table_btn.click()

    office_a_blocks = office_a_form.locator('.table-config-block')
    last_table_no = office_a_blocks.nth(office_a_blocks.count() - 1).locator('input[name="tc_table_no"]')
    first_table_no = office_a_blocks.first.locator('input[name="tc_table_no"]')

    original = first_table_no.input_value()
    last_table_no.fill(original)
    office_a_form.locator('button[type="submit"]').first.click()

    expect(page.get_by_text("Duplicate table_no within office")).to_be_visible()
