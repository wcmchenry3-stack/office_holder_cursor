"""
Mobile responsive end-to-end tests (Issue #459).

Tests are run at a 375×812 viewport (iPhone SE / small phone) to verify:
  1. Layout — sidebar is off-screen, main content takes full width.
  2. Hamburger — #menuBtn is visible; clicking it opens the sidebar drawer.
  3. Drawer close — overlay click and Escape key close the drawer.
  4. Axe-core at mobile viewport — zero WCAG 2.2 AA violations on key pages.
"""

import os

import pytest
from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("PLAYWRIGHT_BASE_URL", "http://127.0.0.1:8000")
AXE_CDN = "https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.9.0/axe.min.js"
AXE_TAGS = ["wcag2a", "wcag2aa", "wcag22aa"]

MOBILE_VIEWPORT = {"width": 375, "height": 812}
SHELL_PAGES = ["/offices", "/run", "/data/wiki-drafts"]


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pw():
    try:
        p = sync_playwright().start()
    except Exception as e:
        pytest.skip(f"Playwright not available: {e}")
    try:
        yield p
    finally:
        p.stop()


@pytest.fixture()
def mobile_page(pw):
    browser = pw.chromium.launch()
    ctx = browser.new_context(viewport=MOBILE_VIEWPORT, bypass_csp=True)
    pg = ctx.new_page()
    yield pg
    browser.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _goto(page, path: str):
    try:
        page.goto(f"{BASE_URL}{path}", wait_until="networkidle", timeout=15_000)
    except Exception as e:
        pytest.skip(f"Server not reachable at {BASE_URL}{path}: {e}")


def _run_axe(page) -> list:
    page.add_script_tag(url=AXE_CDN)
    page.wait_for_function("typeof axe !== 'undefined'", timeout=10_000)
    return page.evaluate(
        """(tags) => new Promise((resolve, reject) => {
            axe.run(
                { runOnly: { type: 'tag', values: tags } },
                function(err, res) {
                    if (err) { reject(err); } else { resolve(res.violations); }
                }
            );
        })""",
        AXE_TAGS,
    )


def _fmt(violations: list) -> str:
    lines = []
    for v in violations:
        lines.append(f"  [{v['impact']}] {v['id']}: {v['description']}")
        for node in v.get("nodes", [])[:2]:
            lines.append(f"    {node.get('html', '')[:120]}")
    return "\n".join(lines) if lines else "(none)"


def _sidebar_x(page) -> float:
    """Return the sidebar's getBoundingClientRect().x (left edge position)."""
    return page.evaluate("document.getElementById('primaryNav').getBoundingClientRect().x")


# ---------------------------------------------------------------------------
# 1. Layout at mobile viewport
# ---------------------------------------------------------------------------


def test_mobile_hamburger_visible(mobile_page):
    """#menuBtn is visible at mobile viewport."""
    _goto(mobile_page, "/offices")
    btn = mobile_page.locator("#menuBtn")
    assert btn.count() == 1, "#menuBtn not found"
    assert btn.is_visible(), "#menuBtn should be visible on mobile"


def test_mobile_sidebar_hidden_by_default(mobile_page):
    """Sidebar is translated off-screen on mobile (x < 0)."""
    _goto(mobile_page, "/offices")
    x = _sidebar_x(mobile_page)
    assert x < 0, f"Sidebar should be off-screen on mobile (x={x})"


def test_mobile_main_content_full_width(mobile_page):
    """Main content margin-inline-start is 0 on mobile."""
    _goto(mobile_page, "/offices")
    margin = mobile_page.evaluate(
        "parseFloat(getComputedStyle(document.querySelector('.main-content')).marginInlineStart)"
    )
    assert margin == 0, f"main-content margin-inline-start should be 0 on mobile, got {margin}"


# ---------------------------------------------------------------------------
# 2. Hamburger / drawer
# ---------------------------------------------------------------------------


def test_mobile_hamburger_opens_sidebar(mobile_page):
    """Clicking #menuBtn slides the sidebar into view."""
    _goto(mobile_page, "/offices")
    mobile_page.locator("#menuBtn").click()
    x = _sidebar_x(mobile_page)
    assert x >= 0, f"Sidebar should be on-screen after hamburger click (x={x})"


def test_mobile_hamburger_aria_expanded(mobile_page):
    """aria-expanded on #menuBtn reflects open/closed state."""
    _goto(mobile_page, "/offices")
    btn = mobile_page.locator("#menuBtn")
    assert btn.get_attribute("aria-expanded") == "false", "Should start closed"
    btn.click()
    assert btn.get_attribute("aria-expanded") == "true", "Should be open after click"
    btn.click()
    assert btn.get_attribute("aria-expanded") == "false", "Should close on second click"


def test_mobile_overlay_closes_sidebar(mobile_page):
    """Clicking the sidebar overlay closes the drawer."""
    _goto(mobile_page, "/offices")
    mobile_page.locator("#menuBtn").click()
    # Overlay is now visible — click it
    mobile_page.locator("#sidebarOverlay").click()
    x = _sidebar_x(mobile_page)
    assert x < 0, f"Sidebar should close after overlay click (x={x})"


def test_mobile_escape_closes_sidebar(mobile_page):
    """Pressing Escape closes the sidebar drawer."""
    _goto(mobile_page, "/offices")
    mobile_page.locator("#menuBtn").click()
    mobile_page.keyboard.press("Escape")
    x = _sidebar_x(mobile_page)
    assert x < 0, f"Sidebar should close on Escape (x={x})"


# ---------------------------------------------------------------------------
# 3. Axe-core at mobile viewport — no WCAG regressions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", SHELL_PAGES)
def test_mobile_axe(mobile_page, path):
    """Zero WCAG 2.2 AA violations on key pages at 375px viewport."""
    _goto(mobile_page, path)
    violations = _run_axe(mobile_page)
    assert violations == [], f"{path} mobile WCAG violations:\n{_fmt(violations)}"
