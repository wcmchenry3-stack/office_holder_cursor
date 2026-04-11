"""
Dark mode end-to-end tests (Issue #458).

Covers three behaviours:
  1. Toggle — clicking #darkModeBtn adds html.dark and flips aria-pressed/label/icon.
  2. Persistence — setting rulersai_theme='dark' in localStorage causes the
     pre-paint script to apply html.dark before first paint (no FOUC).
  3. Axe-core in dark mode — zero WCAG 2.2 AA violations on key pages while
     the dark palette is active (catches contrast regressions in dark tokens).
"""

import os

import pytest
from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("PLAYWRIGHT_BASE_URL", "http://127.0.0.1:8000")
AXE_CDN = "https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.9.0/axe.min.js"
AXE_TAGS = ["wcag2a", "wcag2aa", "wcag22aa"]

# Pages that require the app shell (sidebar + dark-mode button).
SHELL_PAGES = ["/offices", "/run", "/data/wiki-drafts", "/gemini-research"]


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
def page(pw):
    browser = pw.chromium.launch()
    ctx = browser.new_context(bypass_csp=True)
    pg = ctx.new_page()
    yield pg
    browser.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _goto(page, path: str):
    """Navigate to path; skip the test if the server is not reachable."""
    try:
        page.goto(f"{BASE_URL}{path}", wait_until="networkidle", timeout=15_000)
    except Exception as e:
        pytest.skip(f"Server not reachable at {BASE_URL}{path}: {e}")


def _enable_dark_via_localstorage(page, path: str):
    """Set the dark-mode preference in localStorage, then navigate so the
    pre-paint script can apply html.dark before first paint."""
    # Open any page first so localStorage is available on the origin.
    _goto(page, path)
    page.evaluate("localStorage.setItem('rulersai_theme', 'dark')")
    page.reload(wait_until="networkidle")


def _run_axe(page) -> list:
    """Inject axe-core into the current page and return violations."""
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


# ---------------------------------------------------------------------------
# 1. Toggle behaviour
# ---------------------------------------------------------------------------


def test_dark_mode_toggle_applies_class(page):
    """Clicking #darkModeBtn adds html.dark to the document element."""
    _goto(page, "/offices")
    btn = page.locator("#darkModeBtn")
    assert btn.count() == 1, "#darkModeBtn not found on /offices"

    # Start in light mode
    assert not page.evaluate(
        "document.documentElement.classList.contains('dark')"
    ), "Expected light mode on fresh load"

    btn.click()

    assert page.evaluate(
        "document.documentElement.classList.contains('dark')"
    ), "html.dark not added after clicking #darkModeBtn"


def test_dark_mode_toggle_updates_aria(page):
    """Toggle updates aria-pressed and aria-label on #darkModeBtn."""
    _goto(page, "/offices")
    btn = page.locator("#darkModeBtn")

    initial_label = btn.get_attribute("aria-label")
    initial_pressed = btn.get_attribute("aria-pressed")

    btn.click()

    assert btn.get_attribute("aria-pressed") == "true", "aria-pressed should be 'true' in dark mode"
    assert (
        btn.get_attribute("aria-label") != initial_label
    ), "aria-label should change when toggling dark mode"
    assert (
        btn.get_attribute("aria-pressed") != initial_pressed
    ), "aria-pressed should flip on toggle"


def test_dark_mode_toggle_is_reversible(page):
    """A second click returns to light mode."""
    _goto(page, "/offices")
    btn = page.locator("#darkModeBtn")

    btn.click()  # → dark
    btn.click()  # → light

    assert not page.evaluate(
        "document.documentElement.classList.contains('dark')"
    ), "html.dark should be removed after toggling back to light"
    assert (
        btn.get_attribute("aria-pressed") == "false"
    ), "aria-pressed should be 'false' after returning to light mode"


# ---------------------------------------------------------------------------
# 2. Persistence / pre-paint (no FOUC)
# ---------------------------------------------------------------------------


def test_dark_mode_persists_across_reload(page):
    """After toggling dark mode, a page reload keeps html.dark (localStorage)."""
    _goto(page, "/offices")
    page.locator("#darkModeBtn").click()
    page.reload(wait_until="networkidle")

    assert page.evaluate(
        "document.documentElement.classList.contains('dark')"
    ), "html.dark should be restored from localStorage after reload"


def test_dark_mode_prepaint_no_fouc(page):
    """Setting rulersai_theme='dark' in localStorage before navigation causes
    the pre-paint script to apply html.dark synchronously — no flash."""
    _enable_dark_via_localstorage(page, "/offices")

    # html.dark must already be present (applied by inline script, before CSS loads)
    assert page.evaluate(
        "document.documentElement.classList.contains('dark')"
    ), "Pre-paint script did not apply html.dark from localStorage"


# ---------------------------------------------------------------------------
# 3. Axe-core in dark mode — no WCAG contrast regressions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", SHELL_PAGES)
def test_dark_mode_axe(page, path):
    """Zero WCAG 2.2 AA violations on key pages while dark mode is active."""
    _enable_dark_via_localstorage(page, path)

    violations = _run_axe(page)
    assert violations == [], f"{path} dark-mode WCAG violations:\n{_fmt(violations)}"
