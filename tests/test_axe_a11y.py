"""
Axe-core WCAG 2.2 CI tests  (Issue #446).

Injects axe-core into 7 key pages and asserts zero WCAG violations.
Marked xfail(strict=False) until all screen stories (#447–#457) ship —
each story's definition of done includes keeping these tests green.
Remove the xfail marker for a given test once its screen story is complete.
Completed: #448 (login), #449 (offices), #451 (offices/new), #453 (run), #454 (wiki-drafts), #457 (operations/reports/refs).
"""

import os

import pytest
from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("PLAYWRIGHT_BASE_URL", "http://127.0.0.1:8000")
AXE_CDN = "https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.9.0/axe.min.js"
AXE_TAGS = ["wcag2a", "wcag2aa", "wcag22aa"]


@pytest.fixture(scope="session")
def playwright_instance():
    try:
        p = sync_playwright().start()
    except Exception as e:
        pytest.skip(f"Playwright not available: {e}")
    try:
        yield p
    finally:
        p.stop()


@pytest.fixture()
def page(playwright_instance):
    browser = playwright_instance.chromium.launch()
    # bypass_csp=True lets axe-core load from the CDN without being blocked by
    # the app's Content-Security-Policy (acceptable for test contexts only).
    ctx = browser.new_context(bypass_csp=True)
    page = ctx.new_page()
    yield page
    browser.close()


def _run_axe(page, path: str) -> list:
    """Navigate to path, inject axe-core, run audit, return violations list."""
    try:
        page.goto(f"{BASE_URL}{path}", wait_until="networkidle", timeout=15_000)
    except Exception as e:
        pytest.skip(f"Server not reachable at {BASE_URL}{path}: {e}")

    page.add_script_tag(url=AXE_CDN)
    page.wait_for_function("typeof axe !== 'undefined'", timeout=10_000)

    violations = page.evaluate(
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
    return violations


def _fmt(violations: list) -> str:
    lines = []
    for v in violations:
        lines.append(f"  [{v['impact']}] {v['id']}: {v['description']}")
        for node in v.get("nodes", [])[:2]:
            lines.append(f"    {node.get('html', '')[:120]}")
    return "\n".join(lines) if lines else "(none)"


# All tests are xfail(strict=False) until screen stories (#447–#457) ship.
# strict=False means: a failure is XFAIL (non-blocking), a pass is XPASS (also non-blocking).
# Flip to a plain passing test once the relevant screen story is done.


@pytest.mark.xfail(strict=False, reason="WCAG violations expected until screen story #448 ships")
def test_axe_login(page):
    v = _run_axe(page, "/login")
    assert v == [], f"/login WCAG violations:\n{_fmt(v)}"


def test_axe_offices(page):
    v = _run_axe(page, "/offices")
    assert v == [], f"/offices WCAG violations:\n{_fmt(v)}"


def test_axe_offices_new(page):
    v = _run_axe(page, "/offices/new")
    assert v == [], f"/offices/new WCAG violations:\n{_fmt(v)}"


def test_axe_run(page):
    v = _run_axe(page, "/run")
    assert v == [], f"/run WCAG violations:\n{_fmt(v)}"


def test_axe_wiki_drafts(page):
    v = _run_axe(page, "/data/wiki-drafts")
    assert v == [], f"/data/wiki-drafts WCAG violations:\n{_fmt(v)}"


@pytest.mark.xfail(strict=False, reason="WCAG violations expected until screen story #455 ships")
def test_axe_gemini_research(page):
    v = _run_axe(page, "/gemini-research")
    assert v == [], f"/gemini-research WCAG violations:\n{_fmt(v)}"


@pytest.mark.xfail(strict=False, reason="WCAG violations expected until screen story #457 ships")
def test_axe_refs(page):
    v = _run_axe(page, "/refs")
    assert v == [], f"/refs WCAG violations:\n{_fmt(v)}"
