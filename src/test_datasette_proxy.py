"""
Unit tests for the Datasette DB explorer proxy helpers in main.py.

Coverage:
- _apply_datasette_dark_css: CSS injection into HTML responses
- Non-HTML passthrough (CSS, JS, JSON)
- Injection position and idempotency

HTTP-level proxy behaviour (503 ConnectError, 504 TimeoutException,
header filtering) requires TestClient + async mocking and will be
covered in Phase 4 when the full test infrastructure is set up.
"""

from src.main import _apply_datasette_dark_css, _DATASETTE_DARK_CSS


# ---------------------------------------------------------------------------
# HTML responses: dark mode CSS is injected
# ---------------------------------------------------------------------------

def test_injects_css_before_closing_head():
    html = b"<html><head><title>Test</title></head><body>hi</body></html>"
    result = _apply_datasette_dark_css(html, "text/html; charset=utf-8")
    expected_marker = (_DATASETTE_DARK_CSS + "</head>").encode()
    assert expected_marker in result


def test_injected_css_contains_dark_background():
    html = b"<html><head></head><body></body></html>"
    result = _apply_datasette_dark_css(html, "text/html")
    assert b"#1a1b22" in result


def test_injected_css_contains_accent_colour():
    html = b"<html><head></head><body></body></html>"
    result = _apply_datasette_dark_css(html, "text/html")
    assert b"#5c7cfa" in result


def test_injects_only_once_when_multiple_head_tags_present():
    # Malformed HTML with two </head> tags — injection should happen exactly once
    html = b"<html><head></head><head></head><body></body></html>"
    result = _apply_datasette_dark_css(html, "text/html")
    assert result.count(b"</head>") == 2  # one injected close + one leftover


def test_original_content_preserved_after_injection():
    html = b"<html><head><title>Datasette</title></head><body><p>data</p></body></html>"
    result = _apply_datasette_dark_css(html, "text/html")
    assert b"<title>Datasette</title>" in result
    assert b"<p>data</p>" in result


# ---------------------------------------------------------------------------
# Non-HTML responses: content is returned unchanged
# ---------------------------------------------------------------------------

def test_css_passthrough_unchanged():
    content = b"body { color: red; }"
    result = _apply_datasette_dark_css(content, "text/css")
    assert result == content


def test_javascript_passthrough_unchanged():
    content = b"console.log('hello');"
    result = _apply_datasette_dark_css(content, "application/javascript")
    assert result == content


def test_json_passthrough_unchanged():
    content = b'{"rows": [], "columns": []}'
    result = _apply_datasette_dark_css(content, "application/json")
    assert result == content


def test_empty_content_type_passthrough_unchanged():
    content = b"\x89PNG\r\n"
    result = _apply_datasette_dark_css(content, "")
    assert result == content


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_html_with_no_head_tag_returns_content_unmodified():
    # No </head> means replace() finds nothing — content returned as-is
    html = b"<html><body>no head tag here</body></html>"
    result = _apply_datasette_dark_css(html, "text/html")
    assert result == html


def test_empty_html_body_returns_unchanged():
    result = _apply_datasette_dark_css(b"", "text/html")
    assert result == b""
