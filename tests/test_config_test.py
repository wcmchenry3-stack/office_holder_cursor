# -*- coding: utf-8 -*-
"""Unit tests for src/scraper/config_test.py.

Tests cover:
- get_table_header_from_html: pure HTML parsing — no mocks needed
- test_office_config: cached fetch mocked
- get_raw_table_preview: cached fetch mocked
- get_all_tables_preview: HTTP mocked

All network I/O is mocked — no live Wikipedia requests are made.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.scraper.config_test import (
    get_table_header_from_html,
    test_office_config as validate_office_config,
    get_raw_table_preview,
    get_all_tables_preview,
)

# ---------------------------------------------------------------------------
# get_table_header_from_html — pure HTML parsing, no mocks
# ---------------------------------------------------------------------------


class TestGetTableHeaderFromHtml:
    def test_returns_list_of_tuples(self):
        html = "<table><tr><th>Name</th><th>Party</th><th>Start</th></tr></table>"
        result = get_table_header_from_html(html)
        assert result == [(0, "Name"), (1, "Party"), (2, "Start")]

    def test_empty_string_returns_empty(self):
        assert get_table_header_from_html("") == []

    def test_none_returns_empty(self):
        assert get_table_header_from_html(None) == []

    def test_no_table_returns_empty(self):
        assert get_table_header_from_html("<div>No table here</div>") == []

    def test_empty_table_returns_empty(self):
        assert get_table_header_from_html("<table></table>") == []

    def test_table_with_td_header(self):
        html = "<table><tr><td>Col A</td><td>Col B</td></tr></table>"
        result = get_table_header_from_html(html)
        assert result == [(0, "Col A"), (1, "Col B")]

    def test_strips_whitespace(self):
        html = "<table><tr><th>  Name  </th><th>\nParty\n</th></tr></table>"
        result = get_table_header_from_html(html)
        assert result[0] == (0, "Name")
        assert result[1] == (1, "Party")

    def test_uses_first_table_only(self):
        html = (
            "<table><tr><th>Table1</th></tr></table>"
            "<table><tr><th>Table2</th></tr></table>"
        )
        result = get_table_header_from_html(html)
        assert len(result) == 1
        assert result[0][1] == "Table1"

    def test_index_is_zero_based(self):
        html = "<table><tr><th>A</th><th>B</th><th>C</th></tr></table>"
        indices = [r[0] for r in get_table_header_from_html(html)]
        assert indices == [0, 1, 2]


# ---------------------------------------------------------------------------
# test_office_config — mocked cache
# ---------------------------------------------------------------------------


class TestOfficeConfig:
    def _make_table_html(self, cols=3, rows=2):
        cells = "".join(f"<td>val{i}</td>" for i in range(cols))
        row = f"<tr>{cells}</tr>"
        header = "<tr>" + "".join(f"<th>H{i}</th>" for i in range(cols)) + "</tr>"
        return f"<table>{header}{''.join([row] * rows)}</table>"

    def test_no_url_returns_false(self):
        ok, msg = validate_office_config({"url": "", "table_no": 1, "link_column": 0})
        assert not ok
        assert "No URL" in msg

    def _mock_table_config(self, link_column=0):
        return {
            "table_no": 1,
            "link_column": link_column,
            "term_start_column": -1,
            "term_end_column": -1,
            "party_column": -1,
            "district_column": -1,
        }

    def test_cache_error_returns_false(self):
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"error": "timeout"},
        ), patch(
            "src.scraper.config_test.db_offices.office_row_to_table_config",
            return_value=self._mock_table_config(),
        ):
            ok, msg = validate_office_config(
                {"url": "https://en.wikipedia.org/wiki/Test", "id": None, "alt_links": []}
            )
        assert not ok
        assert "timeout" in msg

    def test_no_table_html_returns_false(self):
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"html": ""},
        ), patch(
            "src.scraper.config_test.db_offices.office_row_to_table_config",
            return_value=self._mock_table_config(),
        ):
            ok, msg = validate_office_config(
                {"url": "https://en.wikipedia.org/wiki/Test", "id": None, "alt_links": []}
            )
        assert not ok
        assert "No table HTML" in msg

    def test_valid_config_returns_ok(self):
        table_html = self._make_table_html(cols=3, rows=2)
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"html": table_html},
        ), patch("src.scraper.config_test.db_offices.office_row_to_table_config") as mock_cfg:
            mock_cfg.return_value = {
                "table_no": 1,
                "link_column": 0,
                "term_start_column": 1,
                "term_end_column": -1,
                "party_column": -1,
                "district_column": -1,
            }
            ok, msg = validate_office_config(
                {"url": "https://en.wikipedia.org/wiki/Test", "id": None, "alt_links": []}
            )
        assert ok
        assert msg == "OK"

    def test_link_column_not_configured_returns_false(self):
        table_html = self._make_table_html(cols=3, rows=2)
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"html": table_html},
        ), patch("src.scraper.config_test.db_offices.office_row_to_table_config") as mock_cfg:
            mock_cfg.return_value = {
                "table_no": 1,
                "link_column": -1,
                "term_start_column": -1,
                "term_end_column": -1,
                "party_column": -1,
                "district_column": -1,
            }
            ok, msg = validate_office_config(
                {"url": "https://en.wikipedia.org/wiki/Test", "id": None, "alt_links": []}
            )
        assert not ok
        assert "Link column not configured" in msg

    def test_link_column_out_of_range_returns_false(self):
        table_html = self._make_table_html(cols=2, rows=2)
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"html": table_html},
        ), patch("src.scraper.config_test.db_offices.office_row_to_table_config") as mock_cfg:
            mock_cfg.return_value = {
                "table_no": 1,
                "link_column": 5,
                "term_start_column": -1,
                "term_end_column": -1,
                "party_column": -1,
                "district_column": -1,
            }
            ok, msg = validate_office_config(
                {"url": "https://en.wikipedia.org/wiki/Test", "id": None, "alt_links": []}
            )
        assert not ok
        assert "out of range" in msg


# ---------------------------------------------------------------------------
# get_raw_table_preview — mocked cache
# ---------------------------------------------------------------------------


class TestGetRawTablePreview:
    def test_empty_url_returns_none(self):
        assert get_raw_table_preview("") is None
        assert get_raw_table_preview(None) is None

    def test_cache_error_returns_none(self):
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"error": "failed"},
        ):
            result = get_raw_table_preview("https://en.wikipedia.org/wiki/Test")
        assert result is None

    def test_no_table_html_returns_empty_rows(self):
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"html": "", "num_tables": 2},
        ):
            result = get_raw_table_preview("https://en.wikipedia.org/wiki/Test")
        assert result is not None
        assert result["rows"] == []
        assert result["num_tables"] == 2

    def test_returns_rows(self):
        html = "<table><tr><th>H</th></tr><tr><td>Cell1</td></tr><tr><td>Cell2</td></tr></table>"
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"html": html, "num_tables": 1},
        ):
            result = get_raw_table_preview("https://en.wikipedia.org/wiki/Test", max_rows=5)
        assert result is not None
        assert len(result["rows"]) == 2  # 2 data rows (header excluded)
        assert result["rows"][0] == ["Cell1"]

    def test_respects_max_rows(self):
        rows = "".join("<tr><td>R</td></tr>" for _ in range(20))
        html = f"<table><tr><th>H</th></tr>{rows}</table>"
        with patch(
            "src.scraper.config_test.get_table_html_cached",
            return_value={"html": html, "num_tables": 1},
        ):
            result = get_raw_table_preview(
                "https://en.wikipedia.org/wiki/Test", max_rows=3
            )
        assert result is not None
        assert len(result["rows"]) == 3


# ---------------------------------------------------------------------------
# get_all_tables_preview — mocked HTTP
# ---------------------------------------------------------------------------


class TestGetAllTablesPreview:
    def test_empty_url_returns_error(self):
        result = get_all_tables_preview("")
        assert result["num_tables"] == 0
        assert "error" in result

    def test_http_error_returns_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        with patch("src.scraper.config_test.wiki_session") as mock_session:
            mock_session.return_value.get.return_value = mock_resp
            result = get_all_tables_preview("https://en.wikipedia.org/wiki/Test")
        assert result["num_tables"] == 0
        assert "HTTP 500" in result["error"]

    def test_confirm_required_when_many_tables(self):
        many_tables = "".join("<table><tr><td>x</td></tr></table>" for _ in range(15))
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = many_tables
        with patch("src.scraper.config_test.wiki_session") as mock_session:
            mock_session.return_value.get.return_value = mock_resp
            result = get_all_tables_preview(
                "https://en.wikipedia.org/wiki/Test",
                confirm_threshold=10,
                confirmed=False,
            )
        assert result.get("confirm_required") is True
        assert result["num_tables"] == 15

    def test_confirmed_returns_all_tables(self):
        two_tables = (
            "<table><tr><th>H1</th></tr><tr><td>R1</td></tr></table>"
            "<table><tr><th>H2</th></tr><tr><td>R2</td></tr></table>"
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = two_tables
        with patch("src.scraper.config_test.wiki_session") as mock_session:
            mock_session.return_value.get.return_value = mock_resp
            result = get_all_tables_preview(
                "https://en.wikipedia.org/wiki/Test", confirmed=True
            )
        assert result["num_tables"] == 2
        assert len(result["tables"]) == 2
        assert result["tables"][0]["table_index"] == 1

    def test_request_exception_returns_error(self):
        with patch("src.scraper.config_test.wiki_session") as mock_session:
            mock_session.return_value.get.side_effect = Exception("connection refused")
            result = get_all_tables_preview("https://en.wikipedia.org/wiki/Test")
        assert result["num_tables"] == 0
        assert "connection refused" in result["error"]
