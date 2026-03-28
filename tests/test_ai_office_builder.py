# -*- coding: utf-8 -*-
"""Unit tests for src/services/ai_office_builder.py.

Wikipedia API calls made by the scraper include a descriptive User-Agent header
per Wikimedia API etiquette (see src/scraper/wiki_fetch.py: WIKIPEDIA_REQUEST_HEADERS).
OpenAI RateLimitError (HTTP 429) handling is tested below.
"""

from __future__ import annotations

import httpx
import openai
from unittest.mock import MagicMock, patch

import pytest

from src.services.ai_office_builder import (
    AIOfficeBuilder,
    AIOfficePageResponse,
    AITableConfig,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _builder() -> AIOfficeBuilder:
    return AIOfficeBuilder(api_key="test-key")


def _make_completion(tables: list[AITableConfig]) -> MagicMock:
    """Return a mock completion whose .choices[0].message.parsed == AIOfficePageResponse(tables)."""
    parsed = AIOfficePageResponse(tables=tables)
    msg = MagicMock()
    msg.parsed = parsed
    msg.content = '{"tables": []}'  # content used for multi-turn append
    choice = MagicMock()
    choice.message = msg
    completion = MagicMock()
    completion.choices = [choice]
    return completion


_TABLES_PREVIEW = {
    "num_tables": 1,
    "tables": [
        {
            "table_index": 1,
            "rows": [
                ["Name", "Party", "Term Start", "Term End"],
                ["John Smith", "Dem", "2001", "2005"],
                ["Jane Doe", "Rep", "2005", "2009"],
            ],
        }
    ],
}

_BATCH_DEFAULTS = {"country_id": 1, "level_id": 2, "branch_id": 3}


# ---------------------------------------------------------------------------
# _check_success_criteria
# ---------------------------------------------------------------------------


class TestCheckSuccessCriteria:
    def test_passes_majority_links(self):
        b = _builder()
        preview = {
            "error": None,
            "preview_rows": [
                {"Wiki Link": "/wiki/A"},
                {"Wiki Link": "/wiki/B"},
                {"Wiki Link": ""},
            ],
        }
        ok, msg = b._check_success_criteria(preview)
        assert ok
        assert msg == ""

    def test_passes_all_links(self):
        b = _builder()
        preview = {
            "error": None,
            "preview_rows": [{"Wiki Link": "/wiki/A"}, {"Wiki Link": "/wiki/B"}],
        }
        ok, _ = b._check_success_criteria(preview)
        assert ok

    def test_fails_low_link_rate(self):
        b = _builder()
        preview = {
            "error": None,
            "preview_rows": [
                {"Wiki Link": ""},
                {"Wiki Link": ""},
                {"Wiki Link": "/wiki/A"},
            ],
        }
        ok, msg = b._check_success_criteria(preview)
        assert not ok
        assert "50%" in msg

    def test_fails_on_error_string(self):
        b = _builder()
        preview = {"error": "Table not found", "preview_rows": []}
        ok, msg = b._check_success_criteria(preview)
        assert not ok
        assert "Table not found" in msg

    def test_fails_empty_rows(self):
        b = _builder()
        preview = {"error": None, "preview_rows": []}
        ok, msg = b._check_success_criteria(preview)
        assert not ok
        assert "no rows" in msg.lower()


# ---------------------------------------------------------------------------
# _analyze_page
# ---------------------------------------------------------------------------


class TestAnalyzePage:
    @patch("src.services.ai_office_builder.openai.OpenAI")
    def test_appends_system_and_user_messages(self, mock_openai_cls):
        config = AITableConfig(
            table_no=1, name="Gov", link_column=1, term_start_column=3, term_end_column=4
        )
        completion = _make_completion([config])
        mock_openai_cls.return_value.beta.chat.completions.parse.return_value = completion

        b = AIOfficeBuilder(api_key="test")
        messages: list[dict] = []
        result = b._analyze_page("https://en.wikipedia.org/wiki/Test", _TABLES_PREVIEW, messages)

        # system + user + assistant appended
        assert len(messages) == 3
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
        assert messages[2]["role"] == "assistant"
        assert len(result.tables) == 1
        assert result.tables[0].link_column == 1


# ---------------------------------------------------------------------------
# process_url_with_retries
# ---------------------------------------------------------------------------


class TestProcessUrlWithRetries:
    @patch("src.services.ai_office_builder.db_refs.get_country_name", return_value="US")
    @patch("src.services.ai_office_builder.db_refs.get_level_name", return_value="Federal")
    @patch("src.services.ai_office_builder.db_refs.get_branch_name", return_value="Executive")
    @patch("src.services.ai_office_builder.db_refs.get_state_name", return_value="")
    @patch("src.services.ai_office_builder.db_offices.validate_office_table_config")
    @patch("src.services.ai_office_builder.db_offices.create_office", return_value=42)
    @patch("src.services.ai_office_builder.preview_with_config")
    @patch("src.services.ai_office_builder.get_all_tables_preview", return_value=_TABLES_PREVIEW)
    @patch("src.services.ai_office_builder.openai.OpenAI")
    def test_success_on_first_attempt(
        self,
        mock_openai_cls,
        mock_preview_fn,
        mock_pwc,
        mock_create,
        mock_validate,
        mock_state,
        mock_branch,
        mock_level,
        mock_country,
    ):
        config = AITableConfig(
            table_no=1, name="Gov", link_column=1, term_start_column=3, term_end_column=4
        )
        mock_openai_cls.return_value.beta.chat.completions.parse.return_value = _make_completion(
            [config]
        )
        mock_pwc.return_value = {
            "error": None,
            "preview_rows": [{"Wiki Link": "/wiki/A"}, {"Wiki Link": "/wiki/B"}],
        }

        b = AIOfficeBuilder(api_key="test")
        result = b.process_url_with_retries("https://en.wikipedia.org/wiki/Test", _BATCH_DEFAULTS)

        assert result["status"] == "success"
        assert result["offices_created"] == [42]
        assert result["attempts"] == 1
        mock_create.assert_called_once()

    @patch("src.services.ai_office_builder.db_refs.get_country_name", return_value="US")
    @patch("src.services.ai_office_builder.db_refs.get_level_name", return_value="Federal")
    @patch("src.services.ai_office_builder.db_refs.get_branch_name", return_value="Executive")
    @patch("src.services.ai_office_builder.db_refs.get_state_name", return_value="")
    @patch("src.services.ai_office_builder.db_offices.validate_office_table_config")
    @patch("src.services.ai_office_builder.db_offices.create_office", return_value=99)
    @patch("src.services.ai_office_builder.preview_with_config")
    @patch("src.services.ai_office_builder.get_all_tables_preview", return_value=_TABLES_PREVIEW)
    @patch("src.services.ai_office_builder.openai.OpenAI")
    def test_success_on_second_attempt(
        self,
        mock_openai_cls,
        mock_preview_fn,
        mock_pwc,
        mock_create,
        mock_validate,
        mock_state,
        mock_branch,
        mock_level,
        mock_country,
    ):
        """First preview returns empty rows; second attempt returns good config that passes."""
        bad_config = AITableConfig(
            table_no=1, name="Gov", link_column=9, term_start_column=3, term_end_column=4
        )
        good_config = AITableConfig(
            table_no=1, name="Gov", link_column=1, term_start_column=3, term_end_column=4
        )

        call_n = {"n": 0}

        def parse_side_effect(**_):
            call_n["n"] += 1
            cfg = bad_config if call_n["n"] == 1 else good_config
            return _make_completion([cfg])

        mock_openai_cls.return_value.beta.chat.completions.parse.side_effect = parse_side_effect

        # First preview empty, second preview with links
        mock_pwc.side_effect = [
            {"error": None, "preview_rows": []},
            {"error": None, "preview_rows": [{"Wiki Link": "/wiki/A"}, {"Wiki Link": "/wiki/B"}]},
        ]

        b = AIOfficeBuilder(api_key="test")
        result = b.process_url_with_retries(
            "https://en.wikipedia.org/wiki/Test", _BATCH_DEFAULTS, max_retries=5
        )

        assert result["status"] == "success"
        assert result["offices_created"] == [99]
        assert result["attempts"] == 2
        mock_create.assert_called_once()

    @patch("src.services.ai_office_builder.db_refs.get_country_name", return_value="US")
    @patch("src.services.ai_office_builder.db_refs.get_level_name", return_value="Federal")
    @patch("src.services.ai_office_builder.db_refs.get_branch_name", return_value="Executive")
    @patch("src.services.ai_office_builder.db_refs.get_state_name", return_value="")
    @patch("src.services.ai_office_builder.db_offices.validate_office_table_config")
    @patch("src.services.ai_office_builder.db_offices.create_office")
    @patch("src.services.ai_office_builder.preview_with_config")
    @patch("src.services.ai_office_builder.get_all_tables_preview", return_value=_TABLES_PREVIEW)
    @patch("src.services.ai_office_builder.openai.OpenAI")
    def test_gives_up_after_max_retries(
        self,
        mock_openai_cls,
        mock_preview_fn,
        mock_pwc,
        mock_create,
        mock_validate,
        mock_state,
        mock_branch,
        mock_level,
        mock_country,
    ):
        """Always fails validation; create_office is never called; status=failed."""
        bad_config = AITableConfig(
            table_no=1, name="Gov", link_column=9, term_start_column=3, term_end_column=4
        )
        mock_openai_cls.return_value.beta.chat.completions.parse.return_value = _make_completion(
            [bad_config]
        )
        mock_pwc.return_value = {"error": None, "preview_rows": []}  # always empty

        b = AIOfficeBuilder(api_key="test")
        result = b.process_url_with_retries(
            "https://en.wikipedia.org/wiki/Test", _BATCH_DEFAULTS, max_retries=5
        )

        assert result["status"] == "failed"
        assert result["offices_created"] == []
        assert result["attempts"] == 5
        mock_create.assert_not_called()

    @patch("src.services.ai_office_builder.get_all_tables_preview", return_value=_TABLES_PREVIEW)
    @patch("src.services.ai_office_builder.openai.OpenAI")
    def test_rate_limit_error_returns_failed(self, mock_openai_cls, mock_preview_fn):
        """openai.RateLimitError (HTTP 429) must return a failed result without crashing."""
        rate_limit_err = openai.RateLimitError(
            "rate limit exceeded",
            response=httpx.Response(429, request=httpx.Request("POST", "https://api.openai.com")),
            body={},
        )
        mock_openai_cls.return_value.beta.chat.completions.parse.side_effect = rate_limit_err
        b = AIOfficeBuilder(api_key="test")
        result = b.process_url_with_retries("https://en.wikipedia.org/wiki/Test", _BATCH_DEFAULTS)
        assert result["status"] == "failed"
        assert result["attempts"] >= 1

    @patch("src.services.ai_office_builder.get_all_tables_preview")
    def test_fetch_error_fails_immediately(self, mock_preview_fn):
        mock_preview_fn.return_value = {"num_tables": 0, "error": "HTTP 404"}
        b = _builder()
        result = b.process_url_with_retries("https://en.wikipedia.org/wiki/Dead", _BATCH_DEFAULTS)
        assert result["status"] == "failed"
        assert "404" in (result["error"] or "")
        assert result["attempts"] == 0

    @patch("src.services.ai_office_builder.get_all_tables_preview", return_value=_TABLES_PREVIEW)
    @patch("src.services.ai_office_builder.openai.OpenAI")
    def test_no_tables_returned(self, mock_openai_cls, mock_preview_fn):
        mock_openai_cls.return_value.beta.chat.completions.parse.return_value = _make_completion([])
        b = AIOfficeBuilder(api_key="test")
        result = b.process_url_with_retries("https://en.wikipedia.org/wiki/Test", _BATCH_DEFAULTS)
        assert result["status"] == "no_tables"

    @patch("src.services.ai_office_builder.db_refs.get_country_name", return_value="US")
    @patch("src.services.ai_office_builder.db_refs.get_level_name", return_value="Federal")
    @patch("src.services.ai_office_builder.db_refs.get_branch_name", return_value="Executive")
    @patch("src.services.ai_office_builder.db_refs.get_state_name", return_value="")
    @patch("src.services.ai_office_builder.db_offices.validate_office_table_config")
    @patch("src.services.ai_office_builder.db_offices.create_office")
    @patch("src.services.ai_office_builder.preview_with_config")
    @patch("src.services.ai_office_builder.get_all_tables_preview", return_value=_TABLES_PREVIEW)
    @patch("src.services.ai_office_builder.openai.OpenAI")
    def test_cancel_check_stops_processing(
        self,
        mock_openai_cls,
        mock_preview_fn,
        mock_pwc,
        mock_create,
        mock_validate,
        mock_state,
        mock_branch,
        mock_level,
        mock_country,
    ):
        config = AITableConfig(
            table_no=1, name="Gov", link_column=1, term_start_column=3, term_end_column=4
        )
        mock_openai_cls.return_value.beta.chat.completions.parse.return_value = _make_completion(
            [config]
        )
        mock_pwc.return_value = {"error": None, "preview_rows": []}

        b = AIOfficeBuilder(api_key="test")
        result = b.process_url_with_retries(
            "https://en.wikipedia.org/wiki/Test",
            _BATCH_DEFAULTS,
            max_retries=5,
            cancel_check=lambda: True,
        )

        assert result["status"] == "cancelled"
        mock_create.assert_not_called()


# ---------------------------------------------------------------------------
# Router HTTP tests (requires TestClient)
# ---------------------------------------------------------------------------

try:
    from fastapi.testclient import TestClient
    from src.main import app

    _client = TestClient(app, raise_server_exceptions=False)

    class TestBatchRoutes:
        def test_start_missing_country_id(self):
            r = _client.post(
                "/api/ai-offices/batch",
                json={"urls": ["https://example.com"], "defaults": {"level_id": 1, "branch_id": 1}},
            )
            assert r.status_code in (400, 503)

        def test_start_empty_urls(self):
            r = _client.post(
                "/api/ai-offices/batch",
                json={"urls": [], "defaults": {"country_id": 1, "level_id": 1, "branch_id": 1}},
            )
            assert r.status_code in (400, 503)

        def test_status_unknown_job(self):
            r = _client.get("/api/ai-offices/batch/nonexistent-uuid-1234/status")
            assert r.status_code == 404

except Exception:
    # TestClient or app import may fail in CI without DB; skip gracefully
    pass
