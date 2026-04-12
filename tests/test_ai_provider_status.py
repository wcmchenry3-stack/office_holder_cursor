# -*- coding: utf-8 -*-
"""Unit tests for src/services/ai_provider_status.py.

All SDK calls are mocked — no live API requests are made.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.ai_provider_status import (
    ProviderCheckResult,
    _clear_probe_cache,
    check_providers,
    is_provider_enabled,
    poll_anthropic_balance,
    poll_gemini_balance,
    poll_openai_balance,
)


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear the probe cache before and after every test."""
    _clear_probe_cache()
    yield
    _clear_probe_cache()


# ---------------------------------------------------------------------------
# is_provider_enabled
# ---------------------------------------------------------------------------


class TestIsProviderEnabled:
    def test_enabled_by_default(self, monkeypatch):
        monkeypatch.delenv("GEMINI_ENABLED", raising=False)
        assert is_provider_enabled("gemini") is True

    @pytest.mark.parametrize("value", ["0", "false", "False", "FALSE", "no", "off"])
    def test_disabled_values(self, monkeypatch, value):
        monkeypatch.setenv("GEMINI_ENABLED", value)
        assert is_provider_enabled("gemini") is False

    @pytest.mark.parametrize("value", ["1", "true", "yes", "on", ""])
    def test_enabled_values(self, monkeypatch, value):
        monkeypatch.setenv("GEMINI_ENABLED", value)
        assert is_provider_enabled("gemini") is True

    def test_anthropic_reads_correct_var(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_ENABLED", "0")
        monkeypatch.delenv("GEMINI_ENABLED", raising=False)
        assert is_provider_enabled("anthropic") is False
        assert is_provider_enabled("gemini") is True

    def test_openai_reads_correct_var(self, monkeypatch):
        monkeypatch.setenv("OPENAI_ENABLED", "false")
        assert is_provider_enabled("openai") is False


# ---------------------------------------------------------------------------
# check_providers — kill switch behaviour
# ---------------------------------------------------------------------------


class TestCheckProvidersKillSwitch:
    def test_gemini_disabled_returns_not_available(self, monkeypatch):
        monkeypatch.setenv("GEMINI_ENABLED", "0")
        result = check_providers(["gemini"])
        assert result.all_available is False
        assert "gemini" in result.disabled_providers
        assert result.skip_reason is not None

    def test_anthropic_disabled_returns_not_available(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_ENABLED", "0")
        result = check_providers(["anthropic"])
        assert result.all_available is False
        assert "anthropic" in result.disabled_providers

    def test_openai_disabled_returns_not_available(self, monkeypatch):
        monkeypatch.setenv("OPENAI_ENABLED", "false")
        result = check_providers(["openai"])
        assert result.all_available is False
        assert "openai" in result.disabled_providers

    def test_disabled_provider_skips_remaining_probes(self, monkeypatch):
        """If first required provider is disabled, no balance probes are called."""
        monkeypatch.setenv("ANTHROPIC_ENABLED", "0")
        with patch("src.services.ai_provider_status.poll_openai_balance") as mock_probe:
            result = check_providers(["anthropic", "openai"])
        assert result.all_available is False
        mock_probe.assert_not_called()

    def test_second_provider_disabled_skips_first_probe(self, monkeypatch):
        """Kill-switch check happens before any probes — even for the first provider."""
        monkeypatch.setenv("OPENAI_ENABLED", "0")
        with patch("src.services.ai_provider_status.poll_gemini_balance") as mock_probe:
            result = check_providers(["gemini", "openai"])
        assert result.all_available is False
        mock_probe.assert_not_called()

    def test_all_enabled_with_mocked_probes_returns_available(self, monkeypatch):
        monkeypatch.delenv("GEMINI_ENABLED", raising=False)
        monkeypatch.delenv("OPENAI_ENABLED", raising=False)
        with (
            patch("src.services.ai_provider_status.poll_gemini_balance", return_value=True),
            patch("src.services.ai_provider_status.poll_openai_balance", return_value=True),
        ):
            result = check_providers(["gemini", "openai"])
        assert result.all_available is True
        assert result.disabled_providers == []
        assert result.exhausted_providers == []


# ---------------------------------------------------------------------------
# check_providers — exhaustion behaviour
# ---------------------------------------------------------------------------


class TestCheckProvidersExhaustion:
    def test_anthropic_rate_limit_error_marks_exhausted(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_ENABLED", raising=False)
        with patch("src.services.ai_provider_status.poll_anthropic_balance", return_value=False):
            result = check_providers(["anthropic"])
        assert result.all_available is False
        assert "anthropic" in result.exhausted_providers
        assert result.disabled_providers == []

    def test_openai_rate_limit_error_marks_exhausted(self, monkeypatch):
        monkeypatch.delenv("OPENAI_ENABLED", raising=False)
        with patch("src.services.ai_provider_status.poll_openai_balance", return_value=False):
            result = check_providers(["openai"])
        assert result.all_available is False
        assert "openai" in result.exhausted_providers

    def test_gemini_resource_exhausted_marks_exhausted(self, monkeypatch):
        monkeypatch.delenv("GEMINI_ENABLED", raising=False)
        with patch("src.services.ai_provider_status.poll_gemini_balance", return_value=False):
            result = check_providers(["gemini"])
        assert result.all_available is False
        assert "gemini" in result.exhausted_providers


# ---------------------------------------------------------------------------
# poll_anthropic_balance
# ---------------------------------------------------------------------------


class TestPollAnthropicBalance:
    def test_rate_limit_error_returns_false(self, monkeypatch):
        import anthropic

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = anthropic.RateLimitError(
            message="rate limited",
            response=MagicMock(status_code=429, headers={}),
            body=None,
        )
        with patch("anthropic.Anthropic", return_value=mock_client):
            result = poll_anthropic_balance()
        assert result is False

    def test_non_quota_error_returns_true(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = ConnectionError("network error")
        with patch("anthropic.Anthropic", return_value=mock_client):
            result = poll_anthropic_balance()
        assert result is True

    def test_success_returns_true(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        mock_client = MagicMock()
        with patch("anthropic.Anthropic", return_value=mock_client):
            result = poll_anthropic_balance()
        assert result is True

    def test_no_key_returns_true_without_probing(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        with patch("anthropic.Anthropic") as mock_cls:
            result = poll_anthropic_balance()
        assert result is True
        mock_cls.assert_not_called()

    def test_result_is_cached(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        mock_client = MagicMock()
        with patch("anthropic.Anthropic", return_value=mock_client):
            first = poll_anthropic_balance()
            second = poll_anthropic_balance()
        assert first == second
        assert mock_client.messages.create.call_count == 1


# ---------------------------------------------------------------------------
# poll_openai_balance
# ---------------------------------------------------------------------------


class TestPollOpenAIBalance:
    def test_rate_limit_error_returns_false(self, monkeypatch):
        import openai

        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = openai.RateLimitError(
            message="rate limited",
            response=MagicMock(status_code=429, headers={}),
            body=None,
        )
        with patch("openai.OpenAI", return_value=mock_client):
            result = poll_openai_balance()
        assert result is False

    def test_non_quota_error_returns_true(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = TimeoutError("timeout")
        with patch("openai.OpenAI", return_value=mock_client):
            result = poll_openai_balance()
        assert result is True

    def test_no_key_returns_true_without_probing(self, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        with patch("openai.OpenAI") as mock_cls:
            result = poll_openai_balance()
        assert result is True
        mock_cls.assert_not_called()

    def test_result_is_cached(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        mock_client = MagicMock()
        with patch("openai.OpenAI", return_value=mock_client):
            first = poll_openai_balance()
            second = poll_openai_balance()
        assert first == second
        assert mock_client.chat.completions.create.call_count == 1


# ---------------------------------------------------------------------------
# poll_gemini_balance
# ---------------------------------------------------------------------------


class TestPollGeminiBalance:
    def test_resource_exhausted_error_returns_false(self, monkeypatch):
        monkeypatch.setenv("GEMINI_OFFICE_HOLDER", "test-key")
        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = Exception(
            "RESOURCE_EXHAUSTED: quota exceeded"
        )
        with patch("google.genai.Client", return_value=mock_client):
            result = poll_gemini_balance()
        assert result is False

    def test_non_quota_error_returns_true(self, monkeypatch):
        monkeypatch.setenv("GEMINI_OFFICE_HOLDER", "test-key")
        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = ConnectionError("network")
        with patch("google.genai.Client", return_value=mock_client):
            result = poll_gemini_balance()
        assert result is True

    def test_no_key_returns_true_without_probing(self, monkeypatch):
        monkeypatch.delenv("GEMINI_OFFICE_HOLDER", raising=False)
        with patch("google.genai.Client") as mock_cls:
            result = poll_gemini_balance()
        assert result is True
        mock_cls.assert_not_called()

    def test_result_is_cached(self, monkeypatch):
        monkeypatch.setenv("GEMINI_OFFICE_HOLDER", "test-key")
        mock_client = MagicMock()
        with patch("google.genai.Client", return_value=mock_client):
            first = poll_gemini_balance()
            second = poll_gemini_balance()
        assert first == second
        assert mock_client.models.generate_content.call_count == 1


# ---------------------------------------------------------------------------
# ProviderCheckResult dataclass
# ---------------------------------------------------------------------------


class TestProviderCheckResult:
    def test_defaults(self):
        r = ProviderCheckResult(all_available=True)
        assert r.disabled_providers == []
        assert r.exhausted_providers == []
        assert r.skip_reason is None

    def test_skip_reason_populated_when_unavailable(self, monkeypatch):
        monkeypatch.setenv("GEMINI_ENABLED", "0")
        result = check_providers(["gemini"])
        assert result.skip_reason is not None
        assert "gemini" in result.skip_reason
