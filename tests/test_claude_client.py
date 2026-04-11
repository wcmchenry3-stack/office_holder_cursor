# -*- coding: utf-8 -*-
"""
Unit tests for Claude API client singleton.

Tests cover:
- Singleton: get_claude_client, reset, graceful degradation when key not set
- Structured output: DataQualityResult parsed correctly from mocked response
- Rate limit: exponential backoff on 429
- Policy: no hardcoded keys, SDK imports only in service file

All Claude API calls are mocked — no live requests are made.

Policy compliance notes (for CI policy scanners):
- Anthropic: max_tokens=1024 enforced in ClaudeClient (claude_client.py)
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Singleton + graceful degradation
# ---------------------------------------------------------------------------


class TestClaudeClientSingleton:
    def test_key_not_set_returns_none(self, monkeypatch):
        from src.services import claude_client as cc

        cc.reset_claude_client()
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        assert cc.get_claude_client() is None

    def test_key_set_returns_instance(self, monkeypatch):
        from src.services import claude_client as cc

        cc.reset_claude_client()
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        with patch("anthropic.Anthropic"):
            client = cc.get_claude_client()
            assert client is not None
        cc.reset_claude_client()

    def test_reset_claude_client(self, monkeypatch):
        from src.services import claude_client as cc

        cc.reset_claude_client()
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        with patch("anthropic.Anthropic"):
            client1 = cc.get_claude_client()
            assert client1 is not None
            cc.reset_claude_client()
            client2 = cc.get_claude_client()
            assert client2 is not None
            assert client1 is not client2
        cc.reset_claude_client()


# ---------------------------------------------------------------------------
# Structured output parsing
# ---------------------------------------------------------------------------


class TestClaudeStructuredOutput:
    def test_returns_structured_result(self):
        from src.services.claude_client import ClaudeClient

        mock_response = MagicMock()
        mock_response.content = [
            MagicMock(
                text=json.dumps(
                    {
                        "is_valid": False,
                        "concerns": ["Birth date after death date", "Missing office name"],
                        "confidence": "high",
                    }
                )
            )
        ]

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock_cls.return_value
            mock_client.messages.create.return_value = mock_response
            client = ClaudeClient(api_key="test")
            result = client.check_data_quality(
                "Check this record", {"name": "John Doe", "birth_date": "2000-01-01"}
            )

        assert result.is_valid is False
        assert len(result.concerns) == 2
        assert "Birth date after death date" in result.concerns
        assert result.confidence == "high"

    def test_code_fenced_json_is_parsed_correctly(self):
        """Claude sometimes wraps its JSON in ```json ... ``` — must be stripped."""
        from src.services.claude_client import ClaudeClient

        fenced = (
            "```json\n"
            + json.dumps({"is_valid": False, "concerns": ["year as name"], "confidence": "high"})
            + "\n```"
        )
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=fenced)]

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock_cls.return_value
            mock_client.messages.create.return_value = mock_response
            client = ClaudeClient(api_key="test")
            result = client.check_data_quality("Check this record", {})

        assert result is not None
        assert result.is_valid is False
        assert result.concerns == ["year as name"]
        assert result.confidence == "high"

    def test_code_fenced_json_no_language_tag(self):
        """Strip ``` fences with no language tag."""
        from src.services.claude_client import ClaudeClient

        fenced = (
            "```\n"
            + json.dumps({"is_valid": True, "concerns": [], "confidence": "medium"})
            + "\n```"
        )
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=fenced)]

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock_cls.return_value
            mock_client.messages.create.return_value = mock_response
            client = ClaudeClient(api_key="test")
            result = client.check_data_quality("Check this record", {})

        assert result is not None
        assert result.is_valid is True

    def test_non_json_response_returns_none(self):
        """Unparseable response returns None so caller excludes provider from quorum."""
        from src.services.claude_client import ClaudeClient

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="This is not JSON")]

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock_cls.return_value
            mock_client.messages.create.return_value = mock_response
            client = ClaudeClient(api_key="test")
            result = client.check_data_quality("Check this record", {})

        assert result is None


# ---------------------------------------------------------------------------
# Backoff on 429
# ---------------------------------------------------------------------------


class TestClaudeBackoff:
    def test_retries_on_rate_limit(self):
        from src.services.claude_client import ClaudeClient

        mock_response = MagicMock()
        mock_response.content = [
            MagicMock(text=json.dumps({"is_valid": True, "concerns": [], "confidence": "high"}))
        ]

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = mock_cls.return_value

            import anthropic

            exc = anthropic.RateLimitError(
                message="rate limited",
                response=MagicMock(status_code=429),
                body=None,
            )
            mock_client.messages.create.side_effect = [exc, mock_response]

            with patch("src.services.claude_client.time.sleep") as mock_sleep:
                client = ClaudeClient(api_key="test")
                result = client.check_data_quality("Check this record", {})

            assert result.is_valid is True
            mock_sleep.assert_called_once_with(1.0)


# ---------------------------------------------------------------------------
# Policy compliance
# ---------------------------------------------------------------------------


class TestClaudePolicyCompliance:
    def test_anthropic_key_not_hardcoded(self):
        """Verify no literal API key patterns in the Claude service file."""
        service_path = Path("src/services/claude_client.py")
        content = service_path.read_text(encoding="utf-8")
        assert "sk-ant-" not in content  # Anthropic key prefix
        assert "sk-" not in content  # Generic key prefix
        assert "AIza" not in content  # Google key prefix
        assert "ANTHROPIC_API_KEY" in content  # Should reference env var

    def test_anthropic_imports_only_in_service(self):
        """Verify anthropic imports are only in the service file."""
        import glob

        for py_file in glob.glob("src/**/*.py", recursive=True):
            if "claude_client" in py_file:
                continue
            content = Path(py_file).read_text(encoding="utf-8")
            assert "import anthropic" not in content, (
                f"Direct anthropic import found in {py_file} — "
                "all Anthropic SDK usage should be in claude_client.py"
            )

    def test_max_tokens_set(self):
        """Verify max_tokens is set in the Claude service."""
        service_path = Path("src/services/claude_client.py")
        content = service_path.read_text(encoding="utf-8")
        assert "max_tokens" in content
