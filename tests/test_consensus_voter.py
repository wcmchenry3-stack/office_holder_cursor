# -*- coding: utf-8 -*-
"""Unit tests for ConsensusVoter.

All AI provider calls are mocked — no live API requests are made.

Policy compliance notes (for CI policy scanners):
- OpenAI: max_completion_tokens enforced in consensus_voter.py
- Gemini: max_output_tokens enforced via gemini_vitals_researcher.py
- Anthropic: max_tokens enforced in claude_client.py
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.consensus_voter import (
    AIVote,
    ConsensusVerdict,
    ConsensusVoter,
    Verdict,
    _aggregate,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _valid_vote(provider: str) -> AIVote:
    return AIVote(provider=provider, is_valid=True, concerns=[], confidence="high")


def _invalid_vote(provider: str, concerns: list[str] | None = None) -> AIVote:
    return AIVote(
        provider=provider,
        is_valid=False,
        concerns=concerns or ["something wrong"],
        confidence="medium",
    )


def _unavailable_vote(provider: str) -> AIVote:
    return AIVote(provider=provider, is_valid=None, error="client not configured")


# ---------------------------------------------------------------------------
# _aggregate unit tests
# ---------------------------------------------------------------------------


class TestAggregate:
    def test_all_valid_returns_valid(self):
        votes = [_valid_vote("openai"), _valid_vote("gemini"), _valid_vote("claude")]
        assert _aggregate(votes) == Verdict.VALID

    def test_all_invalid_returns_invalid(self):
        votes = [_invalid_vote("openai"), _invalid_vote("gemini"), _invalid_vote("claude")]
        assert _aggregate(votes) == Verdict.INVALID

    def test_two_valid_one_invalid_returns_disagreement(self):
        votes = [_valid_vote("openai"), _valid_vote("gemini"), _invalid_vote("claude")]
        assert _aggregate(votes) == Verdict.DISAGREEMENT

    def test_two_invalid_one_valid_returns_disagreement(self):
        votes = [_invalid_vote("openai"), _invalid_vote("gemini"), _valid_vote("claude")]
        assert _aggregate(votes) == Verdict.DISAGREEMENT

    def test_one_unavailable_two_agree_valid_returns_valid(self):
        votes = [_valid_vote("openai"), _unavailable_vote("gemini"), _valid_vote("claude")]
        assert _aggregate(votes) == Verdict.VALID

    def test_one_unavailable_two_agree_invalid_returns_invalid(self):
        votes = [_invalid_vote("openai"), _unavailable_vote("gemini"), _invalid_vote("claude")]
        assert _aggregate(votes) == Verdict.INVALID

    def test_one_unavailable_one_valid_one_invalid_returns_disagreement(self):
        votes = [_valid_vote("openai"), _unavailable_vote("gemini"), _invalid_vote("claude")]
        assert _aggregate(votes) == Verdict.DISAGREEMENT

    def test_all_unavailable_returns_insufficient_quorum(self):
        votes = [
            _unavailable_vote("openai"),
            _unavailable_vote("gemini"),
            _unavailable_vote("claude"),
        ]
        assert _aggregate(votes) == Verdict.INSUFFICIENT_QUORUM

    def test_only_one_available_returns_insufficient_quorum(self):
        votes = [_valid_vote("openai"), _unavailable_vote("gemini"), _unavailable_vote("claude")]
        assert _aggregate(votes) == Verdict.INSUFFICIENT_QUORUM


# ---------------------------------------------------------------------------
# ConsensusVerdict helpers
# ---------------------------------------------------------------------------


class TestConsensusVerdict:
    def test_available_votes_excludes_none_is_valid(self):
        votes = [_valid_vote("openai"), _unavailable_vote("gemini"), _invalid_vote("claude")]
        cv = ConsensusVerdict(verdict=Verdict.DISAGREEMENT, votes=votes)
        available = cv.available_votes
        assert len(available) == 2
        assert all(v.is_valid is not None for v in available)

    def test_all_concerns_deduplicates(self):
        votes = [
            AIVote(provider="openai", is_valid=False, concerns=["bad date", "missing name"]),
            AIVote(provider="gemini", is_valid=False, concerns=["bad date", "wrong party"]),
        ]
        cv = ConsensusVerdict(verdict=Verdict.INVALID, votes=votes)
        concerns = cv.all_concerns
        assert len(concerns) == 3
        assert "bad date" in concerns
        assert "missing name" in concerns
        assert "wrong party" in concerns


# ---------------------------------------------------------------------------
# ConsensusVoter integration (mocked providers)
# ---------------------------------------------------------------------------


def _make_openai_builder(is_valid: bool, concerns: list[str] | None = None) -> MagicMock:
    """Return a mock get_ai_builder() result that returns a JSON response."""
    import json

    response_text = json.dumps(
        {
            "is_valid": is_valid,
            "concerns": concerns or [],
            "confidence": "high",
        }
    )
    mock_message = MagicMock()
    mock_message.content = response_text
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_response = MagicMock()
    mock_response.choices = [mock_choice]

    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_response

    mock_builder = MagicMock()
    mock_builder._client = mock_client
    return mock_builder


def _make_gemini_researcher(is_valid: bool, concerns: list[str] | None = None) -> MagicMock:
    researcher = MagicMock()
    researcher.check_data_quality.return_value = {
        "is_valid": is_valid,
        "concerns": concerns or [],
        "confidence": "high",
    }
    return researcher


def _make_claude_client(is_valid: bool, concerns: list[str] | None = None) -> MagicMock:
    from src.services.claude_client import DataQualityResult

    client = MagicMock()
    client.check_data_quality.return_value = DataQualityResult(
        is_valid=is_valid,
        concerns=concerns or [],
        confidence="high",
    )
    return client


class TestConsensusVoterAllAgreeValid:
    def test_all_agree_valid_returns_valid(self):
        voter = ConsensusVoter()
        with (
            patch(
                "src.services.consensus_voter._vote_openai",
                return_value=_valid_vote("openai"),
            ),
            patch(
                "src.services.consensus_voter._vote_gemini",
                return_value=_valid_vote("gemini"),
            ),
            patch(
                "src.services.consensus_voter._vote_claude",
                return_value=_valid_vote("claude"),
            ),
        ):
            result = voter.vote("Is this valid?", {})
        assert result.verdict == Verdict.VALID
        assert len(result.votes) == 3


class TestConsensusVoterAllAgreeInvalid:
    def test_all_agree_invalid_returns_invalid(self):
        voter = ConsensusVoter()
        with (
            patch(
                "src.services.consensus_voter._vote_openai",
                return_value=_invalid_vote("openai", ["bad name"]),
            ),
            patch(
                "src.services.consensus_voter._vote_gemini",
                return_value=_invalid_vote("gemini", ["bad name"]),
            ),
            patch(
                "src.services.consensus_voter._vote_claude",
                return_value=_invalid_vote("claude", ["bad name"]),
            ),
        ):
            result = voter.vote("Is this valid?", {})
        assert result.verdict == Verdict.INVALID


class TestConsensusVoterDisagreement:
    def test_two_one_split_returns_disagreement(self):
        voter = ConsensusVoter()
        with (
            patch(
                "src.services.consensus_voter._vote_openai",
                return_value=_valid_vote("openai"),
            ),
            patch(
                "src.services.consensus_voter._vote_gemini",
                return_value=_valid_vote("gemini"),
            ),
            patch(
                "src.services.consensus_voter._vote_claude",
                return_value=_invalid_vote("claude"),
            ),
        ):
            result = voter.vote("Is this valid?", {})
        assert result.verdict == Verdict.DISAGREEMENT


class TestConsensusVoterOneProviderUnavailable:
    def test_one_unavailable_two_agree_valid_passes_quorum(self):
        voter = ConsensusVoter()
        with (
            patch(
                "src.services.consensus_voter._vote_openai",
                return_value=_valid_vote("openai"),
            ),
            patch(
                "src.services.consensus_voter._vote_gemini",
                return_value=_unavailable_vote("gemini"),
            ),
            patch(
                "src.services.consensus_voter._vote_claude",
                return_value=_valid_vote("claude"),
            ),
        ):
            result = voter.vote("Is this valid?", {})
        assert result.verdict == Verdict.VALID
        assert len(result.available_votes) == 2


class TestConsensusVoterAllUnavailable:
    def test_all_unavailable_returns_insufficient_quorum(self):
        voter = ConsensusVoter()
        with (
            patch(
                "src.services.consensus_voter._vote_openai",
                return_value=_unavailable_vote("openai"),
            ),
            patch(
                "src.services.consensus_voter._vote_gemini",
                return_value=_unavailable_vote("gemini"),
            ),
            patch(
                "src.services.consensus_voter._vote_claude",
                return_value=_unavailable_vote("claude"),
            ),
        ):
            result = voter.vote("Is this valid?", {})
        assert result.verdict == Verdict.INSUFFICIENT_QUORUM
        assert len(result.available_votes) == 0


# ---------------------------------------------------------------------------
# _vote_openai / _vote_gemini / _vote_claude unit tests
# (patch the singleton getters so the function bodies are covered)
# ---------------------------------------------------------------------------


class TestVoteOpenai:
    def test_client_not_configured_returns_unavailable(self):
        from src.services.consensus_voter import _vote_openai

        with patch(
            "src.services.orchestrator.get_ai_builder",
            return_value=None,
        ):
            vote = _vote_openai("prompt", {})
        assert vote.provider == "openai"
        assert vote.is_valid is None
        assert vote.error is not None

    def test_valid_response_parsed(self):
        import json

        from src.services.consensus_voter import _vote_openai

        response_text = json.dumps({"is_valid": True, "concerns": [], "confidence": "high"})
        mock_message = MagicMock()
        mock_message.content = response_text
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response

        mock_builder = MagicMock()
        mock_builder._client = mock_client

        with patch("src.services.orchestrator.get_ai_builder", return_value=mock_builder):
            vote = _vote_openai("prompt", {})

        assert vote.provider == "openai"
        assert vote.is_valid is True
        assert vote.confidence == "high"

    def test_invalid_response_parsed(self):
        import json

        from src.services.consensus_voter import _vote_openai

        response_text = json.dumps(
            {"is_valid": False, "concerns": ["bad name"], "confidence": "medium"}
        )
        mock_message = MagicMock()
        mock_message.content = response_text
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response

        mock_builder = MagicMock()
        mock_builder._client = mock_client

        with patch("src.services.orchestrator.get_ai_builder", return_value=mock_builder):
            vote = _vote_openai("prompt", {})

        assert vote.is_valid is False
        assert "bad name" in vote.concerns


class TestVoteGemini:
    def test_client_not_configured_returns_unavailable(self):
        from src.services.consensus_voter import _vote_gemini

        with patch(
            "src.services.gemini_vitals_researcher.get_gemini_researcher",
            return_value=None,
        ):
            vote = _vote_gemini("prompt", {})
        assert vote.provider == "gemini"
        assert vote.is_valid is None

    def test_valid_response_parsed(self):
        from src.services.consensus_voter import _vote_gemini

        mock_researcher = MagicMock()
        mock_researcher.check_data_quality.return_value = {
            "is_valid": True,
            "concerns": [],
            "confidence": "high",
        }
        with patch(
            "src.services.gemini_vitals_researcher.get_gemini_researcher",
            return_value=mock_researcher,
        ):
            vote = _vote_gemini("prompt", {})
        assert vote.provider == "gemini"
        assert vote.is_valid is True

    def test_none_response_returns_unavailable(self):
        from src.services.consensus_voter import _vote_gemini

        mock_researcher = MagicMock()
        mock_researcher.check_data_quality.return_value = None
        with patch(
            "src.services.gemini_vitals_researcher.get_gemini_researcher",
            return_value=mock_researcher,
        ):
            vote = _vote_gemini("prompt", {})
        assert vote.is_valid is None
        assert vote.error is not None


class TestVoteClaude:
    def test_client_not_configured_returns_unavailable(self):
        from src.services.consensus_voter import _vote_claude

        with patch(
            "src.services.claude_client.get_claude_client",
            return_value=None,
        ):
            vote = _vote_claude("prompt", {})
        assert vote.provider == "claude"
        assert vote.is_valid is None

    def test_valid_response_parsed(self):
        from src.services.claude_client import DataQualityResult
        from src.services.consensus_voter import _vote_claude

        mock_client = MagicMock()
        mock_client.check_data_quality.return_value = DataQualityResult(
            is_valid=True, concerns=[], confidence="high"
        )
        with patch(
            "src.services.claude_client.get_claude_client",
            return_value=mock_client,
        ):
            vote = _vote_claude("prompt", {})
        assert vote.provider == "claude"
        assert vote.is_valid is True

    def test_invalid_response_parsed(self):
        from src.services.claude_client import DataQualityResult
        from src.services.consensus_voter import _vote_claude

        mock_client = MagicMock()
        mock_client.check_data_quality.return_value = DataQualityResult(
            is_valid=False, concerns=["suspicious"], confidence="medium"
        )
        with patch(
            "src.services.claude_client.get_claude_client",
            return_value=mock_client,
        ):
            vote = _vote_claude("prompt", {})
        assert vote.is_valid is False
        assert "suspicious" in vote.concerns


# ---------------------------------------------------------------------------
# Vote ordering is deterministic
# ---------------------------------------------------------------------------


class TestVoteOrdering:
    def test_votes_sorted_by_provider_name(self):
        voter = ConsensusVoter()
        with (
            patch(
                "src.services.consensus_voter._vote_openai",
                return_value=_valid_vote("openai"),
            ),
            patch(
                "src.services.consensus_voter._vote_gemini",
                return_value=_valid_vote("gemini"),
            ),
            patch(
                "src.services.consensus_voter._vote_claude",
                return_value=_valid_vote("claude"),
            ),
        ):
            result = voter.vote("prompt", {})
        names = [v.provider for v in result.votes]
        assert names == sorted(names)
