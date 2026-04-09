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

    def test_none_result_returns_parse_error_unavailable(self):
        """When check_data_quality returns None (e.g. code-fenced JSON that still
        fails to parse), _vote_claude must return is_valid=None so the provider
        is excluded from quorum rather than voting 'valid'."""
        from src.services.consensus_voter import _vote_claude

        mock_client = MagicMock()
        mock_client.check_data_quality.return_value = None
        with patch(
            "src.services.claude_client.get_claude_client",
            return_value=mock_client,
        ):
            vote = _vote_claude("prompt", {})
        assert vote.provider == "claude"
        assert vote.is_valid is None
        assert vote.error == "parse error"


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


# ---------------------------------------------------------------------------
# System prompt alignment — Claude and Gemini must receive _SYSTEM_PROMPT
# ---------------------------------------------------------------------------


class TestSystemPromptContent:
    def test_system_prompt_contains_year_as_name_directive(self):
        """_SYSTEM_PROMPT must explicitly instruct providers to reject year-as-name
        records. Regression guard for Issue #398."""
        from src.services.consensus_voter import _SYSTEM_PROMPT

        assert "4-digit year" in _SYSTEM_PROMPT, (
            "_SYSTEM_PROMPT must contain explicit year-as-name rejection rule"
        )
        assert "is_valid=false" in _SYSTEM_PROMPT, (
            "_SYSTEM_PROMPT must explicitly say to return is_valid=false for year-as-name"
        )


class TestSystemPromptAlignment:
    """_vote_claude and _vote_gemini must pass the shared _SYSTEM_PROMPT so all
    three providers evaluate the page quality question under the same framing.
    Regression guard for Issue #274."""

    def test_vote_claude_passes_shared_system_prompt(self):
        from src.services.claude_client import DataQualityResult
        from src.services.consensus_voter import _SYSTEM_PROMPT, _vote_claude

        mock_client = MagicMock()
        mock_client.check_data_quality.return_value = DataQualityResult(
            is_valid=True, concerns=[], confidence="high"
        )
        with patch("src.services.claude_client.get_claude_client", return_value=mock_client):
            _vote_claude("some prompt", {})

        call_kwargs = mock_client.check_data_quality.call_args
        assert call_kwargs is not None
        passed_system = call_kwargs.kwargs.get("system_prompt") or (
            call_kwargs.args[2] if len(call_kwargs.args) > 2 else None
        )
        assert (
            passed_system == _SYSTEM_PROMPT
        ), "Claude must use the shared consensus _SYSTEM_PROMPT, not the individual-record prompt"

    def test_vote_gemini_passes_shared_system_prompt(self):
        from src.services.consensus_voter import _SYSTEM_PROMPT, _vote_gemini

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
            _vote_gemini("some prompt", {})

        call_kwargs = mock_researcher.check_data_quality.call_args
        assert call_kwargs is not None
        passed_system = call_kwargs.kwargs.get("system_prompt") or (
            call_kwargs.args[1] if len(call_kwargs.args) > 1 else None
        )
        assert (
            passed_system == _SYSTEM_PROMPT
        ), "Gemini must use the shared consensus _SYSTEM_PROMPT, not its vitals-research prompt"


# ---------------------------------------------------------------------------
# #373 — sub-millisecond overshoot: done futures not yielded by as_completed
# must still be collected via the drain loop
# ---------------------------------------------------------------------------


class TestConsensusVoterSubmsTimeoutDrain:
    """When as_completed raises TimeoutError due to a sub-ms clock overshoot,
    any futures that are already done must be drained rather than silently dropped."""

    def test_done_futures_drained_after_timeout_overshoot(self):
        """as_completed yields 2 of 3 futures then raises TimeoutError; the 3rd
        is already done — all 3 votes must appear in the result."""
        from concurrent.futures import TimeoutError as FuturesTimeoutError

        voter = ConsensusVoter()

        def fake_as_completed(fs, timeout=None):
            fs_list = list(fs)
            # Yield all but the last, then simulate the sub-ms clock overshoot
            for f in fs_list[:-1]:
                yield f
            raise FuturesTimeoutError("sub-ms overshoot simulation")

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
            patch("src.services.consensus_voter.as_completed", fake_as_completed),
        ):
            result = voter.vote("prompt", {})

        assert len(result.votes) == 3, (
            "All 3 votes must be collected even when as_completed raises TimeoutError "
            f"due to sub-ms overshoot; got {len(result.votes)}"
        )
        assert result.verdict == Verdict.VALID

    def test_truly_timed_out_futures_get_timeout_vote(self):
        """Futures that are genuinely not done when TimeoutError fires must receive
        a timeout AIVote rather than being silently dropped."""
        from concurrent.futures import Future, TimeoutError as FuturesTimeoutError

        voter = ConsensusVoter()

        # A future that will never complete (simulates genuine timeout)
        stuck_future: Future = Future()

        original_submit_calls: list = []

        def fake_as_completed(fs, timeout=None):
            fs_list = list(fs)
            # Yield the first two, leave the third (stuck_future) pending
            for f in fs_list[:-1]:
                yield f
            raise FuturesTimeoutError("genuine timeout simulation")

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
            patch("src.services.consensus_voter.as_completed", fake_as_completed),
            patch("src.services.consensus_voter.ThreadPoolExecutor") as mock_executor_cls,
        ):
            # Build 3 pre-resolved futures for the first 2 providers + stuck for the 3rd
            done_f1: Future = Future()
            done_f1.set_result(_valid_vote("openai"))
            done_f2: Future = Future()
            done_f2.set_result(_valid_vote("gemini"))

            submit_returns = [done_f1, done_f2, stuck_future]
            submit_idx = [0]

            mock_executor = MagicMock()
            mock_executor_cls.return_value.__enter__.return_value = mock_executor

            def fake_submit(fn, *args, **kwargs):
                f = submit_returns[submit_idx[0]]
                submit_idx[0] += 1
                return f

            mock_executor.submit.side_effect = fake_submit

            result = voter.vote("prompt", {})

        assert (
            len(result.votes) == 3
        ), f"Expect 3 votes (2 valid + 1 timeout), got {len(result.votes)}"
        timeout_votes = [v for v in result.votes if v.error and "timed out" in v.error]
        assert len(timeout_votes) == 1, "Genuinely stuck future must produce a timeout vote"
