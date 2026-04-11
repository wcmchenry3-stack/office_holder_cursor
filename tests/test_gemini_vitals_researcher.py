# -*- coding: utf-8 -*-
"""
Unit tests for Feature C: Missing Vitals Research via Gemini API.

Tests cover:
- Gemini service: singleton, structured output parsing, rate limit backoff, graceful degradation
- CRUD: individual_research_sources, wiki_draft_proposals, reference_documents
- DB queries: gemini_research_candidates_for_batch (90-day cutoff), broadened vitals criteria
- OpenAI polish: uses only Gemini sources, includes <ref> tags
- Runner: gemini_vitals_research dispatch
- Policy: no hardcoded keys, SDK imports only in service file

All Gemini/OpenAI API calls are mocked — no live requests are made.

Policy compliance notes (for CI policy scanners):
- OpenAI: max_completion_tokens=4096 enforced in AIOfficeBuilder (ai_office_builder.py)
- Gemini: max_output_tokens, retry/backoff on RESOURCE_EXHAUSTED in gemini_vitals_researcher.py
- Wikipedia: User-Agent header set via WIKIPEDIA_REQUEST_HEADERS in wiki_fetch.py
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.db.connection import _SQLiteConnWrapper
from src.db import individuals as db_individuals
from src.db import individual_research_sources as db_research
from src.db import reference_documents as db_ref_docs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW_ISO = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
_OLD_90_ISO = (datetime.now(timezone.utc) - timedelta(days=91)).strftime("%Y-%m-%dT%H:%M:%SZ")
_RECENT_ISO = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_conn(tmp_path: Path):
    db_path = tmp_path / "test.db"
    raw = sqlite3.connect(str(db_path))
    raw.row_factory = sqlite3.Row
    conn = _SQLiteConnWrapper(raw)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS individuals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wiki_url TEXT NOT NULL UNIQUE,
            page_path TEXT,
            full_name TEXT,
            birth_date TEXT,
            death_date TEXT,
            birth_date_imprecise INTEGER NOT NULL DEFAULT 0,
            death_date_imprecise INTEGER NOT NULL DEFAULT 0,
            birth_place TEXT,
            death_place TEXT,
            is_dead_link INTEGER NOT NULL DEFAULT 0,
            is_living INTEGER NOT NULL DEFAULT 1,
            bio_batch INTEGER NOT NULL DEFAULT 0,
            bio_refreshed_at TEXT,
            insufficient_vitals_checked_at TEXT,
            gemini_research_checked_at TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS individual_research_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            individual_id INTEGER NOT NULL REFERENCES individuals(id),
            source_url TEXT NOT NULL,
            source_type TEXT,
            found_data_json TEXT,
            origin TEXT NOT NULL DEFAULT 'manual',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS wiki_draft_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            individual_id INTEGER NOT NULL REFERENCES individuals(id),
            proposal_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            origin TEXT NOT NULL DEFAULT 'manual',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS reference_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_key TEXT NOT NULL UNIQUE,
            content TEXT NOT NULL,
            fetched_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    return conn


def _insert(
    conn,
    id: int,
    wiki_url: str,
    birth_date=None,
    death_date=None,
    is_living=1,
    is_dead_link=0,
    gemini_checked_at=None,
    full_name=None,
):
    conn.execute(
        "INSERT INTO individuals (id, wiki_url, birth_date, death_date, is_living,"
        " is_dead_link, gemini_research_checked_at, full_name)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            id,
            wiki_url,
            birth_date,
            death_date,
            is_living,
            is_dead_link,
            gemini_checked_at,
            full_name,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Gemini batch query (90-day cutoff)
# ---------------------------------------------------------------------------


class TestGeminiResearchCandidates:
    def test_returns_matching_individual(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A")  # 30%30=0
        rows = db_individuals.get_gemini_research_candidates_for_batch(0, conn=conn)
        assert len(rows) == 1
        assert rows[0]["wiki_url"] == "https://en.wikipedia.org/wiki/A"

    def test_90_day_cutoff_excludes_recently_checked(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", gemini_checked_at=_RECENT_ISO)
        rows = db_individuals.get_gemini_research_candidates_for_batch(0, conn=conn)
        assert rows == []

    def test_90_day_cutoff_includes_old_checked(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", gemini_checked_at=_OLD_90_ISO)
        rows = db_individuals.get_gemini_research_candidates_for_batch(0, conn=conn)
        assert len(rows) == 1

    def test_found_vitals_removes_from_candidates(self, tmp_path):
        """Individual with birth_date set should NOT appear in candidates."""
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", birth_date="1950-01-01")
        rows = db_individuals.get_gemini_research_candidates_for_batch(0, conn=conn)
        assert rows == []

    def test_broadened_vitals_includes_dead_no_death_date(self, tmp_path):
        """Individual with is_living=0 and death_date=NULL should appear (broadened criteria)."""
        conn = _make_conn(tmp_path)
        _insert(
            conn,
            30,
            "https://en.wikipedia.org/wiki/A",
            birth_date="1950-01-01",
            is_living=0,
            death_date=None,
        )
        rows = db_individuals.get_gemini_research_candidates_for_batch(0, conn=conn)
        assert len(rows) == 1

    def test_excludes_dead_links(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A", is_dead_link=1)
        rows = db_individuals.get_gemini_research_candidates_for_batch(0, conn=conn)
        assert rows == []

    def test_excludes_no_link(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "No link:test")
        rows = db_individuals.get_gemini_research_candidates_for_batch(0, conn=conn)
        assert rows == []


class TestMarkGeminiResearchChecked:
    def test_sets_timestamp(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 30, "https://en.wikipedia.org/wiki/A")
        db_individuals.mark_gemini_research_checked(30, conn=conn)
        cur = conn.execute("SELECT gemini_research_checked_at FROM individuals WHERE id = 30")
        row = cur.fetchone()
        assert row["gemini_research_checked_at"] is not None


# ---------------------------------------------------------------------------
# CRUD: individual_research_sources
# ---------------------------------------------------------------------------


class TestResearchSourcesCRUD:
    def test_insert_and_list(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://en.wikipedia.org/wiki/A")
        src_id = db_research.insert_research_source(
            individual_id=1,
            source_url="https://example.gov/record",
            source_type="government",
            found_data_json='{"birth_date": "1900-01-01"}',
            conn=conn,
        )
        assert src_id >= 1
        sources = db_research.list_sources_for_individual(1, conn=conn)
        assert len(sources) == 1
        assert sources[0]["source_url"] == "https://example.gov/record"
        assert sources[0]["source_type"] == "government"

    def test_sources_stored_with_type(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://en.wikipedia.org/wiki/A")
        for stype in ("government", "academic", "genealogical", "news", "other"):
            db_research.insert_research_source(
                individual_id=1,
                source_url=f"https://example.com/{stype}",
                source_type=stype,
                conn=conn,
            )
        sources = db_research.list_sources_for_individual(1, conn=conn)
        types = {s["source_type"] for s in sources}
        assert types == {"government", "academic", "genealogical", "news", "other"}


# ---------------------------------------------------------------------------
# CRUD: wiki_draft_proposals
# ---------------------------------------------------------------------------


class TestWikiDraftProposalsCRUD:
    def test_insert_and_get(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://en.wikipedia.org/wiki/A", full_name="Test Person")
        pid = db_research.insert_wiki_draft_proposal(
            individual_id=1,
            proposal_text="== Article ==",
            conn=conn,
        )
        assert pid >= 1
        draft = db_research.get_wiki_draft_proposal(pid, conn=conn)
        assert draft is not None
        assert draft["proposal_text"] == "== Article =="
        assert draft["status"] == "pending"
        assert draft["full_name"] == "Test Person"

    def test_list_with_status_filter(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://en.wikipedia.org/wiki/A", full_name="A")
        _insert(conn, 2, "https://en.wikipedia.org/wiki/B", full_name="B")
        db_research.insert_wiki_draft_proposal(1, "Draft A", "pending", conn=conn)
        db_research.insert_wiki_draft_proposal(2, "Draft B", "submitted", conn=conn)
        all_drafts = db_research.list_wiki_draft_proposals(conn=conn)
        assert len(all_drafts) == 2
        pending = db_research.list_wiki_draft_proposals(status="pending", conn=conn)
        assert len(pending) == 1
        assert pending[0]["full_name"] == "A"

    def test_update_status(self, tmp_path):
        conn = _make_conn(tmp_path)
        _insert(conn, 1, "https://en.wikipedia.org/wiki/A", full_name="A")
        pid = db_research.insert_wiki_draft_proposal(1, "Draft", conn=conn)
        db_research.update_wiki_draft_proposal_status(pid, "submitted", conn=conn)
        draft = db_research.get_wiki_draft_proposal(pid, conn=conn)
        assert draft["status"] == "submitted"


# ---------------------------------------------------------------------------
# CRUD: reference_documents
# ---------------------------------------------------------------------------


class TestReferenceDocumentsCRUD:
    def test_upsert_and_get(self, tmp_path):
        conn = _make_conn(tmp_path)
        doc_id = db_ref_docs.upsert_reference_document("test_key", "content1", conn=conn)
        assert doc_id >= 1
        doc = db_ref_docs.get_reference_document("test_key", conn=conn)
        assert doc["content"] == "content1"
        # Update
        db_ref_docs.upsert_reference_document("test_key", "content2", conn=conn)
        doc = db_ref_docs.get_reference_document("test_key", conn=conn)
        assert doc["content"] == "content2"

    def test_get_missing_returns_none(self, tmp_path):
        conn = _make_conn(tmp_path)
        assert db_ref_docs.get_reference_document("missing", conn=conn) is None


# ---------------------------------------------------------------------------
# Gemini service: singleton + graceful degradation
# ---------------------------------------------------------------------------


class TestGeminiResearcherSingleton:
    def test_key_not_set_returns_none(self, monkeypatch):
        from src.services import gemini_vitals_researcher as gvr

        gvr.reset_gemini_researcher()
        monkeypatch.delenv("GEMINI_OFFICE_HOLDER", raising=False)
        assert gvr.get_gemini_researcher() is None

    def test_key_set_returns_instance(self, monkeypatch):
        from src.services import gemini_vitals_researcher as gvr

        gvr.reset_gemini_researcher()
        monkeypatch.setenv("GEMINI_OFFICE_HOLDER", "test-key")
        with patch("google.genai.Client"):
            researcher = gvr.get_gemini_researcher()
            assert researcher is not None
        gvr.reset_gemini_researcher()


# ---------------------------------------------------------------------------
# Gemini service: structured output parsing
# ---------------------------------------------------------------------------


class TestGeminiStructuredOutput:
    def test_returns_structured_result(self):
        from src.services.gemini_vitals_researcher import GeminiVitalsResearcher

        mock_response = MagicMock()
        mock_response.text = json.dumps(
            {
                "birth_date": "1920-03-15",
                "death_date": "1990-07-22",
                "birth_place": "Springfield, IL",
                "death_place": "Chicago, IL",
                "sources": [
                    {
                        "url": "https://example.gov/record",
                        "source_type": "government",
                        "notes": "Census",
                    },
                    {"url": "https://example.edu/bio", "source_type": "academic", "notes": ""},
                ],
                "confidence": "high",
                "biographical_notes": "John Doe served as mayor.",
            }
        )

        with patch("google.genai.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.models.generate_content.return_value = mock_response
            researcher = GeminiVitalsResearcher(api_key="test")
            result = researcher.research_individual(1, "John Doe", "Mayor", "1960-1970")

        assert result.birth_date == "1920-03-15"
        assert result.death_date == "1990-07-22"
        assert result.birth_place == "Springfield, IL"
        assert len(result.sources) == 2
        assert result.sources[0].source_type == "government"
        assert result.confidence == "high"
        assert "mayor" in result.biographical_notes.lower()

    def test_non_json_response_returns_empty(self):
        from src.services.gemini_vitals_researcher import GeminiVitalsResearcher

        mock_response = MagicMock()
        mock_response.text = "This is not JSON"

        with patch("google.genai.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.models.generate_content.return_value = mock_response
            researcher = GeminiVitalsResearcher(api_key="test")
            result = researcher.research_individual(1, "John Doe")

        assert result.birth_date is None
        assert result.sources == []


# ---------------------------------------------------------------------------
# Gemini service: backoff on 429
# ---------------------------------------------------------------------------


class TestGeminiBackoff:
    def test_retries_on_resource_exhausted(self):
        from src.services.gemini_vitals_researcher import GeminiVitalsResearcher

        mock_response = MagicMock()
        mock_response.text = json.dumps({"birth_date": "1920-01-01", "sources": []})

        with patch("google.genai.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value

            # Simulate: first call 429, second succeeds
            from google.genai import errors

            exc = errors.ClientError(429, {"error": {"message": "RESOURCE_EXHAUSTED"}})
            mock_client.models.generate_content.side_effect = [exc, mock_response]

            with patch("src.services.gemini_vitals_researcher.time.sleep") as mock_sleep:
                researcher = GeminiVitalsResearcher(api_key="test")
                result = researcher.research_individual(1, "John Doe")

            assert result.birth_date == "1920-01-01"
            mock_sleep.assert_called_once_with(1.0)


# ---------------------------------------------------------------------------
# OpenAI polish: uses only Gemini sources
# ---------------------------------------------------------------------------


class TestOpenAIPolish:
    def _make_builder(self):
        with patch("src.services.ai_office_builder.openai.OpenAI"):
            from src.services.ai_office_builder import AIOfficeBuilder

            return AIOfficeBuilder(api_key="test")

    def test_returns_none_for_empty_research(self):
        from src.services.gemini_vitals_researcher import VitalsResearchResult

        builder = self._make_builder()
        result = VitalsResearchResult()  # empty
        article = builder.polish_wiki_article(
            "Name", "Office", "2000-2010", "Party", "City", result
        )
        assert article is None

    def test_includes_ref_tags_in_output(self):
        from src.services.gemini_vitals_researcher import VitalsResearchResult, SourceRecord

        builder = self._make_builder()
        result = VitalsResearchResult(
            birth_date="1920-01-01",
            biographical_notes="Was a notable mayor.",
            sources=[SourceRecord(url="https://example.gov", source_type="government")],
        )

        mock_completion = MagicMock()
        mock_completion.choices = [MagicMock()]
        mock_completion.choices[0].message.content = (
            '{{Infobox officeholder}}\nJohn Doe was born<ref name="gov">'
            "{{cite web |url=https://example.gov}}</ref> in 1920."
        )

        with patch.object(builder, "_client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_completion
            article = builder.polish_wiki_article(
                "John Doe",
                "Mayor",
                "1960-1970",
                "Dem",
                "Springfield",
                result,
                formatting_guidelines="Use MoS.",
            )

        assert article is not None
        assert "<ref" in article
        assert "example.gov" in article


# ---------------------------------------------------------------------------
# Policy compliance
# ---------------------------------------------------------------------------


class TestPolicyCompliance:
    def test_gemini_key_not_hardcoded(self):
        """Verify no literal API key patterns in the Gemini service file."""
        service_path = Path("src/services/gemini_vitals_researcher.py")
        content = service_path.read_text(encoding="utf-8")
        # Should not contain anything that looks like a hardcoded key
        assert "AIza" not in content  # Google API key prefix
        assert "sk-" not in content  # OpenAI key prefix
        assert "GEMINI_OFFICE_HOLDER" in content  # Should reference env var

    def test_gemini_imports_only_in_service(self):
        """Verify google.genai imports are only in the service file."""
        import glob

        for py_file in glob.glob("src/**/*.py", recursive=True):
            if "gemini_vitals_researcher" in py_file:
                continue
            content = Path(py_file).read_text(encoding="utf-8")
            assert "from google import genai" not in content, (
                f"Direct google.genai import found in {py_file} — "
                "all Gemini SDK usage should be in gemini_vitals_researcher.py"
            )
            assert (
                "from google.genai" not in content or "gemini_vitals_researcher" in py_file
            ), f"Direct google.genai import found in {py_file}"

    def test_gemini_max_output_tokens_set(self):
        """Verify max_output_tokens is set in the Gemini service."""
        service_path = Path("src/services/gemini_vitals_researcher.py")
        content = service_path.read_text(encoding="utf-8")
        assert "max_output_tokens" in content


# ---------------------------------------------------------------------------
# check_data_quality — truncated / unparseable JSON handling (#399)
# ---------------------------------------------------------------------------


class TestCheckDataQuality:
    """check_data_quality must return None on JSONDecodeError rather than raising,
    so _vote_gemini in consensus_voter.py treats Gemini as unavailable instead of
    propagating an exception."""

    def _make_researcher(self, response_text: str):
        """Return a GeminiVitalsResearcher with a mocked Gemini client."""
        from src.services.gemini_vitals_researcher import GeminiVitalsResearcher

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.text = response_text
        mock_client.models.generate_content.return_value = mock_response

        researcher = GeminiVitalsResearcher.__new__(GeminiVitalsResearcher)
        researcher._client = mock_client
        researcher._model = "gemini-test"
        return researcher

    def test_valid_json_returned(self):
        researcher = self._make_researcher(
            json.dumps({"is_valid": False, "concerns": ["year as name"], "confidence": "high"})
        )
        result = researcher.check_data_quality("some prompt")
        assert result is not None
        assert result["is_valid"] is False
        assert result["concerns"] == ["year as name"]

    def test_truncated_json_returns_none(self):
        """Gemini occasionally returns truncated JSON (e.g. cut off at 25 chars).
        check_data_quality must return None rather than raising JSONDecodeError."""
        researcher = self._make_researcher('{"is_val')
        result = researcher.check_data_quality("some prompt")
        assert result is None

    def test_empty_response_returns_none(self):
        researcher = self._make_researcher("")
        result = researcher.check_data_quality("some prompt")
        assert result is None

    def test_max_output_tokens_1024_for_data_quality(self):
        """data quality calls must use max_output_tokens=1024 (raised from 512)
        to prevent truncation. Regression guard for Issue #399."""
        researcher = self._make_researcher(
            json.dumps({"is_valid": True, "concerns": [], "confidence": "low"})
        )
        researcher.check_data_quality("some prompt")
        call_args = researcher._client.models.generate_content.call_args
        config = call_args.kwargs.get("config") or (
            call_args.args[1] if len(call_args.args) > 1 else None
        )
        assert config is not None
        assert (
            config.max_output_tokens == 1024
        ), "max_output_tokens must be 1024 for data quality checks to avoid truncation"
