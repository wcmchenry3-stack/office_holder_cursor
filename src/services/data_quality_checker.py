# -*- coding: utf-8 -*-
"""Multi-AI data quality checking pipeline.

Runs sequential checks: deterministic → OpenAI → Gemini → Claude.
Short-circuits on the first step that flags a concern. Missing AI clients
(key not set) are silently skipped.

Uses the collect/flush pattern from ParseErrorReporter.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

from pydantic import BaseModel

from src.db import data_quality_reports as db_dqr
from src.db.connection import get_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

MAX_BATCH_SIZE = 50


class QualityCheckRequest(BaseModel):
    record_type: str  # "individual" or "office_term"
    record_id: int
    record_data: dict


class QualityCheckResult(BaseModel):
    record_type: str
    record_id: int
    check_type: str
    flagged_by: str  # "deterministic", "openai", "gemini", "claude"
    concerns: list[str]
    confidence: str = "high"


# ---------------------------------------------------------------------------
# Deterministic checks (no API calls)
# ---------------------------------------------------------------------------


def _check_suspicious_dates(record_data: dict) -> QualityCheckResult | None:
    """Check for impossible or suspicious date values."""
    concerns = []

    term_start = record_data.get("term_start_year")
    term_end = record_data.get("term_end_year")
    if term_start and term_end:
        if term_end < term_start:
            concerns.append(
                f"term_end_year ({term_end}) is before term_start_year ({term_start})"
            )
        if term_end - term_start > 80:
            concerns.append(
                f"Term span ({term_end - term_start} years) exceeds 80 years"
            )

    if term_end and term_end > 2030:
        concerns.append(f"term_end_year ({term_end}) is in the far future")
    if term_start and term_start > 2030:
        concerns.append(f"term_start_year ({term_start}) is in the far future")

    if concerns:
        return QualityCheckResult(
            record_type=record_data.get("record_type", "office_term"),
            record_id=record_data.get("record_id", 0),
            check_type="bad_dates",
            flagged_by="deterministic",
            concerns=concerns,
        )
    return None


def _check_missing_wiki_url(record_data: dict) -> str | None:
    """Return 'missing_wiki_url' if the wiki_url is missing or a dead placeholder."""
    wiki_url = record_data.get("wiki_url", "")
    if not wiki_url or wiki_url.startswith("No link:"):
        return "missing_wiki_url"
    return None


def _check_party_resolution(record_data: dict) -> QualityCheckResult | None:
    """Flag when party text exists but party_id is NULL."""
    party_text = record_data.get("party_text")
    party_id = record_data.get("party_id")
    if party_text and party_id is None:
        return QualityCheckResult(
            record_type=record_data.get("record_type", "office_term"),
            record_id=record_data.get("record_id", 0),
            check_type="party_resolution_failure",
            flagged_by="deterministic",
            concerns=[f"Party text '{party_text}' exists but party_id is NULL"],
        )
    return None


def _run_deterministic_checks(record_data: dict) -> QualityCheckResult | None:
    """Run all deterministic checks. Returns first failure or None."""
    result = _check_suspicious_dates(record_data)
    if result:
        return result

    result = _check_party_resolution(record_data)
    if result:
        return result

    return None


# ---------------------------------------------------------------------------
# AI pipeline
# ---------------------------------------------------------------------------


def _run_ai_pipeline(
    record_type: str, record_id: int, record_data: dict, check_type: str
) -> QualityCheckResult | None:
    """Run OpenAI → Gemini → Claude pipeline. Short-circuits on first failure."""
    prompt = _build_quality_prompt(record_data, check_type)
    context = {k: v for k, v in record_data.items() if v is not None}

    # Step 1: OpenAI
    result = _check_with_openai(prompt, context)
    if result and not result.get("is_valid", True):
        return QualityCheckResult(
            record_type=record_type,
            record_id=record_id,
            check_type=check_type,
            flagged_by="openai",
            concerns=result.get("concerns", []),
            confidence=result.get("confidence", "medium"),
        )

    # Step 2: Gemini
    result = _check_with_gemini(prompt, context)
    if result and not result.get("is_valid", True):
        return QualityCheckResult(
            record_type=record_type,
            record_id=record_id,
            check_type=check_type,
            flagged_by="gemini",
            concerns=result.get("concerns", []),
            confidence=result.get("confidence", "medium"),
        )

    # Step 3: Claude
    result = _check_with_claude(prompt, context)
    if result and not result.get("is_valid", True):
        return QualityCheckResult(
            record_type=record_type,
            record_id=record_id,
            check_type=check_type,
            flagged_by="claude",
            concerns=result.get("concerns", []),
            confidence=result.get("confidence", "medium"),
        )

    return None


def _build_quality_prompt(record_data: dict, check_type: str) -> str:
    """Build a prompt for AI quality checking."""
    lines = [f"Data quality check type: {check_type}", "Record data:"]
    for key, value in record_data.items():
        if value is not None:
            lines.append(f"  {key}: {value}")
    return "\n".join(lines)


def _check_with_openai(prompt: str, context: dict) -> dict | None:
    """Check with OpenAI. Returns None if client not available."""
    try:
        from src.services.orchestrator import get_ai_builder

        builder = get_ai_builder()
        if builder is None:
            return None
        # Use a simple completion for quality checking
        import openai

        response = builder._client.chat.completions.create(
            model="gpt-4o-mini",
            max_completion_tokens=512,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a data quality analyst. Assess the record and return JSON: "
                        '{"is_valid": bool, "concerns": [str], "confidence": "high"|"medium"|"low"}'
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        import json

        text = response.choices[0].message.content or ""
        return json.loads(text)
    except Exception:
        logger.exception("OpenAI quality check failed")
        return None


def _check_with_gemini(prompt: str, context: dict) -> dict | None:
    """Check with Gemini. Returns None if client not available."""
    try:
        from src.services.gemini_vitals_researcher import get_gemini_researcher

        researcher = get_gemini_researcher()
        if researcher is None:
            return None
        return researcher.check_data_quality(prompt)
    except Exception:
        logger.exception("Gemini quality check failed")
        return None


def _check_with_claude(prompt: str, context: dict) -> dict | None:
    """Check with Claude. Returns None if client not available."""
    try:
        from src.services.claude_client import get_claude_client

        client = get_claude_client()
        if client is None:
            return None
        result = client.check_data_quality(prompt, context)
        return result.model_dump()
    except Exception:
        logger.exception("Claude quality check failed")
        return None


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


class DataQualityChecker:
    """Multi-AI data quality pipeline with collect/flush pattern."""

    def __init__(self):
        self._buffer: list[QualityCheckRequest] = []
        self._lock = threading.Lock()

    def collect(self, record_type: str, record_data: dict) -> None:
        """Buffer a record for quality checking. Thread-safe."""
        with self._lock:
            self._buffer.append(
                QualityCheckRequest(
                    record_type=record_type,
                    record_id=record_data.get("record_id", 0),
                    record_data=record_data,
                )
            )

    def flush(self, conn=None) -> list[QualityCheckResult]:
        """Run pipeline on all buffered records. Called at end-of-run."""
        with self._lock:
            requests = list(self._buffer)
            self._buffer.clear()

        if not requests:
            return []

        own_conn = conn is None
        if own_conn:
            conn = get_connection()

        try:
            return self._process_batch(requests[:MAX_BATCH_SIZE], conn)
        except Exception:
            logger.exception("Data quality flush failed")
            return []
        finally:
            if own_conn:
                conn.close()

    def run_manual(self, conn=None) -> list[QualityCheckResult]:
        """Run quality checks on eligible records from the DB (manual run mode)."""
        own_conn = conn is None
        if own_conn:
            conn = get_connection()

        try:
            requests = self._query_eligible_records(conn)
            if not requests:
                return []
            return self._process_batch(requests[:MAX_BATCH_SIZE], conn)
        except Exception:
            logger.exception("Data quality manual run failed")
            return []
        finally:
            if own_conn:
                conn.close()

    def _process_batch(
        self, requests: list[QualityCheckRequest], conn
    ) -> list[QualityCheckResult]:
        """Process a batch of requests through the pipeline."""
        results: list[QualityCheckResult] = []

        for req in requests:
            # Dedup: skip records already flagged
            fingerprint = db_dqr.make_fingerprint(
                req.record_type,
                req.record_id,
                self._infer_check_type(req.record_data),
            )
            if db_dqr.find_by_fingerprint(fingerprint, conn=conn) is not None:
                continue

            result = self._check_one(req)
            if result:
                # Persist to DB
                fp = db_dqr.make_fingerprint(
                    result.record_type, result.record_id, result.check_type
                )
                db_dqr.insert_report(
                    fingerprint=fp,
                    record_type=result.record_type,
                    record_id=result.record_id,
                    check_type=result.check_type,
                    flagged_by=result.flagged_by,
                    concern_details="; ".join(result.concerns),
                    conn=conn,
                )
                results.append(result)

        return results

    def _check_one(self, req: QualityCheckRequest) -> QualityCheckResult | None:
        """Run the full pipeline on one record."""
        data = req.record_data

        # Phase 1: deterministic checks
        det_result = _run_deterministic_checks(data)
        if det_result:
            det_result.record_type = req.record_type
            det_result.record_id = req.record_id
            return det_result

        # Phase 2: AI pipeline (only for checks that need it)
        check_type = self._infer_check_type(data)
        if check_type in ("missing_wiki_url", "incomplete_individual", "url_mismatch"):
            ai_result = _run_ai_pipeline(
                req.record_type, req.record_id, data, check_type
            )
            if ai_result:
                return ai_result

        return None

    @staticmethod
    def _infer_check_type(record_data: dict) -> str:
        """Infer the primary check type from the record data."""
        if _check_missing_wiki_url(record_data):
            return "missing_wiki_url"

        full_name = record_data.get("full_name")
        if not full_name:
            return "incomplete_individual"

        return "general"

    @staticmethod
    def _query_eligible_records(conn) -> list[QualityCheckRequest]:
        """Find records in the DB that are eligible for quality checking."""
        requests: list[QualityCheckRequest] = []

        # Find individuals with missing wiki URLs
        cur = conn.execute(
            "SELECT id, wiki_url, full_name FROM individuals"
            " WHERE (wiki_url LIKE %s OR wiki_url = %s)"
            " LIMIT %s",
            ("No link:%", "", MAX_BATCH_SIZE),
        )
        for row in cur.fetchall():
            requests.append(
                QualityCheckRequest(
                    record_type="individual",
                    record_id=row[0],
                    record_data={
                        "record_type": "individual",
                        "record_id": row[0],
                        "wiki_url": row[1],
                        "full_name": row[2],
                    },
                )
            )

        return requests
