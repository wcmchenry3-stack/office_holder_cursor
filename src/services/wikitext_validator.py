# -*- coding: utf-8 -*-
"""Lightweight wikitext format validator.

Checks that AI-generated Wikipedia article drafts conform to the expected
wikitext structure before storage or submission.  All checks are pure-Python
regex operations — no I/O, no external dependencies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class ValidationIssue:
    level: str    # "error" | "warning"
    code: str     # machine-readable key, e.g. "missing_infobox"
    message: str  # human-readable one-liner


@dataclass
class WikitextValidationResult:
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.level == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.level == "warning"]

    @property
    def is_valid(self) -> bool:
        """True when there are no errors (warnings are acceptable)."""
        return len(self.errors) == 0

    def as_dict(self) -> dict:
        return {
            "is_valid": self.is_valid,
            "errors": [{"code": i.code, "message": i.message} for i in self.errors],
            "warnings": [{"code": i.code, "message": i.message} for i in self.warnings],
        }


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_infobox(wikitext: str, issues: list[ValidationIssue]) -> None:
    """Error if {{Infobox officeholder is absent (case-insensitive)."""
    if not re.search(r"\{\{infobox officeholder", wikitext, re.IGNORECASE):
        issues.append(ValidationIssue(
            level="error",
            code="missing_infobox",
            message="Missing {{Infobox officeholder}} template",
        ))


def _check_refs(wikitext: str, issues: list[ValidationIssue]) -> None:
    """Error if no <ref tag is present."""
    if not re.search(r"<ref", wikitext, re.IGNORECASE):
        issues.append(ValidationIssue(
            level="error",
            code="missing_refs",
            message="No <ref> citation tags found — every factual claim needs a citation",
        ))


def _check_reflist(wikitext: str, issues: list[ValidationIssue]) -> None:
    """Error if {{reflist}} is absent (case-insensitive)."""
    if not re.search(r"\{\{reflist", wikitext, re.IGNORECASE):
        issues.append(ValidationIssue(
            level="error",
            code="missing_reflist",
            message="Missing {{reflist}} in References section",
        ))


def _check_references_section(wikitext: str, issues: list[ValidationIssue]) -> None:
    """Error if ==References== section header is absent."""
    if not re.search(r"==\s*References\s*==", wikitext, re.IGNORECASE):
        issues.append(ValidationIssue(
            level="error",
            code="missing_references_section",
            message="Missing ==References== section header",
        ))


def _check_categories(wikitext: str, issues: list[ValidationIssue]) -> None:
    """Error if no [[Category: link is present."""
    if not re.search(r"\[\[Category:", wikitext, re.IGNORECASE):
        issues.append(ValidationIssue(
            level="error",
            code="missing_categories",
            message="No [[Category:...]] links found — article must be categorised",
        ))


def _check_unmatched_braces(wikitext: str, issues: list[ValidationIssue]) -> None:
    """Warning if {{ and }} counts differ (indicates broken template syntax).

    Strips <nowiki>...</nowiki> blocks first to avoid false positives from
    intentionally unbalanced markup in displayed examples.
    """
    stripped = re.sub(r"<nowiki>.*?</nowiki>", "", wikitext, flags=re.DOTALL | re.IGNORECASE)
    open_count = stripped.count("{{")
    close_count = stripped.count("}}")
    if open_count != close_count:
        delta = open_count - close_count
        direction = "unclosed" if delta > 0 else "extra closing"
        issues.append(ValidationIssue(
            level="warning",
            code="unmatched_braces",
            message=f"Unmatched template braces: {abs(delta)} {direction} '{{{{' detected",
        ))


def _check_birth_date_template(
    wikitext: str,
    has_birth_info: bool,
    issues: list[ValidationIssue],
) -> None:
    """Warning if birth info appears present but {{birth date| template is absent."""
    if not has_birth_info:
        return
    if not re.search(r"\{\{birth date", wikitext, re.IGNORECASE):
        issues.append(ValidationIssue(
            level="warning",
            code="missing_birth_date_template",
            message="birth_date field present but missing {{birth date|YYYY|MM|DD}} template",
        ))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate_wikitext(wikitext: str) -> WikitextValidationResult:
    """Run all checks against *wikitext* and return the aggregate result.

    Always returns a result — never raises.  An empty or None-like string will
    accumulate all error codes.
    """
    if not wikitext:
        wikitext = ""

    issues: list[ValidationIssue] = []

    _check_infobox(wikitext, issues)
    _check_refs(wikitext, issues)
    _check_reflist(wikitext, issues)
    _check_references_section(wikitext, issues)
    _check_categories(wikitext, issues)
    _check_unmatched_braces(wikitext, issues)

    # Derive has_birth_info heuristic
    has_birth_info = bool(
        re.search(r"\|\s*birth_date\s*=\s*\S", wikitext, re.IGNORECASE)
        or re.search(r"\bborn\s+\w", wikitext, re.IGNORECASE)
    )
    _check_birth_date_template(wikitext, has_birth_info, issues)

    return WikitextValidationResult(issues=issues)
