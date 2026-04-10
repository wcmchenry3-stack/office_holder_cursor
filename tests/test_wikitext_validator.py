# -*- coding: utf-8 -*-
"""Unit tests for src/services/wikitext_validator.py.

All checks are pure-Python (no I/O). Tests are grouped by check function and
cover both the happy path and each individual failure mode.
"""

from __future__ import annotations

import pytest

from src.services.wikitext_validator import (
    ValidationIssue,
    WikitextValidationResult,
    validate_wikitext,
)

# ---------------------------------------------------------------------------
# Minimal valid wikitext fixture
# ---------------------------------------------------------------------------

_VALID = """\
{{Infobox officeholder
| name       = Jane Doe
| birth_date = {{birth date|1970|01|15}}
}}

Jane Doe is a politician.<ref name="ex">{{cite web |url=https://example.com |title=Jane Doe}}</ref>

==Early life==
Born in Springfield.

==References==
{{reflist}}

[[Category:Living people]]
[[Category:Politicians]]
"""


# ---------------------------------------------------------------------------
# WikitextValidationResult properties
# ---------------------------------------------------------------------------


class TestValidationResult:
    def test_is_valid_true_when_no_issues(self):
        r = WikitextValidationResult()
        assert r.is_valid is True

    def test_is_valid_true_when_only_warnings(self):
        r = WikitextValidationResult(
            issues=[ValidationIssue(level="warning", code="w", message="w")]
        )
        assert r.is_valid is True

    def test_is_valid_false_when_errors_present(self):
        r = WikitextValidationResult(issues=[ValidationIssue(level="error", code="e", message="e")])
        assert r.is_valid is False

    def test_errors_property_filters_correctly(self):
        issues = [
            ValidationIssue(level="error", code="e1", message="e1"),
            ValidationIssue(level="warning", code="w1", message="w1"),
        ]
        r = WikitextValidationResult(issues=issues)
        assert len(r.errors) == 1
        assert r.errors[0].code == "e1"

    def test_warnings_property_filters_correctly(self):
        issues = [
            ValidationIssue(level="error", code="e1", message="e1"),
            ValidationIssue(level="warning", code="w1", message="w1"),
        ]
        r = WikitextValidationResult(issues=issues)
        assert len(r.warnings) == 1
        assert r.warnings[0].code == "w1"

    def test_as_dict_structure(self):
        issues = [
            ValidationIssue(level="error", code="missing_infobox", message="Missing infobox"),
            ValidationIssue(level="warning", code="unmatched_braces", message="Braces"),
        ]
        r = WikitextValidationResult(issues=issues)
        d = r.as_dict()
        assert d["is_valid"] is False
        assert len(d["errors"]) == 1
        assert d["errors"][0] == {"code": "missing_infobox", "message": "Missing infobox"}
        assert len(d["warnings"]) == 1
        assert d["warnings"][0]["code"] == "unmatched_braces"

    def test_as_dict_valid_article(self):
        r = WikitextValidationResult()
        d = r.as_dict()
        assert d == {"is_valid": True, "errors": [], "warnings": []}


# ---------------------------------------------------------------------------
# Full valid article passes all checks
# ---------------------------------------------------------------------------


class TestValidArticle:
    def test_valid_article_has_no_issues(self):
        r = validate_wikitext(_VALID)
        assert r.is_valid is True
        assert r.issues == []


# ---------------------------------------------------------------------------
# Individual error checks
# ---------------------------------------------------------------------------


class TestMissingInfobox:
    def test_missing_infobox_produces_error(self):
        text = _VALID.replace("{{Infobox officeholder", "{{Something else")
        r = validate_wikitext(text)
        codes = [i.code for i in r.errors]
        assert "missing_infobox" in codes

    def test_infobox_match_is_case_insensitive(self):
        text = _VALID.replace("{{Infobox officeholder", "{{infobox officeholder")
        r = validate_wikitext(text)
        assert r.is_valid is True

    def test_infobox_present_no_error(self):
        r = validate_wikitext(_VALID)
        codes = [i.code for i in r.errors]
        assert "missing_infobox" not in codes


class TestMissingRefs:
    def test_missing_ref_tag_produces_error(self):
        text = _VALID.replace("<ref", "REMOVED_REF")
        r = validate_wikitext(text)
        codes = [i.code for i in r.errors]
        assert "missing_refs" in codes

    def test_ref_present_no_error(self):
        r = validate_wikitext(_VALID)
        assert "missing_refs" not in [i.code for i in r.errors]


class TestMissingReflist:
    def test_missing_reflist_produces_error(self):
        text = _VALID.replace("{{reflist}}", "")
        r = validate_wikitext(text)
        assert "missing_reflist" in [i.code for i in r.errors]

    def test_reflist_match_is_case_insensitive(self):
        text = _VALID.replace("{{reflist}}", "{{Reflist}}")
        r = validate_wikitext(text)
        assert "missing_reflist" not in [i.code for i in r.errors]

    def test_reflist_present_no_error(self):
        r = validate_wikitext(_VALID)
        assert "missing_reflist" not in [i.code for i in r.errors]


class TestMissingReferencesSection:
    def test_missing_section_produces_error(self):
        text = _VALID.replace("==References==", "")
        r = validate_wikitext(text)
        assert "missing_references_section" in [i.code for i in r.errors]

    def test_section_with_spaces_passes(self):
        text = _VALID.replace("==References==", "== References ==")
        r = validate_wikitext(text)
        assert "missing_references_section" not in [i.code for i in r.errors]

    def test_references_section_present_no_error(self):
        r = validate_wikitext(_VALID)
        assert "missing_references_section" not in [i.code for i in r.errors]


class TestMissingCategories:
    def test_missing_category_produces_error(self):
        text = _VALID.replace("[[Category:", "[[NotACategory:")
        r = validate_wikitext(text)
        assert "missing_categories" in [i.code for i in r.errors]

    def test_category_present_no_error(self):
        r = validate_wikitext(_VALID)
        assert "missing_categories" not in [i.code for i in r.errors]


# ---------------------------------------------------------------------------
# Warning checks
# ---------------------------------------------------------------------------


class TestUnmatchedBraces:
    def test_balanced_braces_no_warning(self):
        r = validate_wikitext(_VALID)
        assert "unmatched_braces" not in [i.code for i in r.warnings]

    def test_extra_open_brace_produces_warning(self):
        text = _VALID + "\n{{"  # extra unclosed
        r = validate_wikitext(text)
        assert "unmatched_braces" in [i.code for i in r.warnings]

    def test_extra_close_brace_produces_warning(self):
        text = _VALID + "\n}}"  # extra closing
        r = validate_wikitext(text)
        assert "unmatched_braces" in [i.code for i in r.warnings]

    def test_nowiki_block_excluded_from_count(self):
        # <nowiki>{{</nowiki> should not count as an open brace
        text = _VALID + "\n<nowiki>{{</nowiki>"
        r = validate_wikitext(text)
        assert "unmatched_braces" not in [i.code for i in r.warnings]


class TestBirthDateTemplate:
    def test_birth_date_field_without_template_produces_warning(self):
        text = _VALID.replace("{{birth date|1970|01|15}}", "1970-01-15")
        r = validate_wikitext(text)
        assert "missing_birth_date_template" in [i.code for i in r.warnings]

    def test_birth_date_template_present_no_warning(self):
        r = validate_wikitext(_VALID)
        assert "missing_birth_date_template" not in [i.code for i in r.warnings]

    def test_no_birth_info_no_warning(self):
        # Remove all birth references
        text = """\
{{Infobox officeholder
| name = Bob
}}
Bob is a politician.<ref>http://example.com</ref>
==References==
{{reflist}}
[[Category:People]]
"""
        r = validate_wikitext(text)
        assert "missing_birth_date_template" not in [i.code for i in r.warnings]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_string_accumulates_errors(self):
        r = validate_wikitext("")
        codes = [i.code for i in r.errors]
        assert "missing_infobox" in codes
        assert "missing_refs" in codes
        assert "missing_reflist" in codes
        assert "missing_references_section" in codes
        assert "missing_categories" in codes

    def test_multiple_errors_all_accumulated(self):
        r = validate_wikitext("")
        assert len(r.errors) >= 5

    def test_never_raises(self):
        for text in ["", "   ", "\n\n", "random text", None.__class__.__name__]:
            validate_wikitext(text)  # must not raise
