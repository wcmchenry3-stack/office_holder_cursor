# -*- coding: utf-8 -*-
"""Unit tests for src/db/date_utils.py — normalize_date."""

from __future__ import annotations

import pytest

from src.db.date_utils import normalize_date


@pytest.mark.parametrize(
    "value,expected_date,expected_imprecise",
    [
        # None / empty → imprecise
        (None, None, True),
        ("", None, True),
        ("   ", None, True),
        # Placeholder strings → imprecise
        ("present", None, True),
        ("Present", None, True),
        ("PRESENT", None, True),
        ("n/a", None, True),
        ("N/A", None, True),
        ("incumbent", None, True),
        ("invalid date", None, True),
        # Valid ISO date → returned as-is, not imprecise
        ("2000-01-01", "2000-01-01", False),
        ("1985-12-31", "1985-12-31", False),
        ("  2022-06-15  ", "2022-06-15", False),  # whitespace stripped
        # Non-ISO formats → imprecise
        ("Jan 1 2000", None, True),
        ("01/01/2000", None, True),
        ("2000/01/01", None, True),
        ("2000-1-1", None, True),  # missing leading zeros
        ("20000101", None, True),
        ("2000", None, True),
    ],
)
def test_normalize_date(value, expected_date, expected_imprecise):
    result_date, result_imprecise = normalize_date(value)
    assert result_date == expected_date
    assert result_imprecise == expected_imprecise


def test_normalize_date_returns_tuple():
    result = normalize_date("2001-03-04")
    assert isinstance(result, tuple)
    assert len(result) == 2
