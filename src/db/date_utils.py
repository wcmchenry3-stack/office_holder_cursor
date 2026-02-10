"""Shared date normalization for individuals and office_terms. Ensures date columns are either null or YYYY-MM-DD."""

import re

_VALID_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def normalize_date(value: str | None) -> tuple[str | None, bool]:
    """Return (date_or_none, imprecise). If value is invalid or not YYYY-MM-DD, return (None, True)."""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return (None, True)
    s = value.strip()
    if s == "Invalid date":
        return (None, True)
    if not _VALID_DATE.match(s):
        return (None, True)
    return (s, False)
