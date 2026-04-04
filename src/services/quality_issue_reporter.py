# -*- coding: utf-8 -*-
"""Stub reporter for data quality issues.

Full GitHub issue creation will be implemented in #187. This stub logs
quality results so the interface is wired into the runner and ready.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class QualityIssueReporter:
    """Report data quality issues. Currently logs only (GitHub integration in #187)."""

    def report(self, results: list, conn=None) -> int:
        """Log quality results. Returns the number of issues reported.

        Args:
            results: List of QualityCheckResult objects from DataQualityChecker.
            conn: DB connection (unused in stub, reserved for #187).

        Returns:
            Number of issues reported (logged).
        """
        if not results:
            return 0

        for r in results:
            logger.info(
                "Data quality issue: %s #%d [%s] flagged by %s — %s",
                r.record_type,
                r.record_id,
                r.check_type,
                r.flagged_by,
                "; ".join(r.concerns),
            )

        logger.info(
            "Data quality: %d issue(s) logged (GitHub reporting pending #187).", len(results)
        )
        return len(results)
