# -*- coding: utf-8 -*-
"""Logging helpers for the scraper.

configure_run_logging() attaches a per-run timestamped FileHandler to the
'src.scraper' logger hierarchy for the duration of one scraper job.  The
caller is responsible for removing and closing the handler when the job ends.

Log level is controlled by the SCRAPER_LOG_LEVEL env var (default: INFO).
Set DEBUG for local diagnosis, WARNING in prod for clean-run silence.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

HTTP_USER_AGENT = (
    "OfficeHolder/1.0 (https://github.com/wcmchenry3-stack/office-holder; wcmchenry3@gmail.com)"
)

_SCRAPER_LOGGER = "src.scraper"


def configure_run_logging(
    process: str,
    run_type: str,
    log_dir: Path | str | None = None,
) -> logging.FileHandler:
    """Attach a timestamped FileHandler to the src.scraper logger hierarchy.

    Returns the handler so the caller can remove it when the job finishes:

        handler = configure_run_logging("Office", run_type, log_dir)
        try:
            ...
        finally:
            logging.getLogger("src.scraper").removeHandler(handler)
            handler.close()

    Per-run log files are written to log_dir (defaults to get_log_dir()).
    """
    if log_dir is None:
        from src.db.connection import get_log_dir

        log_dir = get_log_dir()

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = log_dir / f"{process}_{run_type}_{timestamp}.txt"

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))

    level_name = os.getenv("SCRAPER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    handler.setLevel(level)

    scraper_logger = logging.getLogger(_SCRAPER_LOGGER)
    scraper_logger.addHandler(handler)
    # Ensure the logger itself passes DEBUG through so the handler can filter.
    if scraper_logger.level == logging.NOTSET or scraper_logger.level > logging.DEBUG:
        scraper_logger.setLevel(logging.DEBUG)

    return handler
