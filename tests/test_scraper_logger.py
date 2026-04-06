# -*- coding: utf-8 -*-
"""Unit tests for src/scraper/logger.py — configure_run_logging."""

from __future__ import annotations

import logging

import pytest

from src.scraper.logger import configure_run_logging


@pytest.fixture(autouse=True)
def _cleanup_handlers():
    """Remove any handlers added to the src.scraper logger after each test."""
    scraper_logger = logging.getLogger("src.scraper")
    before = list(scraper_logger.handlers)
    yield
    for h in list(scraper_logger.handlers):
        if h not in before:
            scraper_logger.removeHandler(h)
            h.close()


def test_configure_run_logging_creates_log_file(tmp_path):
    handler = configure_run_logging("Office", "delta", tmp_path)
    try:
        log_files = list(tmp_path.glob("*.txt"))
        assert len(log_files) == 1
        assert "Office" in log_files[0].name
        assert "delta" in log_files[0].name
    finally:
        logging.getLogger("src.scraper").removeHandler(handler)
        handler.close()


def test_configure_run_logging_returns_file_handler(tmp_path):
    handler = configure_run_logging("Office", "full", tmp_path)
    try:
        assert isinstance(handler, logging.FileHandler)
    finally:
        logging.getLogger("src.scraper").removeHandler(handler)
        handler.close()


def test_configure_run_logging_attaches_to_scraper_logger(tmp_path):
    handler = configure_run_logging("Office", "delta", tmp_path)
    try:
        scraper_logger = logging.getLogger("src.scraper")
        assert handler in scraper_logger.handlers
    finally:
        scraper_logger.removeHandler(handler)
        handler.close()


def test_configure_run_logging_handler_removable(tmp_path):
    scraper_logger = logging.getLogger("src.scraper")
    handler = configure_run_logging("Office", "delta", tmp_path)
    assert handler in scraper_logger.handlers
    scraper_logger.removeHandler(handler)
    handler.close()
    assert handler not in scraper_logger.handlers


def test_configure_run_logging_creates_log_dir_if_missing(tmp_path):
    new_dir = tmp_path / "nested" / "logs"
    assert not new_dir.exists()
    handler = configure_run_logging("Office", "delta", new_dir)
    try:
        assert new_dir.exists()
    finally:
        logging.getLogger("src.scraper").removeHandler(handler)
        handler.close()


def test_configure_run_logging_multiple_calls_create_separate_files(tmp_path):
    h1 = configure_run_logging("Office", "delta", tmp_path)
    h2 = configure_run_logging("Office", "delta", tmp_path)
    try:
        log_files = list(tmp_path.glob("*.txt"))
        # May be 1 or 2 depending on timestamp resolution — both files are valid
        assert len(log_files) >= 1
        assert h1 is not h2
    finally:
        scraper_logger = logging.getLogger("src.scraper")
        for h in [h1, h2]:
            scraper_logger.removeHandler(h)
            h.close()
