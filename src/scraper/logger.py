# -*- coding: utf-8 -*-
"""File-based logger for scraper (no Colab/Drive)."""

HTTP_USER_AGENT = "OfficeHolder/1.0 (https://github.com/wcmchenry3-stack/office-holder; wcmchenry3@gmail.com)"

from datetime import datetime
import os
from pathlib import Path
import threading


def get_default_log_dir() -> Path:
    from ..db.connection import get_log_dir

    return get_log_dir()


class Logger:
    def __init__(self, run_type: str, process: str, log_dir: Path | str | None = None):
        self.run_type = run_type
        self.process = process
        if log_dir is None:
            log_dir = get_default_log_dir()
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file_name = f"{process}_{run_type}_{timestamp}.txt"
        self.log_file_path = log_dir / log_file_name
        self.log_file = open(self.log_file_path, "w", encoding="utf-8")
        self._lock = threading.Lock()
        print(f"using log file: {self.log_file_path}")

    def log(self, message: str, print_flag: bool) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n\n"
        with self._lock:
            self.log_file.write(formatted_message)
            self.log_file.flush()
        if print_flag and self.run_type:
            try:
                print(f"{message}\n\n")
            except UnicodeEncodeError:
                safe = message.encode("ascii", errors="replace").decode("ascii")
                print(f"{safe}\n\n")

    def debug_log(self, message: str, print_flag: bool) -> None:
        if self.run_type == "test run":
            self.log(message, print_flag)

    def close(self) -> None:
        if self.log_file:
            self.log_file.close()
