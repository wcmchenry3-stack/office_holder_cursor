"""Run-level in-memory page cache for deduplicating Wikipedia HTTP calls within a single run."""

from __future__ import annotations

import threading
from collections import OrderedDict


class RunPageCache:
    """Thread-safe LRU cache: {fetch_url: full_html_text}. Max 300 entries (~24MB).

    Stores the full HTML response for a Wikipedia REST API URL so that multiple
    tables on the same page (or the same person's infobox fetched again during
    bio refresh) only require one HTTP call per run.
    """

    def __init__(self, max_entries: int = 300):
        self._max = max_entries
        self._store: OrderedDict[str, str] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, url: str) -> str | None:
        with self._lock:
            if url not in self._store:
                return None
            self._store.move_to_end(url)
            return self._store[url]

    def set(self, url: str, html: str) -> None:
        with self._lock:
            if url in self._store:
                self._store.move_to_end(url)
            self._store[url] = html
            if len(self._store) > self._max:
                self._store.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)
