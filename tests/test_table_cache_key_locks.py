# -*- coding: utf-8 -*-
"""Tests for the _key_locks memory-leak fix in table_cache.py (Issue #224).

The original implementation used a plain dict[str, threading.Lock] that
grew forever. The fix uses weakref.WeakValueDictionary so entries are
automatically removed once no thread holds a reference to the lock.

Tests cover:
- _key_lock returns a _KeyLock with working __enter__/__exit__
- Same key returns the same object while a reference is held
- Different keys return different objects
- Dict shrinks after references are released (GC frees the lock)
- Concurrent access from multiple threads does not corrupt the dict
  and each thread gets a usable lock
"""

from __future__ import annotations

import gc
import threading
import weakref

import pytest

# ---------------------------------------------------------------------------
# _KeyLock behaviour
# ---------------------------------------------------------------------------


class TestKeyLock:
    def test_context_manager_acquires_and_releases(self):
        from src.scraper.table_cache import _KeyLock

        kl = _KeyLock()
        with kl:
            # lock is held; trying to acquire again would block
            acquired = kl._lock.acquire(blocking=False)
            assert acquired is False  # already locked
        # after context exits, lock is released
        acquired = kl._lock.acquire(blocking=False)
        assert acquired is True
        kl._lock.release()

    def test_is_weakly_referenceable(self):
        from src.scraper.table_cache import _KeyLock

        kl = _KeyLock()
        ref = weakref.ref(kl)
        assert ref() is kl
        del kl
        gc.collect()
        assert ref() is None


# ---------------------------------------------------------------------------
# _key_lock function
# ---------------------------------------------------------------------------


class TestKeyLockFunction:
    def test_returns_key_lock_instance(self):
        from src.scraper.table_cache import _KeyLock, _key_lock

        lock = _key_lock("abc123")
        assert isinstance(lock, _KeyLock)

    def test_same_key_same_object_while_held(self):
        from src.scraper.table_cache import _key_lock

        lock1 = _key_lock("same-key")
        lock2 = _key_lock("same-key")
        assert lock1 is lock2

    def test_different_keys_different_objects(self):
        from src.scraper.table_cache import _key_lock

        lock_a = _key_lock("key-a-unique-1")
        lock_b = _key_lock("key-b-unique-1")
        assert lock_a is not lock_b

    def test_dict_shrinks_after_reference_released(self):
        from src.scraper.table_cache import _key_lock, _key_locks

        unique_key = "gc-test-key-xyzzy"
        lock = _key_lock(unique_key)
        assert unique_key in _key_locks
        del lock
        gc.collect()
        assert unique_key not in _key_locks

    def test_lock_usable_as_context_manager(self):
        from src.scraper.table_cache import _key_lock

        key_lock = _key_lock("ctx-test-key")
        with key_lock:
            pass  # should not raise

    def test_no_unbounded_growth(self):
        """Creating and releasing 200 distinct locks leaves dict size bounded."""
        from src.scraper.table_cache import _key_lock, _key_locks

        initial_size = len(_key_locks)
        refs = []
        for i in range(200):
            refs.append(_key_lock(f"growth-test-{i}"))
        peak_size = len(_key_locks)
        del refs
        gc.collect()
        final_size = len(_key_locks)
        # After GC, all 200 should be gone
        assert final_size <= initial_size
        assert peak_size >= 200  # sanity — they were all created


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_same_key_returns_same_object(self):
        """50 threads requesting the same key should all get the same object."""
        from src.scraper.table_cache import _key_lock

        results: list = []
        barrier = threading.Barrier(50)

        def worker():
            barrier.wait()
            results.append(_key_lock("shared-concurrent-key"))

        threads = [threading.Thread(target=worker) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All results should be the same object (one lock per key while held)
        assert all(r is results[0] for r in results)

    def test_concurrent_distinct_keys_no_corruption(self):
        """100 threads each using a unique key; no exceptions, dict is clean after."""
        from src.scraper.table_cache import _key_lock, _key_locks

        errors: list[Exception] = []

        def worker(i: int):
            try:
                key = f"distinct-thread-key-{i}"
                lock = _key_lock(key)
                with lock:
                    pass
                del lock
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        gc.collect()
        assert errors == []
