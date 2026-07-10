"""Trap B guard: suppress re-emitted alerts from growing update-mode windows.

`outputMode("update")` re-emits a window every micro-batch its aggregate
changes. A user's 4th, 5th, 6th transaction in one velocity window each
re-trigger the `count >= 4` filter, producing the same alert repeatedly.
`AlertDeduper` remembers `(rule, user_id, window_start)` keys it has already
let through and evicts them once they are older than `ttl_seconds`. Plain
Python, no Spark, so it is unit-testable standalone and safe to hold as
driver-local state across streaming batches.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Callable, Hashable

DEFAULT_TTL_SECONDS = 600.0


class AlertDeduper:
    def __init__(self, ttl_seconds: float = DEFAULT_TTL_SECONDS,
                 now: Callable[[], float] = time.monotonic) -> None:
        self._ttl = ttl_seconds
        self._now = now
        self._seen: set[tuple[Hashable, Hashable, Hashable]] = set()
        self._order: deque[tuple[float, tuple]] = deque()

    def should_emit(self, rule: str, user_id: str, window_start: Hashable) -> bool:
        """True (and remembers the key) the first time `key` is seen within the
        TTL; False on every repeat until the key is evicted."""
        self._evict()
        key = (rule, user_id, window_start)
        if key in self._seen:
            return False
        self._seen.add(key)
        self._order.append((self._now(), key))
        return True

    def _evict(self) -> None:
        cutoff = self._now() - self._ttl
        while self._order and self._order[0][0] <= cutoff:
            _, key = self._order.popleft()
            self._seen.discard(key)

    def __len__(self) -> int:
        return len(self._seen)
