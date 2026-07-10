"""Runtime assertion: if a query reports numInputRows == 0 for 60 consecutive
seconds while the producer runs, log WARNING: stream starved - check
watermark/offsets. Deliberately not a StreamingQueryListener - a plain
polling loop over `query.lastProgress` is enough and keeps the monitor pure
and independently testable.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Mapping

from pyspark.sql.streaming import StreamingQuery

log = logging.getLogger(__name__)

DEFAULT_THRESHOLD_SECONDS = 60.0
DEFAULT_POLL_INTERVAL_SECONDS = 5.0


class StreamHealthMonitor:
    """Pure, injectable-clock. `record(name, num_input_rows)` per poll; warns
    once per starvation episode, and can warn again after recovery."""

    def __init__(self, threshold_seconds: float = DEFAULT_THRESHOLD_SECONDS,
                 now: Callable[[], float] = time.monotonic) -> None:
        self._threshold = threshold_seconds
        self._now = now
        self._last_nonzero: dict[str, float] = {}
        self._warned: set[str] = set()

    def record(self, name: str, num_input_rows: int) -> None:
        t = self._now()
        if num_input_rows > 0:
            self._last_nonzero[name] = t
            self._warned.discard(name)
            return
        idle_since = self._last_nonzero.setdefault(name, t)
        if t - idle_since >= self._threshold and name not in self._warned:
            log.warning("stream starved - check watermark/offsets: query=%s idle=%.0fs",
                        name, t - idle_since)
            self._warned.add(name)


def poll_forever(queries: Mapping[str, StreamingQuery], stop_event,
                  monitor: StreamHealthMonitor | None = None,
                  poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS) -> None:
    """Thin adapter: reads real StreamingQuery.lastProgress on a loop. Not
    unit-tested directly (needs a live query) - StreamHealthMonitor.record()
    carries all the logic and is tested standalone."""
    monitor = monitor or StreamHealthMonitor()
    while not stop_event.is_set():
        for name, query in queries.items():
            progress = query.lastProgress
            rows = progress["numInputRows"] if progress else 0
            monitor.record(name, rows)
        stop_event.wait(poll_interval)
