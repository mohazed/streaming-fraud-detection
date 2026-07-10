"""Trap E guard: fan out alerts to every SSE subscriber without ever letting a
slow or idle browser tab block the producer.

Pure asyncio - no Kafka, no FastAPI - so it is unit-testable standalone.
"""
from __future__ import annotations

import asyncio
from typing import Any

QUEUE_MAXSIZE = 200


class Broadcaster:
    """Fan-out over a set of bounded asyncio.Queues. `publish()` is a plain
    synchronous function - it never awaits and therefore never blocks - so it
    is safe to call from anywhere already on the event loop thread (including
    via `loop.call_soon_threadsafe` from a foreign thread)."""

    def __init__(self, maxsize: int = QUEUE_MAXSIZE) -> None:
        self._maxsize = maxsize
        self._subscribers: set[asyncio.Queue] = set()
        self.dropped = 0

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    def publish(self, item: Any) -> None:
        """Deliver `item` to every subscriber. On a full queue, drop the
        oldest item to make room rather than blocking or dropping the new
        one - a stalled subscriber only loses history, never freshness."""
        for queue in self._subscribers:
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                    self.dropped += 1
                except asyncio.QueueEmpty:
                    pass
                queue.put_nowait(item)
