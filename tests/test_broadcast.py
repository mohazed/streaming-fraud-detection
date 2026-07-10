"""Trap E guard tests. Pure asyncio - no Kafka, no FastAPI.
"""
from __future__ import annotations

import asyncio
import time

import pytest

from api.broadcast import Broadcaster


@pytest.mark.asyncio
async def test_subscribe_then_publish():
    b = Broadcaster()
    q = b.subscribe()

    b.publish({"n": 1})

    assert await asyncio.wait_for(q.get(), timeout=0.1) == {"n": 1}


@pytest.mark.asyncio
async def test_two_subscribers_both_receive():
    b = Broadcaster()
    q1 = b.subscribe()
    q2 = b.subscribe()

    b.publish({"n": 1})

    assert await asyncio.wait_for(q1.get(), timeout=0.1) == {"n": 1}
    assert await asyncio.wait_for(q2.get(), timeout=0.1) == {"n": 1}


@pytest.mark.asyncio
async def test_unsubscribe_stops_delivery():
    b = Broadcaster()
    q = b.subscribe()
    b.unsubscribe(q)

    b.publish({"n": 1})

    assert q.empty()


@pytest.mark.asyncio
async def test_queue_full_drops_oldest():
    b = Broadcaster(maxsize=3)
    q = b.subscribe()
    for i in range(3):
        b.publish(i)
    assert q.qsize() == 3

    b.publish(99)

    assert q.qsize() == 3
    assert b.dropped == 1
    assert [q.get_nowait() for _ in range(3)] == [1, 2, 99]


@pytest.mark.asyncio
async def test_publish_never_blocks():
    b = Broadcaster(maxsize=1)
    b.subscribe()  # never read from

    start = time.perf_counter()
    for _ in range(10):
        b.publish("x")
    elapsed = time.perf_counter() - start

    assert elapsed < 0.01


@pytest.mark.asyncio
async def test_slow_subscriber_does_not_affect_fast_one():
    b = Broadcaster(maxsize=1)
    slow = b.subscribe()  # never drained
    fast = b.subscribe()

    for i in range(5):
        b.publish(i)
        assert fast.get_nowait() == i

    assert b.dropped >= 1
    assert slow.qsize() == 1
