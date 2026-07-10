"""Kafka consumer thread feeding the SSE dashboard. Consumer group is unique
per process (`dashboard-{uuid4()}`) with `auto.offset.reset=latest` so two API
processes started by an accidental `--reload` fork each see the whole
`fraud-alerts` stream instead of racing over its one partition - Trap D.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from collections import deque
from typing import Any, Callable, Optional

from confluent_kafka import Consumer

from common.config import Config
from common.contracts import ALERT_SCHEMA

_ALERT_FIELDS = frozenset(ALERT_SCHEMA.fieldNames())

ConsumerFactory = Callable[[dict], Any]
DEFAULT_BUFFER_SIZE = 500


def _build_conf(config: Config) -> dict:
    return {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "group.id": f"dashboard-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
    }


class AlertConsumer:
    """Reads `config.topic_alerts` on a daemon thread, keeps the newest
    `buffer_size` valid alerts (newest first), and hands each one to
    `on_alert`. A malformed message increments `invalid_count` and is
    otherwise ignored - it never kills the thread."""

    def __init__(self, config: Config, on_alert: Callable[[dict], None],
                 loop: Optional[asyncio.AbstractEventLoop] = None,
                 consumer_factory: Optional[ConsumerFactory] = None,
                 buffer_size: int = DEFAULT_BUFFER_SIZE) -> None:
        self._on_alert = on_alert
        self._loop = loop
        self._buffer: deque[dict] = deque(maxlen=buffer_size)
        self.invalid_count = 0
        self.alerts_seen = 0
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        factory = consumer_factory or (lambda conf: Consumer(conf))
        self.conf = _build_conf(config)
        self._consumer = factory(self.conf)
        self._consumer.subscribe([config.topic_alerts])

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True,
                                         name="alert-consumer")
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._consumer.close()

    @property
    def alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def snapshot(self, limit: Optional[int] = None) -> list[dict]:
        items = list(self._buffer)
        return items[:limit] if limit is not None else items

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                msg = self._consumer.poll(1.0)
            except Exception:
                continue
            if msg is None:
                time.sleep(0.01)
                continue
            try:
                if msg.error():
                    continue
                self._handle(msg.value())
            except Exception:
                pass  # the consumer thread must never die

    def _handle(self, raw: Optional[bytes]) -> None:
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict) or not _ALERT_FIELDS.issubset(payload.keys()):
                raise ValueError("alert missing required fields")
        except (json.JSONDecodeError, ValueError, TypeError):
            self.invalid_count += 1
            return
        self.alerts_seen += 1
        self._buffer.appendleft(payload)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._on_alert, payload)
        else:
            self._on_alert(payload)
