"""No broker: FakeKafkaConsumer yields scripted messages (Trap D).
"""
from __future__ import annotations

import json
import time

from common.config import load_config
from api.consumer import AlertConsumer
from tests.fakes import FakeKafkaConsumer

CONFIG = load_config({})


def _alert(transaction_id: str = "t-0000001") -> dict:
    return {
        "alert_id": "a-1", "transaction_id": transaction_id, "user_id": "u0001",
        "event_time": "2026-07-10T10:12:33Z", "alert_time": "2026-07-10T10:12:35Z",
        "rule": "velocity", "severity": "high",
        "amount": 185.20, "currency": "EUR", "amount_eur": 185.20,
        "location": "Paris", "country": "FR",
        "p_fraud": None,
        "detail": "5 transactions in window 10:12:00-10:13:00",
        "is_fraud": 1,
    }


def _alert_bytes(transaction_id: str = "t-0000001") -> bytes:
    return json.dumps(_alert(transaction_id)).encode("utf-8")


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    assert predicate(), "condition never became true within timeout"


def _make_consumer(messages: list[bytes], **kwargs) -> AlertConsumer:
    return AlertConsumer(CONFIG, on_alert=kwargs.pop("on_alert", lambda payload: None),
                          consumer_factory=lambda conf: FakeKafkaConsumer(messages),
                          **kwargs)


def test_valid_alerts_land_in_buffer():
    consumer = _make_consumer([_alert_bytes("t-1"), _alert_bytes("t-2")])
    consumer.start()
    try:
        _wait_until(lambda: consumer.alerts_seen == 2)
        assert len(consumer.snapshot()) == 2
    finally:
        consumer.stop()


def test_buffer_is_bounded():
    messages = [_alert_bytes(f"t-{i}") for i in range(1000)]
    consumer = _make_consumer(messages, buffer_size=500)
    consumer.start()
    try:
        _wait_until(lambda: consumer.alerts_seen == 1000, timeout=5.0)
        assert len(consumer.snapshot()) == 500
    finally:
        consumer.stop()


def test_newest_first():
    consumer = _make_consumer([_alert_bytes("t-1"), _alert_bytes("t-2"), _alert_bytes("t-3")])
    consumer.start()
    try:
        _wait_until(lambda: consumer.alerts_seen == 3)
        snapshot = consumer.snapshot()
        assert snapshot[0]["transaction_id"] == "t-3"
        assert snapshot[-1]["transaction_id"] == "t-1"
    finally:
        consumer.stop()


def test_invalid_json_increments_counter_and_does_not_raise():
    consumer = _make_consumer([b"not json", _alert_bytes("t-1")])
    consumer.start()
    try:
        _wait_until(lambda: consumer.alerts_seen == 1)
        assert consumer.invalid_count == 1
        assert consumer.snapshot()[0]["transaction_id"] == "t-1"
        assert consumer.alive
    finally:
        consumer.stop()


def test_group_id_is_unique_per_instance():
    c1 = _make_consumer([])
    c2 = _make_consumer([])
    try:
        assert c1.conf["group.id"] != c2.conf["group.id"]
        assert c1.conf["group.id"].startswith("dashboard-")
        assert c2.conf["group.id"].startswith("dashboard-")
    finally:
        c1.stop()
        c2.stop()


def test_auto_offset_reset_is_latest():
    consumer = _make_consumer([])
    try:
        assert consumer.conf["auto.offset.reset"] == "latest"
    finally:
        consumer.stop()
