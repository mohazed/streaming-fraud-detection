import json
from datetime import datetime
from pathlib import Path

import pytest

from common.contracts import LABEL_FIELD, TRANSACTION_FIELDS
from producer.producer import parse_args, send, validate_rate
from producer.simulator import generate
from tests.fakes import FakeProducer

START = datetime(2024, 1, 1, 0, 0, 0)
SIMULATOR_SRC = Path(__file__).parents[1] / "producer" / "simulator.py"
_BANNED_IMPORTS = ("requests", "kafka", "confluent_kafka")


def test_produces_via_fake():
    records = generate(1, 25, START)
    fake = FakeProducer()

    sent = send(fake, records, rate=50)

    assert sent == 25
    assert len(fake.messages) == 25
    assert fake.flushed
    expected_keys = set(TRANSACTION_FIELDS) | {LABEL_FIELD}
    for msg, record in zip(fake.messages, records):
        assert msg["key"] == record["user_id"]
        payload = json.loads(msg["value"])
        assert set(payload.keys()) == expected_keys


def test_rate_limiting():
    records = generate(2, 50, START)
    fake = FakeProducer()
    slept = []

    send(fake, records, rate=50, sleep=slept.append)

    total = sum(slept)
    assert abs(total - 1.0) <= 0.2 * 1.0


def test_rate_within_brief_bounds():
    with pytest.raises(ValueError):
        validate_rate(5)
    with pytest.raises(ValueError):
        validate_rate(500)
    validate_rate(10)
    validate_rate(100)

    with pytest.raises(SystemExit):
        parse_args(["--rate", "5"])
    with pytest.raises(SystemExit):
        parse_args(["--rate", "500"])
    args = parse_args(["--rate", "75"])
    assert args.rate == 75.0


def test_no_network_import():
    source = SIMULATOR_SRC.read_text()
    for token in _BANNED_IMPORTS:
        assert f"import {token}" not in source, f"simulator.py must not import {token}"
        assert f"from {token}" not in source, f"simulator.py must not import from {token}"
