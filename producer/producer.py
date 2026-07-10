"""Thin Kafka adapter around producer/simulator.py. No detection logic here.

Timestamps are stamped with now() at send time, not the simulator's synthetic
clock. Replaying backdated event time would make the watermark-based rules
meaningless.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from typing import Callable, Iterable

import confluent_kafka

from common.config import CONFIG
from common.contracts import LABEL_FIELD, TRANSACTION_FIELDS
from producer.simulator import generate

RATE_MIN, RATE_MAX = 10, 100
_REQUIRED_FIELDS = set(TRANSACTION_FIELDS) | {LABEL_FIELD}
_MAX_REJECT_RATE = 0.01


def validate_rate(rate: float) -> float:
    if not (RATE_MIN <= rate <= RATE_MAX):
        raise ValueError(f"--rate must be in [{RATE_MIN}, {RATE_MAX}], got {rate}")
    return rate


def _stamp_now(record: dict, now: Callable[[], datetime]) -> dict:
    stamped = dict(record)
    stamped["timestamp"] = now().strftime("%Y-%m-%dT%H:%M:%SZ")
    return stamped


def send(
    producer,
    records: Iterable[dict],
    rate: float,
    topic: str = CONFIG.topic_transactions,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Send each record to `producer` at ~`rate` msgs/sec. Returns count sent.

    Every record is validated against TRANSACTION_FIELDS + is_fraud before send;
    a reject rate above 1% aborts rather than emitting garbage.
    """
    validate_rate(rate)
    interval = 1.0 / rate
    sent = 0
    rejected = 0

    for record in records:
        stamped = _stamp_now(record, now)
        if not _REQUIRED_FIELDS.issubset(stamped):
            rejected += 1
            continue
        payload = json.dumps(stamped)
        producer.produce(topic, key=stamped["user_id"], value=payload)
        producer.poll(0)
        sent += 1
        sleep(interval)

    producer.flush()

    total = sent + rejected
    if total and rejected / total > _MAX_REJECT_RATE:
        raise RuntimeError(f"reject rate {rejected}/{total} exceeds 1% - refusing to emit garbage")
    return sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulated transaction producer")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rate", type=float, default=50.0,
                         help=f"transactions/sec, in [{RATE_MIN}, {RATE_MAX}]")
    parser.add_argument("--limit", type=int, default=5000, help="number of records to send")
    args = parser.parse_args(argv)
    try:
        validate_rate(args.rate)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    producer = confluent_kafka.Producer({"bootstrap.servers": CONFIG.kafka_bootstrap_servers})
    records = generate(args.seed, args.limit, datetime.now(timezone.utc))
    sent = send(producer, records, args.rate, topic=CONFIG.topic_transactions)
    print(f"sent {sent} records to {CONFIG.topic_transactions}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
