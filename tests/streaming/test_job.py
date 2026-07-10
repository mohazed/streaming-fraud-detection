"""Exercises spark/job.py's actual wiring (build_queries, the per-rule batch
closures, AlertDeduper integration) end to end, off a file source instead of
Kafka. `read_transactions`/`main` are thin Kafka/entrypoint adapters that need
a real broker or spark-submit --packages to even import the connector - and
if a function needs a SparkSession to be tested, it is doing too much - so
they are exercised by the smoke test, not here.
"""
from __future__ import annotations

import json
from pathlib import Path

from common.config import load_config
from common.contracts import RULE_HIGH_VALUE, RULE_VELOCITY, TRANSACTION_SCHEMA
from spark.job import build_queries

_BASE = {"amount": 42.0, "currency": "EUR", "method": "credit_card",
         "location": "Paris", "is_fraud": 0}


def _write(path: Path, records: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")


def test_build_queries_wires_rules_dedup_and_sinks(spark, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    stream = spark.readStream.schema(TRANSACTION_SCHEMA).json(str(src))
    config = load_config({
        "CHECKPOINT_ROOT": str(tmp_path / "ck"),
        "OUTPUT_ROOT": str(tmp_path / "out"),
    })

    seen: list[dict] = []

    def collector(df, batch_id):
        seen.extend(row.asDict() for row in df.collect())

    queries = build_queries(stream, config=config, sinks=[collector])
    assert len(queries) == 3

    # One high-value hit, four velocity hits for the same user/window.
    _write(src / "b1.json", [
        {**_BASE, "user_id": "u1000", "transaction_id": "t-0000001", "amount": 1500.0,
         "timestamp": "2026-01-01T10:00:00Z"},
        *[
            {**_BASE, "user_id": "u2000", "transaction_id": f"t-000000{i + 2}",
             "timestamp": f"2026-01-01T10:00:{10 * i:02d}Z"}
            for i in range(4)
        ],
    ])
    for q in queries:
        q.processAllAvailable()

    rules_fired = {row["rule"] for row in seen}
    assert RULE_HIGH_VALUE in rules_fired
    assert RULE_VELOCITY in rules_fired

    velocity_count_before = sum(1 for row in seen if row["rule"] == RULE_VELOCITY)
    assert velocity_count_before == 1  # not zero, not duplicated

    # A fifth transaction re-triggers the same (rule, user, window) aggregate
    # under outputMode("update") - AlertDeduper must suppress the re-emission.
    _write(src / "b2.json", [
        {**_BASE, "user_id": "u2000", "transaction_id": "t-0000006",
         "timestamp": "2026-01-01T10:00:45Z"},
    ])
    for q in queries:
        q.processAllAvailable()

    velocity_count_after = sum(1 for row in seen if row["rule"] == RULE_VELOCITY)
    assert velocity_count_after == 1, "AlertDeduper did not suppress the re-emitted window (Trap B)"

    for q in queries:
        q.stop()
