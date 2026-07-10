"""Trap A guard: outputMode("append") on a windowed aggregation emits nothing
until the watermark passes the window end. If Q2/Q3 ever regress to `append`,
this file source -> memory sink test goes red immediately instead of five
minutes of silent console output. See PLAN.md §8 Trap A and §11.

Write this before writing R2. Watch it fail. Then make it pass.
"""
from __future__ import annotations

import json
from pathlib import Path

from spark.enrich import enrich
from spark.rules import geo_hop_rule, velocity_rule

_BASE = {
    "amount": 42.0,
    "currency": "EUR",
    "method": "credit_card",
    "is_fraud": 0,
}


def write_jsonl(path: Path, records: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")


def four_txs_one_user_within_60s() -> list[dict]:
    times = ["2026-01-01T10:12:00Z", "2026-01-01T10:12:15Z",
             "2026-01-01T10:12:30Z", "2026-01-01T10:12:45Z"]
    return [
        {**_BASE, "user_id": "u1000", "transaction_id": f"t-{i:07d}",
         "timestamp": t, "location": "Paris"}
        for i, t in enumerate(times)
    ]


def _read_stream(spark, src: Path):
    from common.contracts import TRANSACTION_SCHEMA
    return spark.readStream.schema(TRANSACTION_SCHEMA).json(str(src))


def test_velocity_emits_without_waiting_for_window_close(spark, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    stream = _read_stream(spark, src)
    q = (velocity_rule(enrich(stream))
         .writeStream.format("memory").queryName("out")
         .outputMode("update")
         .option("checkpointLocation", str(tmp_path / "ck")).start())
    write_jsonl(src / "b1.json", four_txs_one_user_within_60s())
    q.processAllAvailable()
    assert spark.sql("select * from out").count() > 0, \
        "velocity emitted nothing - check outputMode (Trap A)"
    q.stop()


def test_geo_hop_across_batches(spark, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    stream = _read_stream(spark, src)
    q = (geo_hop_rule(enrich(stream))
         .writeStream.format("memory").queryName("geo_out")
         .outputMode("update")
         .option("checkpointLocation", str(tmp_path / "ck")).start())

    write_jsonl(src / "b1.json", [
        {**_BASE, "user_id": "u2000", "transaction_id": "t-0000001",
         "timestamp": "2026-01-01T10:00:00Z", "location": "Paris"},
    ])
    q.processAllAvailable()

    write_jsonl(src / "b2.json", [
        {**_BASE, "user_id": "u2000", "transaction_id": "t-0000002",
         "timestamp": "2026-01-01T10:01:00Z", "location": "London"},
    ])
    q.processAllAvailable()

    assert spark.sql("select * from geo_out").count() > 0, \
        "geo_hop did not fire across batches - streaming state not surviving"
    q.stop()


def test_malformed_json_does_not_kill_query(spark, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    stream = _read_stream(spark, src)
    q = (velocity_rule(enrich(stream))
         .writeStream.format("memory").queryName("malformed_out")
         .outputMode("update")
         .option("checkpointLocation", str(tmp_path / "ck")).start())

    bad_and_good = "\n".join([
        "{not valid json at all",
        json.dumps({**_BASE, "user_id": "u3000", "transaction_id": "t-0000001",
                    "timestamp": "2026-01-01T10:12:00Z", "location": "Paris"}),
    ]) + "\n"
    (src / "b1.json").write_text(bad_and_good)

    q.processAllAvailable()

    assert q.exception() is None
    assert q.isActive
    q.stop()
