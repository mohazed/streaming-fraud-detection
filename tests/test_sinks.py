import json
import threading

import pytest
from pyspark.sql import functions as F

from common.contracts import ALERT_SCHEMA
from spark.sinks import make_parquet_sink, write_alerts
from tests.fakes import FakeSink


def _alert_row(rule="high_value"):
    return {
        "alert_id": "a-1", "transaction_id": "t-0000001", "user_id": "u1000",
        "event_time": "2026-01-01T10:00:00Z", "alert_time": "2026-01-01T10:00:01Z",
        "rule": rule, "severity": "medium", "amount": 1500.0, "currency": "EUR",
        "amount_eur": 1500.0, "location": "Paris", "country": "FR",
        "p_fraud": None, "detail": "test", "is_fraud": 0,
    }


def test_write_alerts_calls_all_three(spark):
    df = spark.createDataFrame([_alert_row()], schema=ALERT_SCHEMA)
    fakes = [FakeSink(), FakeSink(), FakeSink()]
    write_alerts(df, 0, sinks=fakes)
    for fake in fakes:
        assert len(fake.calls) == 1
        assert fake.calls[0][1] == 0


def test_persist_and_unpersist(spark):
    df = spark.createDataFrame([_alert_row()], schema=ALERT_SCHEMA)
    captured = {}

    def first_sink(alerts_df, batch_id):
        captured["df"] = alerts_df
        captured["persisted_during"] = alerts_df.storageLevel.useMemory

    def raising_sink(alerts_df, batch_id):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        write_alerts(df, 0, sinks=[first_sink, raising_sink])

    assert captured["persisted_during"] is True
    assert captured["df"].storageLevel.useMemory is False


@pytest.mark.spark
def test_kafka_json_keeps_null_fields(spark):
    """The real kafka sink serializes via F.to_json, which drops null-valued
    keys by default. p_fraud is null for R1/R2/R3 and country is null for
    unknown cities; api/consumer.py rejects any alert missing an ALERT_SCHEMA
    field, so every rule-based alert would be silently discarded at the
    dashboard. The value JSON must carry all fields, present-with-null."""
    row = _alert_row(rule="high_value")
    row["country"] = None  # unknown-city case: this field is nullable too
    df = spark.createDataFrame([row], schema=ALERT_SCHEMA)
    value = (
        df.select(F.to_json(F.struct(*ALERT_SCHEMA.fieldNames()),
                            {"ignoreNullFields": "false"}).alias("value"))
          .collect()[0]["value"]
    )
    payload = json.loads(value)
    assert set(ALERT_SCHEMA.fieldNames()).issubset(payload.keys())
    assert payload["p_fraud"] is None
    assert payload["country"] is None


@pytest.mark.spark
def test_parquet_lands_partitioned(spark, tmp_path):
    rows = [_alert_row(rule="high_value"), _alert_row(rule="velocity")]
    df = spark.createDataFrame(rows, schema=ALERT_SCHEMA)
    sink = make_parquet_sink(str(tmp_path))
    write_alerts(df, 0, sinks=[sink])

    partitions = {p.name for p in tmp_path.iterdir() if p.is_dir()}
    assert "rule=high_value" in partitions
    assert "rule=velocity" in partitions


@pytest.mark.spark
def test_parquet_sink_survives_concurrent_writers(spark, tmp_path):
    """Q1/Q2/Q3 all append to the same output_root; concurrent micro-batch
    commits from different queries must not abort each other's write job
    (they raced on Spark's shared `_temporary` staging directory before the
    write lock was added)."""
    sink = make_parquet_sink(str(tmp_path))
    errors = []

    def write_one(rule):
        try:
            df = spark.createDataFrame([_alert_row(rule=rule)], schema=ALERT_SCHEMA)
            write_alerts(df, 0, sinks=[sink])
        except Exception as exc:  # noqa: BLE001 - want to see any concurrency failure
            errors.append(exc)

    threads = [threading.Thread(target=write_one, args=(rule,))
               for rule in ("high_value", "velocity", "geo_hop")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    partitions = {p.name for p in tmp_path.iterdir() if p.is_dir()}
    assert partitions == {"rule=high_value", "rule=velocity", "rule=geo_hop"}
