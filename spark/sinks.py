"""Every alert lands in exactly three places through exactly one function:
console, parquet, and the fraud-alerts Kafka topic. Whether an alert came from
Q1 (unfiltered) or Q2/Q3 (post-AlertDeduper), it reaches here the same way.
See PLAN.md §4, §11 test_sinks.py.
"""
from __future__ import annotations

import threading
from typing import Callable, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import CONFIG, Config
from common.contracts import ALERT_SCHEMA

SinkFn = Callable[[DataFrame, int], None]

# Q1/Q2/Q3 all append to the same output_root. Concurrent micro-batch commits
# from different queries race on Spark's shared `_temporary` staging
# directory for that path and can abort each other's job. One process-wide
# lock serializes the actual parquet writes; it never blocks the console or
# Kafka sinks.
_parquet_write_lock = threading.Lock()


def console_sink(df: DataFrame, batch_id: int) -> None:
    df.show(n=20, truncate=False)


def make_parquet_sink(output_root: str) -> SinkFn:
    def _sink(df: DataFrame, batch_id: int) -> None:
        with _parquet_write_lock:
            df.write.mode("append").partitionBy("rule").parquet(output_root)
    return _sink


def make_kafka_sink(bootstrap_servers: str, topic: str) -> SinkFn:
    # ignoreNullFields=false: to_json drops null-valued keys by default, but
    # p_fraud is null for R1/R2/R3 and country is null for unknown cities. The
    # §5 alert contract keeps those keys present-with-null, and api/consumer.py
    # rejects any message missing an ALERT_SCHEMA field - so a dropped null key
    # would silently discard every rule-based alert at the dashboard.
    def _sink(df: DataFrame, batch_id: int) -> None:
        (df.select(F.col("user_id").alias("key"),
                   F.to_json(F.struct(*ALERT_SCHEMA.fieldNames()),
                             {"ignoreNullFields": "false"}).alias("value"))
           .write.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap_servers)
           .option("topic", topic)
           .save())
    return _sink


def default_sinks(config: Config = CONFIG) -> tuple[SinkFn, ...]:
    return (
        console_sink,
        make_parquet_sink(config.output_root),
        make_kafka_sink(config.kafka_bootstrap_servers, config.topic_alerts),
    )


def write_alerts(df: DataFrame, batch_id: int, sinks: Optional[Sequence[SinkFn]] = None) -> None:
    """Select exactly the ALERT_SCHEMA fields, persist once, fan out to every
    sink, and unpersist even if a sink raises."""
    sinks = default_sinks() if sinks is None else sinks
    alerts = df.select(*ALERT_SCHEMA.fieldNames())
    alerts.persist()
    try:
        for sink in sinks:
            sink(alerts, batch_id)
    finally:
        alerts.unpersist()
