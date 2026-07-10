"""Wires enrich -> {R1+R4, R2, R3} -> [AlertDeduper] -> write_alerts into
three independent streaming queries reading `transactions` once each.

Q1 (high_value + ml_score) is stateless append; both rules share a query and
a checkpoint since neither is windowed. Q2 (velocity) and Q3 (geo_hop) are
outputMode("update") - PLAN.md §8 Trap A - and every alert they emit is
routed through its own AlertDeduper before write_alerts - Trap B. Each query
gets its own checkpoint path from common/config.py - Trap C.
"""
from __future__ import annotations

import logging
import threading
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common.config import CONFIG, Config
from common.contracts import RULE_GEO_HOP, RULE_VELOCITY, TRANSACTION_SCHEMA
from spark import scoring
from spark.dedup import AlertDeduper
from spark.enrich import enrich
from spark.health import poll_forever
from spark.rules import geo_hop_rule, high_value_rule, ml_score_rule, velocity_rule
from spark.sinks import SinkFn, write_alerts

TRIGGER_INTERVAL = "5 seconds"

log = logging.getLogger(__name__)


def build_spark_session(app_name: str = "fraud-stream") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name).master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_transactions(spark: SparkSession, config: Config = CONFIG) -> DataFrame:
    """Kafka source, parsed against the frozen TRANSACTION_SCHEMA. Malformed
    payloads become null-field rows under from_json's default permissive
    mode - they fail every rule's filter and never crash the query."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.topic_transactions)
        .option("startingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
    )


def _dedup_and_write(df: DataFrame, batch_id: int, deduper: AlertDeduper, rule: str,
                      sinks: Optional[Sequence[SinkFn]]) -> None:
    rows = df.collect()
    allowed = [r for r in rows if deduper.should_emit(rule, r["user_id"], str(r["window_start"]))]
    if allowed:
        write_alerts(df.sparkSession.createDataFrame(allowed, schema=df.schema), batch_id, sinks)


def build_queries(source: DataFrame, config: Config = CONFIG,
                   sinks: Optional[Sequence[SinkFn]] = None) -> list[StreamingQuery]:
    """`sinks=None` uses the production console+parquet+kafka trio (see
    spark/sinks.py::default_sinks); tests inject fakes to stay off the network."""
    enriched = enrich(source)
    velocity_deduper, geo_deduper = AlertDeduper(), AlertDeduper()

    scoring._load()  # startup runtime assertion (PLAN.md §14): dies loudly on
                      # a FEATURE_ORDER mismatch instead of scoring garbage.
    user_profiles = source.sparkSession.read.parquet(config.user_profiles_path)
    tau = scoring.load_threshold()
    row_alerts = high_value_rule(enriched).unionByName(
        ml_score_rule(scoring.add_p_fraud(enriched, user_profiles), tau))

    def row_batch(df: DataFrame, batch_id: int) -> None:
        if not df.isEmpty():
            write_alerts(df, batch_id, sinks)

    def velocity_batch(df: DataFrame, batch_id: int) -> None:
        _dedup_and_write(df, batch_id, velocity_deduper, RULE_VELOCITY, sinks)

    def geo_batch(df: DataFrame, batch_id: int) -> None:
        _dedup_and_write(df, batch_id, geo_deduper, RULE_GEO_HOP, sinks)

    q_row = (
        row_alerts.writeStream.foreachBatch(row_batch)
        .outputMode("append").trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", config.checkpoint_row).start()
    )
    q_velocity = (
        velocity_rule(enriched).writeStream.foreachBatch(velocity_batch)
        .outputMode("update").trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", config.checkpoint_velocity).start()
    )
    q_geo = (
        geo_hop_rule(enriched).writeStream.foreachBatch(geo_batch)
        .outputMode("update").trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", config.checkpoint_geo).start()
    )
    return [q_row, q_velocity, q_geo]


def main() -> int:
    checkpoints = {CONFIG.checkpoint_row, CONFIG.checkpoint_velocity, CONFIG.checkpoint_geo}
    assert len(checkpoints) == 3, "checkpoint paths collide - Trap C"

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    q_row, q_velocity, q_geo = build_queries(read_transactions(spark))
    log.info("3 queries active")

    stop_event = threading.Event()
    queries = {"row": q_row, "velocity": q_velocity, "geo": q_geo}
    health_thread = threading.Thread(target=poll_forever, args=(queries, stop_event),
                                      daemon=True, name="stream-health-monitor")
    health_thread.start()
    try:
        spark.streams.awaitAnyTermination()
    finally:
        stop_event.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
