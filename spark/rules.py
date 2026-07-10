"""R1-R4: each a pure Spark transformation producing rows shaped like
ALERT_SCHEMA. Windowed rules (R2, R3) also carry a `window_start` column,
consumed by the caller as the dedup key (see PLAN.md §8 Trap B) and stripped
before the row reaches a sink (spark/sinks.py selects only ALERT_SCHEMA
fields). R4 (`ml_score`) is stateless like R1: it expects a `p_fraud` column
already assembled by spark/scoring.py::add_p_fraud - feature engineering and
the LightGBM booster live there, so this file stays a pure filter+shape and
its R4 tests never need a real trained model.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.contracts import (
    GEO_MIN_COUNTRIES,
    GEO_WINDOW,
    HIGH_VALUE_EUR,
    LABEL_FIELD,
    RULE_GEO_HOP,
    RULE_HIGH_VALUE,
    RULE_ML_SCORE,
    RULE_VELOCITY,
    SEVERITY,
    VELOCITY_MIN_COUNT,
    VELOCITY_WINDOW,
    WATERMARK,
)

_ISO_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def _iso(col: F.Column) -> F.Column:
    return F.date_format(col, _ISO_FMT)


def _field(struct_col: str, field: str) -> F.Column:
    # getField(), not dotted-string col("struct.field") - "timestamp" is a SQL
    # keyword and the dotted form fails to parse as a nested-field reference.
    return F.col(struct_col).getField(field)


def _representative_tx() -> F.Column:
    """The latest-by-event-time row in a window, as a struct - carries the
    per-transaction fields a windowed aggregate otherwise loses.

    The event-time column is aliased to `event_ts`, not `timestamp`: Spark's
    streaming analyzer treats a nested field literally named `timestamp`
    specially once a watermark is active and fails to resolve it inside a
    struct built across the aggregate boundary.
    """
    return F.struct(
        F.col("transaction_id"), F.col("amount"), F.col("currency"),
        F.col("amount_eur"), F.col("location"), F.col("country"),
        F.col("timestamp").alias("event_ts"), F.col(LABEL_FIELD),
    )


def high_value_rule(df: DataFrame) -> DataFrame:
    """R1, stateless, Q1."""
    hits = df.filter(F.col("amount_eur") > HIGH_VALUE_EUR)
    detail = F.concat(F.lit("amount "), F.col("amount_eur").cast("string"),
                       F.lit(" EUR exceeds "), F.lit(str(HIGH_VALUE_EUR)),
                       F.lit(" threshold"))
    return hits.select(
        F.expr("uuid()").alias("alert_id"),
        F.col("transaction_id"), F.col("user_id"),
        _iso(F.col("timestamp")).alias("event_time"),
        _iso(F.current_timestamp()).alias("alert_time"),
        F.lit(RULE_HIGH_VALUE).alias("rule"),
        F.lit(SEVERITY[RULE_HIGH_VALUE]).alias("severity"),
        F.col("amount"), F.col("currency"), F.col("amount_eur"),
        F.col("location"), F.col("country"),
        F.lit(None).cast("double").alias("p_fraud"),
        detail.alias("detail"),
        F.col(LABEL_FIELD),
    )


def ml_score_rule(df: DataFrame, tau: float) -> DataFrame:
    """R4, stateless, Q1. `df` must already carry a `p_fraud` column (see
    spark/scoring.py::add_p_fraud). Strict `>`, matching R1's boundary
    convention: `p_fraud == tau` does not fire."""
    hits = df.filter(F.col("p_fraud") > tau)
    detail = F.concat(F.lit("p_fraud="), F.round(F.col("p_fraud"), 4).cast("string"),
                       F.lit(" exceeds tau="), F.lit(str(tau)))
    return hits.select(
        F.expr("uuid()").alias("alert_id"),
        F.col("transaction_id"), F.col("user_id"),
        _iso(F.col("timestamp")).alias("event_time"),
        _iso(F.current_timestamp()).alias("alert_time"),
        F.lit(RULE_ML_SCORE).alias("rule"),
        F.lit(SEVERITY[RULE_ML_SCORE]).alias("severity"),
        F.col("amount"), F.col("currency"), F.col("amount_eur"),
        F.col("location"), F.col("country"),
        F.col("p_fraud"),
        detail.alias("detail"),
        F.col(LABEL_FIELD),
    )


def velocity_rule(df: DataFrame) -> DataFrame:
    """R2, 1-minute tumbling window, Q2. outputMode('update') required."""
    win = F.window(F.col("timestamp"), VELOCITY_WINDOW)
    grouped = (
        df.withWatermark("timestamp", WATERMARK)
          .groupBy(win, F.col("user_id"))
          .agg(F.count(F.lit(1)).alias("tx_count"),
               F.max_by(_representative_tx(), F.col("timestamp")).alias("last_tx"))
          .filter(F.col("tx_count") >= VELOCITY_MIN_COUNT)
    )
    # Reference the aggregate's materialized "window" struct column, not `win`
    # again - re-evaluating `win` here would re-derive window() from the raw
    # "timestamp" column, which no longer exists post-aggregation.
    win_start, win_end = _field("window", "start"), _field("window", "end")
    detail = F.concat(F.col("tx_count").cast("string"), F.lit(" transactions in window "),
                       F.date_format(win_start, "HH:mm:ss"), F.lit("-"),
                       F.date_format(win_end, "HH:mm:ss"))
    return grouped.select(
        F.expr("uuid()").alias("alert_id"),
        _field("last_tx", "transaction_id").alias("transaction_id"), F.col("user_id"),
        _iso(_field("last_tx", "event_ts")).alias("event_time"),
        _iso(F.current_timestamp()).alias("alert_time"),
        F.lit(RULE_VELOCITY).alias("rule"),
        F.lit(SEVERITY[RULE_VELOCITY]).alias("severity"),
        _field("last_tx", "amount").alias("amount"),
        _field("last_tx", "currency").alias("currency"),
        _field("last_tx", "amount_eur").alias("amount_eur"),
        _field("last_tx", "location").alias("location"),
        _field("last_tx", "country").alias("country"),
        F.lit(None).cast("double").alias("p_fraud"),
        detail.alias("detail"),
        _field("last_tx", LABEL_FIELD).alias(LABEL_FIELD),
        win_start.alias("window_start"),
    )


def geo_hop_rule(df: DataFrame) -> DataFrame:
    """R3, 5-minute tumbling window, Q3. outputMode('update') required."""
    win = F.window(F.col("timestamp"), GEO_WINDOW)
    grouped = (
        df.withWatermark("timestamp", WATERMARK)
          .groupBy(win, F.col("user_id"))
          .agg(F.approx_count_distinct("country").alias("n_countries"),
               F.collect_set("country").alias("countries"),
               F.max_by(_representative_tx(), F.col("timestamp")).alias("last_tx"))
          .filter(F.col("n_countries") >= GEO_MIN_COUNTRIES)
    )
    win_start, win_end = _field("window", "start"), _field("window", "end")
    detail = F.concat(F.col("n_countries").cast("string"), F.lit(" countries in window "),
                       F.date_format(win_start, "HH:mm:ss"), F.lit("-"),
                       F.date_format(win_end, "HH:mm:ss"), F.lit(": "),
                       F.array_join(F.col("countries"), ","))
    return grouped.select(
        F.expr("uuid()").alias("alert_id"),
        _field("last_tx", "transaction_id").alias("transaction_id"), F.col("user_id"),
        _iso(_field("last_tx", "event_ts")).alias("event_time"),
        _iso(F.current_timestamp()).alias("alert_time"),
        F.lit(RULE_GEO_HOP).alias("rule"),
        F.lit(SEVERITY[RULE_GEO_HOP]).alias("severity"),
        _field("last_tx", "amount").alias("amount"),
        _field("last_tx", "currency").alias("currency"),
        _field("last_tx", "amount_eur").alias("amount_eur"),
        _field("last_tx", "location").alias("location"),
        _field("last_tx", "country").alias("country"),
        F.lit(None).cast("double").alias("p_fraud"),
        detail.alias("detail"),
        _field("last_tx", LABEL_FIELD).alias(LABEL_FIELD),
        win_start.alias("window_start"),
    )
