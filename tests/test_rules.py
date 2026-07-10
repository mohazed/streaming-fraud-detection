from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from common.contracts import (
    ALERT_SCHEMA,
    RULE_GEO_HOP,
    RULE_HIGH_VALUE,
    RULE_ML_SCORE,
    RULE_VELOCITY,
    TRANSACTION_SCHEMA,
)
from spark.enrich import enrich
from spark.rules import geo_hop_rule, high_value_rule, ml_score_rule, velocity_rule

pytestmark = pytest.mark.spark

_T0 = datetime(2026, 1, 1, 10, 0, 0)


def _row(user_id="u1000", transaction_id="t-0000001", amount=42.0, currency="EUR",
         timestamp=_T0, location="Paris", method="credit_card", is_fraud=0):
    return {
        "user_id": user_id, "transaction_id": transaction_id, "amount": amount,
        "currency": currency, "timestamp": timestamp, "location": location,
        "method": method, "is_fraud": is_fraud,
    }


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)


# ---------------------------------------------------------------- R1 high_value

def test_high_value_fires_above_threshold(spark):
    rows = [_row(amount=1000.01, currency="EUR")]
    out = high_value_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 1
    assert out[0]["rule"] == RULE_HIGH_VALUE


def test_high_value_does_not_fire_below_threshold(spark):
    rows = [_row(amount=999.99, currency="EUR")]
    out = high_value_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 0


def test_high_value_boundary_exactly_1000_does_not_fire(spark):
    rows = [_row(amount=1000.00, currency="EUR")]
    out = high_value_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 0


def test_high_value_fires_after_currency_conversion(spark):
    rows = [_row(amount=1200.00, currency="USD")]  # 1200 * 0.92 = 1104 EUR
    out = high_value_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 1
    assert out[0]["amount_eur"] == pytest.approx(1104.00, abs=1e-9)


# ------------------------------------------------------------------ R2 velocity

def test_velocity_fires_with_four_in_59s(spark):
    rows = [_row(user_id="u1000", transaction_id=f"t-{i:07d}",
                  timestamp=_T0 + timedelta(seconds=15 * i))
            for i in range(4)]  # spans 45s, all inside one 1-min window
    out = velocity_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 1
    assert out[0]["rule"] == RULE_VELOCITY


def test_velocity_does_not_fire_with_three(spark):
    rows = [_row(user_id="u1000", transaction_id=f"t-{i:07d}",
                  timestamp=_T0 + timedelta(seconds=15 * i))
            for i in range(3)]
    out = velocity_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 0


def test_velocity_does_not_fire_when_spanning_window_boundary(spark):
    base = datetime(2026, 1, 1, 10, 11, 30)
    offsets = [0, 20, 40, 61]  # crosses the 10:12:00 tumbling boundary
    rows = [_row(user_id="u1000", transaction_id=f"t-{i:07d}",
                  timestamp=base + timedelta(seconds=o))
            for i, o in enumerate(offsets)]
    out = velocity_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 0


def test_velocity_two_users_two_each_do_not_combine(spark):
    rows = []
    for user in ("u1000", "u2000"):
        for i in range(2):
            rows.append(_row(user_id=user, transaction_id=f"t-{user}-{i}",
                              timestamp=_T0 + timedelta(seconds=10 * i)))
    out = velocity_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 0


# ------------------------------------------------------------------- R3 geo_hop

def test_geo_hop_fires_three_countries_in_3_min(spark):
    rows = [
        _row(transaction_id="t-0000001", location="Paris", timestamp=_T0),
        _row(transaction_id="t-0000002", location="London", timestamp=_T0 + timedelta(seconds=90)),
        _row(transaction_id="t-0000003", location="Berlin", timestamp=_T0 + timedelta(seconds=180)),
    ]
    out = geo_hop_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 1
    assert out[0]["rule"] == RULE_GEO_HOP


def test_geo_hop_does_not_fire_same_country(spark):
    rows = [
        _row(transaction_id="t-0000001", location="Paris", timestamp=_T0),
        _row(transaction_id="t-0000002", location="Lyon", timestamp=_T0 + timedelta(seconds=90)),
    ]
    out = geo_hop_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 0


def test_geo_hop_does_not_fire_spanning_window_boundary(spark):
    base = datetime(2026, 1, 1, 10, 3, 0)
    rows = [
        _row(transaction_id="t-0000001", location="Paris", timestamp=base),
        _row(transaction_id="t-0000002", location="London", timestamp=base + timedelta(minutes=6)),
    ]
    out = geo_hop_rule(enrich(_df(spark, rows))).collect()
    assert len(out) == 0


# -------------------------------------------------------------------- R4 ml_score
# ml_score_rule expects `p_fraud` already present (see spark/scoring.py::add_p_fraud),
# so these tests build it directly - no trained booster needed.

def test_ml_score_fires_above_tau(spark):
    rows = [_row(amount=42.0)]
    df = enrich(_df(spark, rows)).withColumn("p_fraud", F.lit(0.85))
    out = ml_score_rule(df, tau=0.8).collect()
    assert len(out) == 1
    assert out[0]["rule"] == RULE_ML_SCORE
    assert out[0]["p_fraud"] == pytest.approx(0.85)


def test_ml_score_does_not_fire_at_tau(spark):
    rows = [_row(amount=42.0)]
    df = enrich(_df(spark, rows)).withColumn("p_fraud", F.lit(0.8))
    out = ml_score_rule(df, tau=0.8).collect()
    assert len(out) == 0


def test_ml_score_does_not_fire_below_tau(spark):
    rows = [_row(amount=42.0)]
    df = enrich(_df(spark, rows)).withColumn("p_fraud", F.lit(0.79))
    out = ml_score_rule(df, tau=0.8).collect()
    assert len(out) == 0


def test_ml_score_alerts_conform_to_schema(spark):
    rows = [_row(amount=42.0)]
    df = enrich(_df(spark, rows)).withColumn("p_fraud", F.lit(0.9))
    out = ml_score_rule(df, tau=0.8)
    assert set(ALERT_SCHEMA.fieldNames()) <= set(out.columns)
    row = out.collect()[0]
    assert row["p_fraud"] == pytest.approx(0.9)
    for field in ("alert_id", "transaction_id", "user_id", "event_time", "alert_time",
                  "rule", "severity", "amount", "currency", "amount_eur", "location",
                  "detail", "is_fraud"):
        assert row[field] is not None


# --------------------------------------------------------------------- general

def test_alerts_conform_to_schema(spark):
    rows = [_row(amount=1500.0)]
    out = high_value_rule(enrich(_df(spark, rows)))
    assert set(ALERT_SCHEMA.fieldNames()) <= set(out.columns)
    row = out.collect()[0]
    for field in ("alert_id", "transaction_id", "user_id", "event_time", "alert_time",
                  "rule", "severity", "amount", "currency", "amount_eur", "location",
                  "detail", "is_fraud"):
        assert row[field] is not None


def test_rules_do_not_read_label(spark):
    rows_fraud_0 = [_row(amount=1500.0, is_fraud=0)]
    rows_fraud_1 = [_row(amount=1500.0, is_fraud=1)]
    n0 = high_value_rule(enrich(_df(spark, rows_fraud_0))).count()
    n1 = high_value_rule(enrich(_df(spark, rows_fraud_1))).count()
    assert n0 == n1 == 1

    velocity_rows_0 = [_row(user_id="u1000", transaction_id=f"t-{i:07d}",
                             timestamp=_T0 + timedelta(seconds=10 * i), is_fraud=0)
                        for i in range(4)]
    velocity_rows_1 = [_row(user_id="u1000", transaction_id=f"t-{i:07d}",
                             timestamp=_T0 + timedelta(seconds=10 * i), is_fraud=1)
                        for i in range(4)]
    v0 = velocity_rule(enrich(_df(spark, velocity_rows_0))).count()
    v1 = velocity_rule(enrich(_df(spark, velocity_rows_1))).count()
    assert v0 == v1 == 1
