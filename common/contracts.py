"""Frozen contracts: schemas, rule names, thresholds.

Short and frozen, imported everywhere. No magic strings anywhere else.
"""
from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# The seven fields the brief mandates, in the brief's order.
TRANSACTION_FIELDS = ("user_id", "transaction_id", "amount", "currency",
                      "timestamp", "location", "method")

# Eighth field: simulator ground truth. Never a feature.
LABEL_FIELD = "is_fraud"

CURRENCIES = ("EUR", "USD", "GBP")
METHODS = ("credit_card", "debit_card", "paypal", "crypto")
AMOUNT_MIN, AMOUNT_MAX = 5.0, 5000.0          # from the brief's generator

# Fixed rates. This is a simulation; there is no FX API and never will be.
FX_TO_EUR = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17}

RULE_HIGH_VALUE = "high_value"
RULE_VELOCITY = "velocity"
RULE_GEO_HOP = "geo_hop"
RULE_ML_SCORE = "ml_score"
RULE_NAMES = (RULE_HIGH_VALUE, RULE_VELOCITY, RULE_GEO_HOP, RULE_ML_SCORE)

SEVERITY = {RULE_HIGH_VALUE: "medium", RULE_VELOCITY: "high",
            RULE_GEO_HOP: "critical", RULE_ML_SCORE: "high"}

FEATURE_ORDER = ("amount_eur", "log_amount", "hour", "dayofweek", "is_night",
                 "method_id", "currency_id", "amount_z", "is_new_user")

assert LABEL_FIELD not in FEATURE_ORDER
assert not any(t in f for f in FEATURE_ORDER for t in ("fraud", "label", "target"))

HIGH_VALUE_EUR = 1000.0                        # the brief's example threshold
VELOCITY_WINDOW, VELOCITY_MIN_COUNT = "1 minute", 4   # "more than 3" means >= 4
GEO_WINDOW, GEO_MIN_COUNTRIES = "5 minutes", 2
WATERMARK = "5 minutes"                        # the brief's value

TOPIC_TRANSACTIONS = "transactions"            # the brief's topic name
TOPIC_ALERTS = "fraud-alerts"                  # the brief's topic name

TRANSACTION_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("location", StringType(), False),
    StructField("method", StringType(), False),
    StructField(LABEL_FIELD, IntegerType(), False),
])
assert TRANSACTION_SCHEMA.fieldNames() == list(TRANSACTION_FIELDS + (LABEL_FIELD,))

ALERT_SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("alert_time", StringType(), False),
    StructField("rule", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("amount_eur", DoubleType(), False),
    StructField("location", StringType(), False),
    StructField("country", StringType(), True),
    StructField("p_fraud", DoubleType(), True),
    StructField("detail", StringType(), False),
    StructField(LABEL_FIELD, IntegerType(), False),
])
