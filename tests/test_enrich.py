from datetime import datetime

import pytest

from common.contracts import TRANSACTION_SCHEMA
from spark.enrich import _assert_no_label_leakage, enrich

pytestmark = pytest.mark.spark


def test_label_leakage_guard_allows_is_fraud():
    _assert_no_label_leakage(["user_id", "amount_eur", "country", "is_fraud"])


def test_label_leakage_guard_rejects_forbidden_columns():
    for bad in ("fraud_score", "label", "target_amount"):
        with pytest.raises(AssertionError):
            _assert_no_label_leakage(["user_id", bad])


def _row(user_id="u1000", amount=100.0, currency="USD", location="Paris",
         method="credit_card", is_fraud=0, transaction_id="t-0000001"):
    return {
        "user_id": user_id,
        "transaction_id": transaction_id,
        "amount": amount,
        "currency": currency,
        "timestamp": datetime(2026, 1, 1, 10, 0, 0),
        "location": location,
        "method": method,
        "is_fraud": is_fraud,
    }


def test_city_join_adds_country(spark):
    df = spark.createDataFrame([_row(location="Paris")], schema=TRANSACTION_SCHEMA)
    out = enrich(df).collect()
    assert out[0]["country"] == "FR"


def test_amount_eur_conversion(spark):
    df = spark.createDataFrame([_row(amount=100.0, currency="USD")], schema=TRANSACTION_SCHEMA)
    out = enrich(df).collect()
    assert out[0]["amount_eur"] == pytest.approx(92.00, abs=1e-9)


def test_unknown_city_does_not_drop_row(spark):
    df = spark.createDataFrame([_row(location="Atlantis")], schema=TRANSACTION_SCHEMA)
    out = enrich(df).collect()
    assert len(out) == 1
    assert out[0]["country"] is None
