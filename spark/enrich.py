"""City lookup + currency conversion. Required before any rule can run: R1
filters on `amount_eur`, R3 groups on `country`. See PLAN.md §2.1, §5.

A broadcast left-join against the static `common/cities.py` table - never an
API, never a shuffle-heavy join, and never drops a row for an unknown city.
"""
from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.cities import CITIES
from common.contracts import FX_TO_EUR, LABEL_FIELD

# PLAN.md §14: "no outgoing column matches /fraud|label|target/i before the
# DataFrame reaches scoring.py." LABEL_FIELD (is_fraud) is the one expected
# exception - it is ground truth carried straight through from the producer,
# never derived here - so it is excluded before the check runs.
_FORBIDDEN_COLUMN = re.compile(r"fraud|label|target", re.IGNORECASE)


def _assert_no_label_leakage(columns: list[str]) -> None:
    leaked = [c for c in columns if c != LABEL_FIELD and _FORBIDDEN_COLUMN.search(c)]
    assert not leaked, f"enrich() introduced label-shaped column(s): {leaked}"


def _cities_frame(spark: SparkSession) -> DataFrame:
    rows = [(city, info.country) for city, info in CITIES.items()]
    return spark.createDataFrame(rows, ["location", "country"])


def _fx_map() -> F.Column:
    pairs = [x for item in FX_TO_EUR.items() for x in item]
    return F.create_map(*[F.lit(x) for x in pairs])


def enrich(df: DataFrame) -> DataFrame:
    """Add `country` (nullable - unknown cities left-join to null) and
    `amount_eur`. Never drops or reorders a row."""
    # Pin the session timezone so date_format() in rules.py round-trips the
    # producer's UTC "Z" timestamps byte-for-byte, regardless of caller.
    df.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    cities = F.broadcast(_cities_frame(df.sparkSession))
    fx = _fx_map()
    out = (
        df.join(cities, on="location", how="left")
          .withColumn("amount_eur", F.round(F.col("amount") * fx[F.col("currency")], 2))
    )
    _assert_no_label_leakage(out.columns)
    return out
