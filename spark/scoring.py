"""R4 (`ml_score`): feature assembly + LightGBM scoring.

The booster loads lazily, once, from a plain absolute path - Spark runs in
local mode (`--master local[*]`), so `SparkFiles`/`addFile` distribution is
neither needed nor used. The `feature_name()` assertion is the only thing
standing between a retrained, reordered model and silent garbage predictions.
"""
from __future__ import annotations

import json
from pathlib import Path

import lightgbm as lgb
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from common.config import CONFIG
from common.contracts import CURRENCIES, FEATURE_ORDER, METHODS

MODEL_PATH = str(Path(CONFIG.model_path).resolve())
THRESHOLD_PATH = str(Path(CONFIG.threshold_path).resolve())

_booster: lgb.Booster | None = None
_threshold: float | None = None


def _load() -> lgb.Booster:
    global _booster
    if _booster is None:
        booster = lgb.Booster(model_file=MODEL_PATH)
        assert tuple(booster.feature_name()) == FEATURE_ORDER, \
            "model features do not match FEATURE_ORDER - retrain"
        _booster = booster
    return _booster


def load_threshold() -> float:
    global _threshold
    if _threshold is None:
        _threshold = json.loads(Path(THRESHOLD_PATH).read_text())["tau"]
    return _threshold


def score_frame(X: pd.DataFrame) -> pd.DataFrame:
    """Pure. X has exactly FEATURE_ORDER columns, in order. Unit-tested directly."""
    assert tuple(X.columns) == FEATURE_ORDER, "column order must match FEATURE_ORDER exactly"
    return pd.DataFrame({"p": _load().predict(X)})


# DoubleType(), not the "double" string form: the string form is parsed via
# the SQL parser, which needs an active SparkContext - and this module (like
# every other pure module here) must stay importable with no session running,
# e.g. from plain pytest collection.
@pandas_udf(DoubleType())
def score_udf(*cols: pd.Series) -> pd.Series:
    X = pd.concat(cols, axis=1, keys=FEATURE_ORDER)
    return score_frame(X)["p"]


def _index_map(values: tuple[str, ...]) -> F.Column:
    # Same create_map-lookup idiom as enrich.py's _fx_map(): array_position()
    # cannot be used here because its needle is a scalar, not a per-row column.
    pairs = [x for i, v in enumerate(values) for x in (v, i)]
    return F.create_map(*[F.lit(x) for x in pairs])


def add_ml_features(df: DataFrame, user_profiles: DataFrame) -> DataFrame:
    """Join the enriched stream (`amount_eur`/`country` already present, see
    spark/enrich.py) against `user_profiles` (`amt_mean`/`amt_std`/`tx_count`
    per `user_id`, built from the TRAIN split only - see ml/train.py) and
    derive the remaining FEATURE_ORDER columns. A user absent from
    `user_profiles` is unseen at train time: `is_new_user=1`, and `amount_z`
    falls back to centering on the transaction's own amount with unit spread
    (z=0) - ml/train.py applies the identical fallback so train and serve
    never disagree on an unseen user.
    """
    joined = df.join(F.broadcast(user_profiles), on="user_id", how="left")
    is_new = F.col("amt_mean").isNull()
    mean_ = F.when(is_new, F.col("amount_eur")).otherwise(F.col("amt_mean"))
    std_ = F.when(is_new, F.lit(1.0)).otherwise(F.greatest(F.col("amt_std"), F.lit(1e-6)))
    hour = F.hour(F.col("timestamp"))
    method_map, currency_map = _index_map(METHODS), _index_map(CURRENCIES)
    return joined.select(
        "*",
        F.log1p(F.col("amount_eur")).alias("log_amount"),
        hour.alias("hour"),
        F.dayofweek(F.col("timestamp")).alias("dayofweek"),
        F.when((hour >= 22) | (hour < 6), 1).otherwise(0).alias("is_night"),
        method_map[F.col("method")].cast("int").alias("method_id"),
        currency_map[F.col("currency")].cast("int").alias("currency_id"),
        ((F.col("amount_eur") - mean_) / std_).alias("amount_z"),
        F.when(is_new, 1).otherwise(0).alias("is_new_user"),
    )


def add_p_fraud(df: DataFrame, user_profiles: DataFrame) -> DataFrame:
    featured = add_ml_features(df, user_profiles)
    return featured.withColumn("p_fraud", score_udf(*[F.col(c) for c in FEATURE_ORDER]))
