import pickle

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

from common.contracts import FEATURE_ORDER
from spark import scoring


def _train_booster(tmp_path, feature_names, filename="model.txt"):
    rng = np.random.default_rng(0)
    X = pd.DataFrame(rng.random((50, len(feature_names))), columns=feature_names)
    y = rng.integers(0, 2, size=50)
    dataset = lgb.Dataset(X, label=y, feature_name=list(feature_names))
    booster = lgb.train(
        {"objective": "binary", "verbosity": -1, "min_data_in_leaf": 1},
        dataset, num_boost_round=2,
    )
    path = tmp_path / filename
    booster.save_model(str(path))
    return path


@pytest.fixture(autouse=True)
def _reset_booster_cache(monkeypatch):
    monkeypatch.setattr(scoring, "_booster", None)
    yield
    monkeypatch.setattr(scoring, "_booster", None)


def test_score_frame_pure(tmp_path, monkeypatch):
    model_path = _train_booster(tmp_path, FEATURE_ORDER)
    monkeypatch.setattr(scoring, "MODEL_PATH", str(model_path))

    n = 10
    X = pd.DataFrame(
        np.random.default_rng(1).random((n, len(FEATURE_ORDER))),
        columns=list(FEATURE_ORDER),
    )
    out = scoring.score_frame(X)
    assert len(out) == n
    assert ((out["p"] >= 0.0) & (out["p"] <= 1.0)).all()


def test_wrong_column_order_raises():
    X = pd.DataFrame(
        np.zeros((3, len(FEATURE_ORDER))),
        columns=list(reversed(FEATURE_ORDER)),
    )
    with pytest.raises(AssertionError):
        scoring.score_frame(X)


def test_feature_name_assertion(tmp_path, monkeypatch):
    wrong_order = tuple(reversed(FEATURE_ORDER))
    model_path = _train_booster(tmp_path, wrong_order, filename="wrong.txt")
    monkeypatch.setattr(scoring, "MODEL_PATH", str(model_path))

    with pytest.raises(AssertionError, match="FEATURE_ORDER"):
        scoring._load()


def test_udf_not_fat():
    assert len(pickle.dumps(scoring.score_udf)) < 50_000
