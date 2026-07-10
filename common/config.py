"""Runtime configuration: broker, topics, paths. All overridable from env.

Checkpoint paths are derived per query name so they can never collide.
See PLAN.md §8 Trap C.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from common.contracts import TOPIC_ALERTS, TOPIC_TRANSACTIONS


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    topic_transactions: str
    topic_alerts: str
    checkpoint_root: str
    output_root: str
    model_dir: str

    @property
    def checkpoint_row(self) -> str:
        return f"{self.checkpoint_root}/row"

    @property
    def checkpoint_velocity(self) -> str:
        return f"{self.checkpoint_root}/velocity"

    @property
    def checkpoint_geo(self) -> str:
        return f"{self.checkpoint_root}/geo"

    @property
    def model_path(self) -> str:
        return f"{self.model_dir}/model.txt"

    @property
    def threshold_path(self) -> str:
        return f"{self.model_dir}/threshold.json"

    @property
    def user_profiles_path(self) -> str:
        return f"{self.model_dir}/user_profiles.parquet"


def load_config(env: dict | None = None) -> Config:
    """Build a Config from `env`, or from os.environ if `env` is None."""
    e = os.environ if env is None else env
    return Config(
        kafka_bootstrap_servers=e.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic_transactions=e.get("TOPIC_TRANSACTIONS", TOPIC_TRANSACTIONS),
        topic_alerts=e.get("TOPIC_ALERTS", TOPIC_ALERTS),
        checkpoint_root=e.get("CHECKPOINT_ROOT", "data/checkpoints"),
        output_root=e.get("OUTPUT_ROOT", "data/out"),
        model_dir=e.get("MODEL_DIR", "ml/artifacts"),
    )


CONFIG = load_config()
