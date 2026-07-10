from common.config import load_config


def test_defaults_resolve_without_env():
    cfg = load_config({})
    assert cfg.kafka_bootstrap_servers == "localhost:9092"
    assert cfg.topic_transactions == "transactions"
    assert cfg.topic_alerts == "fraud-alerts"
    assert cfg.checkpoint_root == "data/checkpoints"
    assert cfg.output_root == "data/out"
    assert cfg.model_dir == "ml/artifacts"


def test_env_override():
    cfg = load_config({
        "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
        "TOPIC_TRANSACTIONS": "custom-transactions",
        "TOPIC_ALERTS": "custom-alerts",
        "CHECKPOINT_ROOT": "/tmp/ck",
        "OUTPUT_ROOT": "/tmp/out",
        "MODEL_DIR": "/tmp/model",
    })
    assert cfg.kafka_bootstrap_servers == "broker:29092"
    assert cfg.topic_transactions == "custom-transactions"
    assert cfg.topic_alerts == "custom-alerts"
    assert cfg.checkpoint_root == "/tmp/ck"
    assert cfg.output_root == "/tmp/out"
    assert cfg.model_dir == "/tmp/model"


def test_checkpoints_unique():
    cfg = load_config({})
    paths = {cfg.checkpoint_row, cfg.checkpoint_velocity, cfg.checkpoint_geo}
    assert len(paths) == 3
