from pathlib import Path

from common.contracts import (
    ALERT_SCHEMA,
    FEATURE_ORDER,
    LABEL_FIELD,
    RULE_NAMES,
    SEVERITY,
    TOPIC_ALERTS,
    TOPIC_TRANSACTIONS,
    TRANSACTION_FIELDS,
    TRANSACTION_SCHEMA,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_transaction_fields_pinned():
    assert TRANSACTION_FIELDS == ("user_id", "transaction_id", "amount", "currency",
                                   "timestamp", "location", "method")


def test_fields_match_the_brief():
    assert list(TRANSACTION_FIELDS) == [
        "user_id", "transaction_id", "amount", "currency",
        "timestamp", "location", "method",
    ]


def test_feature_order_pinned():
    assert FEATURE_ORDER == ("amount_eur", "log_amount", "hour", "dayofweek",
                              "is_night", "method_id", "currency_id",
                              "amount_z", "is_new_user")


def test_no_label_leakage():
    assert LABEL_FIELD not in FEATURE_ORDER
    assert not any(t in f for f in FEATURE_ORDER for t in ("fraud", "label", "target"))


def test_schema_fieldnames():
    assert TRANSACTION_SCHEMA.fieldNames() == list(TRANSACTION_FIELDS + (LABEL_FIELD,))
    assert "alert_id" in ALERT_SCHEMA.fieldNames()


def test_topic_names_match_the_brief():
    assert TOPIC_TRANSACTIONS == "transactions"
    assert TOPIC_ALERTS == "fraud-alerts"


def test_severity_covers_every_rule():
    assert set(SEVERITY) == set(RULE_NAMES)


def test_no_magic_strings():
    """Production code outside common/ must import topic/broker constants, never
    inline them. Tests are exempt: they assert against literals on purpose."""
    banned = ("localhost:9092", '"transactions"', '"fraud-alerts"')
    exempt_dirs = {"common", "tests", ".venv"}
    offenders = []
    for path in REPO_ROOT.rglob("*.py"):
        rel = path.relative_to(REPO_ROOT)
        if rel.parts[0] in exempt_dirs:
            continue
        text = path.read_text()
        for token in banned:
            if token in text:
                offenders.append(f"{rel}: {token}")
    assert not offenders, offenders
