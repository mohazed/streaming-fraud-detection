import json
import re
from datetime import datetime
from pathlib import Path

from common.cities import lookup
from common.contracts import (
    AMOUNT_MAX,
    AMOUNT_MIN,
    CURRENCIES,
    HIGH_VALUE_EUR,
    LABEL_FIELD,
    METHODS,
    TRANSACTION_FIELDS,
)
from producer.simulator import generate

GOLDEN_SEED = 42
GOLDEN_N = 1000
GOLDEN_START = datetime(2024, 1, 1, 0, 0, 0)
GOLDEN_FILE = Path(__file__).parent / "golden" / "seed42.jsonl"

USER_ID_RE = re.compile(r"^u\d{4}$")
TRANSACTION_ID_RE = re.compile(r"^t-\d{7}$")
TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _consecutive_fraud_runs(records: list[dict]) -> list[tuple[int, int]]:
    """(start_index, length) for each maximal run of consecutive is_fraud==1
    records belonging to the same user. Whale=1, traveller=3, burst=5..8 by
    construction, so length alone identifies the archetype."""
    runs = []
    i = 0
    while i < len(records):
        if records[i][LABEL_FIELD] == 1:
            j = i
            while (j < len(records) and records[j][LABEL_FIELD] == 1
                   and records[j]["user_id"] == records[i]["user_id"]):
                j += 1
            runs.append((i, j - i))
            i = j
        else:
            i += 1
    return runs


def _parse_ts(record: dict) -> datetime:
    return datetime.strptime(record["timestamp"], "%Y-%m-%dT%H:%M:%SZ")


def test_determinism():
    a = generate(42, 1000, GOLDEN_START)
    b = generate(42, 1000, GOLDEN_START)
    assert a == b


def test_golden_file():
    records = generate(GOLDEN_SEED, GOLDEN_N, GOLDEN_START)[:100]
    expected = [json.loads(line) for line in GOLDEN_FILE.read_text().splitlines()]
    assert records == expected


def test_has_exactly_eight_fields():
    records = generate(1, 200, GOLDEN_START)
    expected_keys = set(TRANSACTION_FIELDS) | {LABEL_FIELD}
    for r in records:
        assert set(r.keys()) == expected_keys


def test_field_formats_match_brief():
    records = generate(7, 5000, GOLDEN_START)
    for r in records:
        assert USER_ID_RE.match(r["user_id"]), r["user_id"]
        assert TRANSACTION_ID_RE.match(r["transaction_id"]), r["transaction_id"]
        if r[LABEL_FIELD] == 1 and r["amount"] > AMOUNT_MAX:
            continue  # whale records are deliberately allowed to exceed AMOUNT_MAX
        assert AMOUNT_MIN <= r["amount"] <= AMOUNT_MAX


def test_currency_and_method_domains():
    records = generate(3, 2000, GOLDEN_START)
    for r in records:
        assert r["currency"] in CURRENCIES
        assert r["method"] in METHODS


def test_timestamp_is_iso8601_with_z():
    records = generate(5, 500, GOLDEN_START)
    for r in records:
        assert TIMESTAMP_RE.match(r["timestamp"]), r["timestamp"]


def test_fraud_prevalence():
    records = generate(42, 10_000, GOLDEN_START)
    rate = sum(r[LABEL_FIELD] for r in records) / len(records)
    assert 0.005 <= rate <= 0.05, rate


def test_whale_amounts_exceed_threshold():
    records = generate(42, 5000, GOLDEN_START)
    whales = [r for r in records if r[LABEL_FIELD] == 1 and r["amount"] > HIGH_VALUE_EUR]
    assert whales, "expected at least one fraud record whose amount exceeds HIGH_VALUE_EUR"


def test_burst_fits_in_60s():
    records = generate(42, 5000, GOLDEN_START)
    runs = _consecutive_fraud_runs(records)
    bursts = [(i, length) for i, length in runs if length >= 5]
    assert bursts, "expected at least one burst archetype"
    for i, length in bursts:
        span = (_parse_ts(records[i + length - 1]) - _parse_ts(records[i])).total_seconds()
        assert length >= 4
        assert span < 60, span


def test_traveller_spans_multiple_countries():
    records = generate(42, 5000, GOLDEN_START)
    runs = _consecutive_fraud_runs(records)
    travellers = [(i, length) for i, length in runs if length == 3]
    assert travellers, "expected at least one traveller archetype"
    for i, length in travellers:
        countries = {lookup(records[k]["location"]).country for k in range(i, i + length)}
        assert len(countries) >= 2, countries


def test_normal_traffic_sometimes_exceeds_1000():
    records = generate(42, 5000, GOLDEN_START)
    normal_high = [r for r in records if r[LABEL_FIELD] == 0 and r["amount"] > 1000]
    assert normal_high, "normal traffic never exceeded 1000 - R1 precision would be trivially 1.0"


def test_no_duplicate_transaction_ids():
    records = generate(42, 5000, GOLDEN_START)
    ids = [r["transaction_id"] for r in records]
    assert len(ids) == len(set(ids))
