# CODEBASE_NOTES.md

## What we're building

A real-time fraud-detection pipeline for the `osekoo/hands-on-spark-streaming`
final project: a seeded Python simulator feeds a Kafka producer that publishes
synthetic payment-transaction events to the `transactions` topic; a PySpark
Structured Streaming job (`spark/job.py`) reads that topic, enriches each
record (city→country lookup, EUR conversion, derived features), and runs
**exactly three streaming queries implementing four detection rules**: Q1 is a
stateless, row-level query carrying both the high-value filter (R1) and the
optional LightGBM-scored rule (R4 — a column added inside Q1, not a query of
its own); Q2 is a 1-minute windowed velocity check (R2); Q3 is a 5-minute
windowed cross-country check (R3). Matching events are deduplicated and
written simultaneously to Kafka (`fraud-alerts`), Parquet, and the console. A
small FastAPI service consumes `fraud-alerts` and serves a no-build-step
vanilla-JS/CSS dashboard over Server-Sent Events so alerts, vitals, and live
precision/recall metrics appear in the browser within seconds of Spark
emitting them. Everything runs in Docker (Kafka KRaft + Spark local mode),
with no network calls and no external data source at runtime or test time.

## Contracts (PLAN.md §5, verbatim)

> Short, frozen, imported everywhere. **No magic strings elsewhere.**

```python
# The seven fields the brief mandates, in the brief's order.
TRANSACTION_FIELDS = ("user_id", "transaction_id", "amount", "currency",
                      "timestamp", "location", "method")

# Eighth field: simulator ground truth. Never a feature. See §2.3.
LABEL_FIELD = "is_fraud"

CURRENCIES = ("EUR", "USD", "GBP")
METHODS    = ("credit_card", "debit_card", "paypal", "crypto")
AMOUNT_MIN, AMOUNT_MAX = 5.0, 5000.0          # from the brief's generator

# Fixed rates. This is a simulation; there is no FX API and never will be.
FX_TO_EUR = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17}

RULE_HIGH_VALUE = "high_value"
RULE_VELOCITY   = "velocity"
RULE_GEO_HOP    = "geo_hop"
RULE_ML_SCORE   = "ml_score"
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
TOPIC_ALERTS       = "fraud-alerts"            # the brief's topic name
```

> Also exports `TRANSACTION_SCHEMA` and `ALERT_SCHEMA` as `StructType`s, both derived
> from the tuples above so they cannot drift.

### Alert record

```json
{
  "alert_id": "uuid", "transaction_id": "t-0000123", "user_id": "u4821",
  "event_time": "2026-07-10T10:12:33Z", "alert_time": "2026-07-10T10:12:35Z",
  "rule": "velocity", "severity": "high",
  "amount": 185.20, "currency": "EUR", "amount_eur": 185.20,
  "location": "Paris", "country": "FR",
  "p_fraud": 0.94,
  "detail": "5 transactions in window 10:12:00-10:13:00",
  "is_fraud": 1
}
```

> `p_fraud` is null for R1/R2/R3.

## Phase status

| Phase | Description | Status |
|---|---|---|
| 0 | Skeleton, contracts, harness | DONE |
| 1 | Simulator + producer | DONE |
| 2 | Spark core (the graded phase) | NOT STARTED |
| 3 | Model (optional) | NOT STARTED |
| 4a | API layer | NOT STARTED |
| 4b | Frontend | NOT STARTED |
| 5 | Docker, docs, smoke | NOT STARTED |

## Decisions log

- Three streaming queries, four rules. R1 and R4 share Q1 (both stateless).
  R4 must never get its own query or checkpoint.
- Phase 0: `requirements.txt` pinned to pyspark==3.5.8 (not the newer 4.x line —
  untested against this project's stack) with pyarrow==17.0.0 (the 14.x originally
  considered is built against numpy 1.x ABI and segfaults-on-import under
  numpy 2.x; 17.0.0 is the first line confirmed clean here).
- Phase 0: Kafka compose service carries two listeners (`PLAINTEXT` advertised as
  `localhost:9092` for host tools/tests, `DOCKER` advertised as `kafka:29092` for
  same-network containers) even though only one service exists yet, so Phase 5
  doesn't require rewiring listener config when `spark-job`/`producer`/`api`
  containers are added.
- Phase 0: `common/config.py` is a `load_config(env: dict | None)` function
  returning a frozen `Config`, not bare module constants — needed so
  `test_env_override` can pass a dict without mutating `os.environ` or reimporting
  the module.
- `tests/test_contracts.py::test_no_magic_strings` greps every `.py` file outside
  `common/` **and `tests/`** (tests must assert against literals on purpose) for
  `localhost:9092`, `"transactions"`, `"fraud-alerts"`.
- Phase 1: `producer/simulator.py::generate` walks a single synthetic clock
  starting at `start` and, at each step, rolls one `rng.random()` draw against
  three archetype probabilities (whale 0.4%, burst 0.15%, traveller 0.3%) before
  falling back to a normal transaction; an archetype is only started if enough
  `remaining` slots are left to complete it, so a burst/traveller can never be
  truncated by hitting `n`. `transaction_id` is assigned in a final pass over the
  finished list, so it always reflects final emission order and is trivially
  unique.
- Phase 1: normal amounts are per-user log-normal (`mu`, `sigma` fixed at user
  creation); 8% of users are "big spenders" (`mu` drawn from `[6.3, 7.2]`) whose
  *lognormal median alone* clears `HIGH_VALUE_EUR` — this is what makes
  `test_normal_traffic_sometimes_exceeds_1000` pass without any special-casing,
  per PLAN.md §6's warning that suppressing this would make R1's precision a lie.
- Phase 1: archetypes are identified in tests by run *length* alone (whale = 1,
  traveller = 3, burst = 5..8 consecutive same-user `is_fraud=1` records) rather
  than a 9th hidden field — the schema stays at exactly 8 fields
  (`test_has_exactly_eight_fields`).
- Phase 1: `producer/producer.py::send()` takes injectable `now`/`sleep`
  callables so tests can fake the clock (`test_rate_limiting`) without real
  `time.sleep`. Real timestamps are stamped at send time — never the
  simulator's synthetic clock — per PLAN.md §2.2; the golden-file test is the
  only place the synthetic timestamps are checked byte-for-byte.
- Phase 1: golden file `tests/golden/seed42.jsonl` = first 100 records of
  `generate(seed=42, n=1000, start=datetime(2024,1,1,0,0,0))`. Regenerate only
  deliberately (see the script in this session's history / re-derive from
  `producer/simulator.py`), never automatically from a test run.

## Known issues

- This dev sandbox has no `docker`/`docker compose`/Colima/Podman installed, so
  `make up` and `make topics` could not be exercised end-to-end here — `make up`
  fails immediately with `docker: No such file or directory`. The compose file and
  Makefile targets are written and reviewed against the standard KRaft
  single-node pattern for `apache/kafka:3.7.0`, but neither has run to green.
  Verify on a machine with Docker before trusting `make up && make topics`.
- `make test`/`make test-unit`/`make cov` were verified green (14 passed, 96%
  coverage) using a project-local `.venv` (`python3 -m venv .venv && .venv/bin/pip
  install -r requirements.txt`), not the bare `python3` on PATH, which has no
  packages installed. The Makefile's `PYTHON` var defaults to plain `python3`;
  override with `PYTHON=.venv/bin/python3` or activate the venv first.