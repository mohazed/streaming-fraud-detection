# PLAN.md — Real-Time Fraud Detection Pipeline

Final project for `osekoo/hands-on-spark-streaming` (`FINAL-PROJECT.md`).
This file is the **single source of truth**. Plan, contracts, and test spec live here
together so they cannot drift apart. Read it fully before writing any code.

---

## 1. The brief

Verified against the assignment file. Do not paraphrase from memory — this section
is the contract with the grader.

### Goal

Build a real-time analytics pipeline that ingests **simulated** payment transaction
events from Kafka, processes the stream with Spark Structured Streaming, detects
potentially fraudulent patterns, and outputs results to a dashboard sink or a file.

### Prescribed stack

| Component | Technology |
|---|---|
| Ingestion | Kafka producer (Python) |
| Streaming | Spark Structured Streaming (Scala or PySpark) |
| Detection | Spark windowed aggregations + filters |
| Output | Parquet sink, console, and Kafka topic |
| Optional | Local dashboard (e.g. Flask or Streamlit) |

### Event schema — exactly these seven fields

```json
{
  "user_id":        "u1234",
  "transaction_id": "t-001",
  "amount":         185.20,
  "currency":       "EUR",
  "timestamp":      "2025-06-04T10:12:33Z",
  "location":       "Paris",
  "method":         "credit_card"
}
```

From the brief's own generator: `user_id` is `u{1000..9999}`, `transaction_id` is
`t-{i:07}`, `amount` is uniform over `[5.0, 5000.0]` rounded to 2dp,
`currency ∈ {EUR, USD, GBP}`, `location` is `faker.city()`,
`method ∈ {credit_card, debit_card, paypal, crypto}`.

### Required steps

1. **Producer** — simulate 10–100 transactions/second, realistic variety across
   users, amounts, timestamps, locations. Push to Kafka topic `transactions`.
2. **Spark** — read `transactions`, parse and transform JSON, apply logic such as:
   - high-value transactions over a threshold (e.g. `> 1000`)
   - more than 3 transactions from the same user in `< 1 minute`
   - transactions in multiple **countries** within 5 minutes

   The brief supplies the shape of the windowed aggregation directly:
   ```
   .withWatermark("timestamp", "5 minutes")
   .groupBy(window($"timestamp", "1 minute"), $"user_id")
   .count()
   ```
3. **Output** — write suspicious events to Kafka topic `fraud-alerts`, **and** to a
   parquet file, **and** to console. Optionally update a dashboard.

### Bonus

Consume `fraud-alerts` with a Python script. Display flagged events in a simple web
UI using Flask or Streamlit.

### Grading

| Criterion | Points |
|---|---|
| Kafka producer implemented and sending | 3 |
| Spark pipeline correctly reading stream | 3 |
| JSON parsing and schema enforcement | 3 |
| At least 3 fraud detection rules | 6 |
| Output to sink (file, Kafka, console) | 3 |
| Code quality and structure | 2 |
| Bonus: live dashboard or alert system | +2 |

### Deliverables

- Source for producer and Spark app
- Sample output or logs
- README with instructions to run locally **with Docker (Kafka + Spark)**
- Optional dashboard screenshot

---

## 2. Where we deliberately deviate, and why

Three deviations. Each is forced, each is documented in the README. A grader who
notices them should see a decision, not an accident.

### 2.1 `location` is a city; rule 3 needs countries

The brief's generator emits `faker.city()`. Rule 3 asks for *"transactions in
multiple **countries** within 5 minutes."* **The brief's own data cannot satisfy the
brief's own rule.** A city name alone carries no country.

**Resolution**: `common/cities.py` ships a 40-row static table mapping
`city → (country_iso2, lat, lon)`. The producer emits `location` as a city name,
exactly per the schema. Spark broadcast-joins the table to derive `country`.
No API, no geocoding, no network. The seven required fields are untouched.

### 2.2 Timestamps are `now()`, not backdated

The brief's sample generator writes one million rows to a file with
`ref_start_time + i seconds` — event time advances one second per record. Replaying
that at 50 records/second would advance event time 50× faster than wall clock, which
makes the velocity rule meaningless and the "real-time" framing a fiction.

**Resolution**: our producer stamps `timestamp` with `now()` at emission, ISO-8601
with a `Z` suffix — matching the brief's *example* JSON, which does use `Z`. Event
time and wall clock agree. Watermarks behave. Latency is measurable.

### 2.3 One extra field: `is_fraud`

The simulator knows which transactions it injected as fraud. It ships that as an
eighth field so the dashboard can compute live precision and recall.

**It is never read by any rule, never a model feature, and asserted absent from
`FEATURE_ORDER` at import time.** The seven required fields are present and
unmodified; `from_json` enforces all eight explicitly. This is additive, not a
change to the contract.

---

## 3. Design principles

These exist to make the implementation hard to get wrong. Violating one is a bug
even if the code runs.

1. **One data source.** A seeded simulator. No downloads, no APIs, no network at
   runtime or test time. Ever.
2. **Spark runs in local mode.** `--master local[*]`, inside the container. Driver
   and executor share a process and a filesystem. This removes serialization,
   `SparkFiles`, and broadcast-distribution as entire categories of failure.
3. **Pure core, thin adapters.** Every piece of logic is a pure function over plain
   Python/pandas types. Spark and Kafka are one-line wrappers around it. If a
   function needs a `SparkSession` to be tested, it is doing too much.
4. **Frozen contracts.** Schemas, feature order, and rule names are constants in
   `common/contracts.py`, pinned by tests against hardcoded literals.
5. **Determinism.** Same seed → byte-identical output. Golden-file tested.
6. **Line budgets.** Each module has a soft cap (§10). Exceeding it means the design
   is wrong, not that the cap is wrong.
7. **No leakage.** `is_fraud` never reaches a feature or a rule.

---

## 4. Architecture

```
                     ┌──────────────────┐
                     │  simulator.py    │  seeded, pure, no I/O
                     └────────┬─────────┘
                              │ list[dict]
                     ┌────────▼─────────┐
                     │  producer.py     │  10–100 tx/s per the brief
                     └────────┬─────────┘
                              │
                   Kafka (KRaft): topic `transactions`
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌────▼──────┐        ┌─────▼──────┐       ┌──────▼──────┐
   │ Q1 row    │        │ Q2 velocity│       │ Q3 geo_hop  │
   │ stateless │        │ 1-min win  │       │ 5-min win   │
   │ R1 + R4   │        │ R2         │       │ R3          │
   └────┬──────┘        └─────┬──────┘       └──────┬──────┘
        │                     │                     │
        └──────────► write_alerts(df, id) ◄─────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
          parquet      kafka `fraud-alerts`  console
                              │
                     ┌────────▼─────────┐
                     │ api/consumer.py  │  ← the brief's "Python script
                     │  thread + deque  │     that consumes fraud-alerts"
                     └────────┬─────────┘
                     ┌────────▼─────────┐
                     │ FastAPI          │  GET /api/alerts  (snapshot)
                     │                  │  GET /api/stream  (SSE)
                     └────────┬─────────┘
                     ┌────────▼─────────┐
                     │ web/ index.html  │  vanilla JS, no build step
                     └──────────────────┘
```

**Three streaming queries, one shared sink function.** Each is independently simple,
independently testable, and fails in isolation. Q1 is stateless. Q2 and Q3 are
windowed aggregations in exactly the shape the brief prescribes.

**Kafka in KRaft mode** (`apache/kafka:3.7`, single container, no Zookeeper). The
brief names Kafka and requires it in Docker. Do not substitute Redpanda.

**FastAPI is on-spec.** The overview table says "e.g. Flask or Streamlit", and the
bonus section describes precisely this: a Python script consuming `fraud-alerts`
feeding a simple web UI. If you run short on time, §18 says how to fall back.

---

## 5. Contracts (`common/contracts.py`)

Short, frozen, imported everywhere. **No magic strings elsewhere.**

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

Also exports `TRANSACTION_SCHEMA` and `ALERT_SCHEMA` as `StructType`s, both derived
from the tuples above so they cannot drift.

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

`p_fraud` is null for R1/R2/R3.

---

## 6. The simulator (`producer/simulator.py`)

Pure. Seeded. No I/O, no Kafka, no ambient clock.

```python
def generate(seed: int, n: int, start: datetime) -> list[dict]:
    """Deterministic. Same (seed, n, start) -> identical list, always."""
```

Match the brief's generator where it makes sense: `user_id` in `u1000..u9999`,
`transaction_id` as `t-{i:07}`, `amount` in `[5, 5000]`, the given currency and
method domains. Diverge where the brief's uniform-random draw would make the project
worse:

- **Amounts are log-normal per user**, not uniform. A user has a spending profile
  assigned at creation. This is what makes `amount_z` a meaningful feature later,
  and it is what "realistic data" in the brief's step 1 actually means.
- **`location` is drawn from `common/cities.py`**, weighted toward a per-user home
  city. Most users look geographically stable. See §2.1.

### Fraud injection

~97–99% normal traffic. Three archetypes injected, each targeting one rule:

| Archetype | Pattern | Trips |
|---|---|---|
| `whale` | one transaction, 1200–8000 EUR | R1, probably R4 |
| `burst` | 5–8 transactions from one user inside 40s | R2 |
| `traveller` | 3 transactions, 3 countries, inside 3 min | R3 |

Injected transactions carry `is_fraud: 1`; everything else `0`.

**Normal traffic must sometimes exceed 1000 EUR on its own.** The brief's own
amount range is `[5, 5000]`, so this happens naturally. Do not suppress it — if
every high-value transaction is fraud, R1's precision is trivially 1.0 and the
dashboard's metrics panel is a lie.

Rate: `--rate` flag, default 50, range 10–100 per the brief.

### Timestamps

`now()` at emission, ISO-8601 with `Z`. See §2.2. This is the single most important
line in the producer: backdated event time silently drops every row behind the
watermark.

---

## 7. The rules

Four rules. The brief requires three; R4 is upside. Each is a pure Spark
transformation in `spark/rules.py` returning a DataFrame conforming to `ALERT_SCHEMA`.

### R1 `high_value` — stateless, Q1

```python
df.filter(col("amount_eur") > HIGH_VALUE_EUR)
```

### R2 `velocity` — 1-minute window, Q2

The brief's snippet, in PySpark:

```python
(df.withWatermark("timestamp", WATERMARK)
   .groupBy(window(col("timestamp"), VELOCITY_WINDOW), col("user_id"))
   .count()
   .filter(col("count") >= VELOCITY_MIN_COUNT))
```

`outputMode("update")`.

### R3 `geo_hop` — 5-minute window, Q3

```python
(df.withWatermark("timestamp", WATERMARK)
   .groupBy(window(col("timestamp"), GEO_WINDOW), col("user_id"))
   .agg(approx_count_distinct("country").alias("n_countries"),
        collect_set("country").alias("countries"))
   .filter(col("n_countries") >= GEO_MIN_COUNTRIES))
```

`outputMode("update")`.

> **Known risk.** `collect_set` inside a streaming aggregation is supported but has
> been finicky across Spark versions. If it raises, drop it and keep
> `approx_count_distinct` alone; `detail` then says "3 countries" without listing
> them. Do not spend more than 15 minutes on this.

### R4 `ml_score` — stateless, Q1

`p_fraud > tau` from a LightGBM booster, scored via `pandas_udf`. See §9.

---

## 8. The five traps

Everything else was designed away. These five are real. Each has a named guard.
A, B, C live in Spark. D and E only exist because we chose FastAPI over Streamlit —
that is part of the price.

### Trap A — `outputMode("append")` on a windowed aggregation emits nothing

Until the watermark passes the window end, `append` emits zero rows. The console sits
silent for five minutes and you conclude the pipeline is broken.

**Guard**: `outputMode("update")` on Q2 and Q3. Pinned by
`tests/streaming/test_watermark_emission.py`.

### Trap B — `update` mode re-emits growing windows, producing duplicate alerts

A user hits 4 transactions, alert fires. They hit 5, the same window re-emits with
`count=5`, alert fires again. The dashboard floods.

**Guard**: `spark/dedup.py` — an `AlertDeduper` holding a set of
`(rule, user_id, window_start)` keys, evicting entries older than 10 minutes. Plain
Python, no Spark, unit-tested standalone. Every windowed query passes alerts through
it before `write_alerts`.

This is the bug that will bite you if nothing else does.

### Trap C — one checkpoint directory per query

Three queries, three paths: `data/checkpoints/{row,velocity,geo}/`. Sharing one
corrupts state in confusing ways.

**Guard**: paths derived from query name in `config.py`;
`tests/test_config.py::test_checkpoints_unique`. `make clean` wipes them.

### Trap D — `uvicorn --reload` starts two Kafka consumers

The reloader forks. Both processes run the lifespan hook and join the same consumer
group. `fraud-alerts` has one partition, so one consumer gets everything and the
other gets nothing — and which one serves your browser is a coin flip.

**Guard**: never run the API with `--reload`. Consumer group is
`dashboard-{uuid4()}` per process with `auto.offset.reset=latest`, so even two
processes both receive everything. `tests/test_api.py::test_no_reload_in_makefile`.

### Trap E — an idle browser tab backs up the server

Each SSE client gets an `asyncio.Queue`. A client that stops reading (backgrounded
tab, sleeping laptop) grows it until the process dies.

**Guard**: `asyncio.Queue(maxsize=200)`. On `QueueFull`, drop the **oldest** item and
increment a `dropped` counter exposed at `/healthz`. `publish()` never blocks.

---

## 9. The model (optional, Phase 3)

The brief does not ask for this. It is ~90 lines, it is the only thing that
distinguishes the project, and in local mode it is safe. Cut it first if time runs
short (§18).

### Training (`ml/train.py`)

- Generate 200k records via `simulator.generate(seed=42, ...)`.
- Temporal 80/20 split. Never shuffle.
- `user_profiles.parquet` built from the **train split only**:
  `user_id, amt_mean, amt_std, tx_count`.
- Features: exactly `FEATURE_ORDER`. Nine features, all from the brief's seven
  fields plus the profile.
- LightGBM, `scale_pos_weight = neg/pos`.
- **Report average precision (PR-AUC), not ROC-AUC.** At ~1% positives ROC-AUC reads
  ~0.99 and means nothing. Print it only as a footnote with a comment saying why.
- Threshold by argmax-F1 on the validation PR curve → `ml/artifacts/threshold.json`.
- `booster.save_model("ml/artifacts/model.txt")`. Text format. Never pickle.

Expect PR-AUC 0.85–0.95. It is a simulator; the model is learning the archetypes.
**Say so in the README.** An honest 0.90 on synthetic data beats a suspicious 0.99
with no caveat.

### Serving (`spark/scoring.py`)

```python
_booster = None

def _load():
    global _booster
    if _booster is None:
        _booster = lgb.Booster(model_file=str(MODEL_PATH))   # local mode: plain path
        assert tuple(_booster.feature_name()) == FEATURE_ORDER, \
            "model features do not match FEATURE_ORDER - retrain"
    return _booster

def score_frame(X: pd.DataFrame) -> pd.DataFrame:
    """Pure. X has exactly FEATURE_ORDER columns, in order. Unit-tested directly."""
    assert tuple(X.columns) == FEATURE_ORDER
    return pd.DataFrame({"p": _load().predict(X)})

@pandas_udf("double")
def score_udf(*cols): ...   # two lines: assemble, call score_frame, return
```

No `SparkFiles`, no `addFile` — we are in local mode. The `feature_name()` assertion
fires at startup: a retrained model with reordered features scores garbage
**silently**, and nothing else in this system would catch it.

---

## 10. Repository layout and line budgets

```
fraud-stream/
├── PLAN.md                       <- this file
├── CODEBASE_NOTES.md             <- Claude Code maintains
├── README.md
├── docker-compose.yml            ~60   kafka + spark + producer + api
├── Dockerfile.spark              ~20   pre-warmed ivy cache
├── Makefile                      ~40
├── requirements.txt
├── pytest.ini
├── common/
│   ├── config.py                 ~50   paths, broker, topics, from env
│   ├── contracts.py              ~70   §5, frozen
│   └── cities.py                 ~50   static table + lookup
├── producer/
│   ├── simulator.py              ~120  pure, seeded
│   └── producer.py               ~80   kafka adapter, --rate --seed --limit
├── ml/
│   ├── train.py                  ~110
│   └── artifacts/                model.txt, user_profiles.parquet, threshold.json
├── spark/
│   ├── job.py                    ~90   session, 3 queries, awaitAnyTermination
│   ├── enrich.py                 ~40   city join, amount_eur, features
│   ├── rules.py                  ~90   R1..R4 -> ALERT_SCHEMA
│   ├── scoring.py                ~40   §9
│   ├── dedup.py                  ~40   Trap B
│   └── sinks.py                  ~50   write_alerts(df, batch_id)
├── api/
│   ├── main.py                   ~80   FastAPI, lifespan, 4 routes
│   ├── consumer.py               ~70   kafka thread -> ring buffer
│   └── broadcast.py              ~60   asyncio fan-out, bounded queues
├── web/
│   ├── index.html                ~90   no build step, no CDN, no framework
│   ├── app.css                   ~180  design tokens, dark, dense
│   └── app.js                    ~180  EventSource, render, tau slider
├── scripts/smoke_test.py         ~90
└── tests/                        §11
```

**~1500 lines of production code.** If any file exceeds its budget by more than 50%,
stop and reconsider the design.

---

## 11. Test specification

`make test` must be green before any phase is called done. **No network in any test.**

```ini
# pytest.ini
[pytest]
markers = spark: needs a local SparkSession (slow)
addopts = -q --strict-markers
testpaths = tests
```

```python
# tests/conftest.py
@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
```

### `tests/test_contracts.py` — drift guards
- `test_transaction_fields_pinned` — compare against a hardcoded tuple literal.
  Changing the schema must require changing this test on purpose.
- `test_fields_match_the_brief` — exactly the seven names, in the brief's order.
- `test_feature_order_pinned`
- `test_no_label_leakage` — `is_fraud` absent from `FEATURE_ORDER`; no feature name
  contains `fraud`, `label`, or `target`
- `test_schema_fieldnames` — `TRANSACTION_SCHEMA.fieldNames() == TRANSACTION_FIELDS + (LABEL_FIELD,)`
- `test_topic_names_match_the_brief` — `transactions`, `fraud-alerts`
- `test_severity_covers_every_rule`
- `test_no_magic_strings` — grep every `.py` outside `common/` for `localhost:9092`,
  `"transactions"`, `"fraud-alerts"`. Zero hits.

### `tests/test_config.py`
- `test_defaults_resolve_without_env`
- `test_env_override`
- `test_checkpoints_unique` (Trap C)

### `tests/test_cities.py`
- `test_every_city_has_country_and_coords`
- `test_at_least_two_countries` — otherwise R3 can never fire
- `test_lookup_unknown_city_raises`

### `tests/test_simulator.py`
- `test_determinism` — `generate(42, 1000, T) == generate(42, 1000, T)`
- `test_golden_file` — first 100 records match `tests/golden/seed42.jsonl` byte for
  byte. Regenerate deliberately, never automatically.
- `test_has_exactly_eight_fields`
- `test_field_formats_match_brief` — `user_id` matches `u\d{4}`, `transaction_id`
  matches `t-\d{7}`, `amount` in `[5, 5000]` for non-whale records
- `test_currency_and_method_domains`
- `test_timestamp_is_iso8601_with_z`
- `test_fraud_prevalence` — over 10k records, rate within `[0.005, 0.05]`
- `test_whale_amounts_exceed_threshold`
- `test_burst_fits_in_60s` — spans < 60s, count >= 4, i.e. it *will* trip R2
- `test_traveller_spans_multiple_countries`
- `test_normal_traffic_sometimes_exceeds_1000`
- `test_no_duplicate_transaction_ids`

### `tests/test_producer.py`
- `test_produces_via_fake` — `FakeProducer` collecting messages; assert count, keys
  are `user_id`, every payload is valid JSON with the eight fields
- `test_rate_limiting` — `--rate 50` with an injected clock: 50 messages ≈ 1s (±20%)
- `test_rate_within_brief_bounds` — `--rate 5` and `--rate 500` are rejected
- `test_no_network_import` — `producer.simulator` imports nothing from `requests`,
  `kafka`, or `confluent_kafka`

### `tests/test_dedup.py` — Trap B
- `test_first_alert_passes`
- `test_duplicate_key_suppressed`
- `test_different_window_passes`
- `test_different_user_passes`
- `test_eviction_after_ten_minutes` — injected clock
- `test_memory_bounded` — 100k keys over a simulated hour, set stays bounded

### `tests/test_scoring.py`
- `test_score_frame_pure` — n rows in, n probabilities in `[0,1]` out
- `test_wrong_column_order_raises` — must **not** silently score garbage
- `test_feature_name_assertion` — a booster with reordered features raises at load
- `test_udf_not_fat` — `len(pickle.dumps(score_udf)) < 50_000`

### `tests/test_rules.py` `[spark]`
Every rule: positive, negative, boundary.
- R1: 1000.01 fires; 999.99 does not; exactly 1000.00 does not (strict `>`);
  1200 USD fires only after `amount_eur` conversion
- R2: 4 txs in 59s fires; 3 in 59s does not; 4 spanning 61s does not; two users with
  2 each do not combine
- R3: Paris→London→Berlin in 3 min fires; Paris→Lyon in 3 min does not (same
  country); 2 countries spanning 6 min does not
- R4: `p > tau` fires; `p == tau` does not
- `test_alerts_conform_to_schema`
- `test_rules_do_not_read_label` — invert `is_fraud` in the input; alert counts
  unchanged

### `tests/test_enrich.py` `[spark]`
- `test_city_join_adds_country`
- `test_amount_eur_conversion` — 100 USD → 92.00 EUR
- `test_unknown_city_does_not_drop_row` — left join, null country, row survives

### `tests/streaming/test_watermark_emission.py` `[spark]` — Trap A

**Write this before you write R2. Watch it fail. Then make it pass.** It is the
highest-value test in the repository. File source in, memory sink out, driven by
`processAllAvailable()`:

```python
def test_velocity_emits_without_waiting_for_window_close(spark, tmp_path):
    src = tmp_path / "src"; src.mkdir()
    stream = spark.readStream.schema(TRANSACTION_SCHEMA).json(str(src))
    q = (velocity_rule(enrich(stream))
         .writeStream.format("memory").queryName("out")
         .outputMode("update")
         .option("checkpointLocation", str(tmp_path / "ck")).start())
    write_jsonl(src / "b1.json", four_txs_one_user_within_60s())
    q.processAllAvailable()
    assert spark.sql("select * from out").count() > 0, \
        "velocity emitted nothing - check outputMode (Trap A)"
    q.stop()
```

Plus:
- `test_geo_hop_across_batches` — batch 1 Paris, batch 2 London 60s later. Only
  fires if state survived the batch boundary.
- `test_malformed_json_does_not_kill_query` — bad line to dead-letter, query lives.

### `tests/test_sinks.py`
- `test_write_alerts_calls_all_three` — inject three `FakeSink`s, each called once
- `test_persist_and_unpersist` — `persist()` before the first write, `unpersist()` in
  a `finally`, even when a sink raises
- `[spark]` `test_parquet_lands_partitioned`

### `tests/test_broadcast.py` — Trap E
Pure asyncio (`pytest-asyncio`). No Kafka, no FastAPI.
- `test_subscribe_then_publish`
- `test_two_subscribers_both_receive`
- `test_unsubscribe_stops_delivery`
- `test_queue_full_drops_oldest` — fill to maxsize, publish once; queue still holds
  maxsize, the oldest is gone, `dropped == 1`
- `test_publish_never_blocks` — a subscriber that never reads does not stall
  `publish()`; assert it returns within 10ms
- `test_slow_subscriber_does_not_affect_fast_one`

### `tests/test_consumer.py`
No broker. Inject a `FakeKafkaConsumer` yielding scripted messages.
- `test_valid_alerts_land_in_buffer`
- `test_buffer_is_bounded` — 1000 in, `len(buffer) == 500`
- `test_newest_first`
- `test_invalid_json_increments_counter_and_does_not_raise`
- `test_group_id_is_unique_per_instance` (Trap D)
- `test_auto_offset_reset_is_latest`

### `tests/test_api.py`
`fastapi.testclient.TestClient`, consumer replaced by a fake.
- `test_healthz_shape` — `{status, alerts_seen, dropped, consumer_alive, uptime_s}`
- `test_alerts_snapshot_respects_limit`
- `test_alerts_snapshot_newest_first`
- `test_stream_content_type` — `text/event-stream`
- `test_stream_emits_frames` — publish two alerts, read two `data:` frames, each
  valid JSON conforming to `ALERT_SCHEMA`
- `test_stream_frame_format` — every frame ends with exactly `\n\n`
- `test_index_served_at_root`
- `test_no_reload_in_makefile` — grep the `api` target for `--reload`, zero hits

### Coverage

`make cov` → **85% floor on `common/`, `producer/`, `spark/`, `api/`.**
`web/` exempt — no JS test runner in this project, and adding one is not worth it.
The frontend is validated by `make smoke` and by looking at it.

---

## 12. Phases

Six phases. Each ends with an acceptance check **you** run. Do not start phase N+1
until N's check passes.

### Phase 0 — Skeleton, contracts, harness (~1 h)

`docker-compose.yml` (Kafka KRaft only for now), `requirements.txt` (pinned),
`Makefile`, `pytest.ini`, `tests/conftest.py`, `tests/fakes.py`, the full tree,
`common/{config,contracts,cities}.py`, and `tests/test_{contracts,config,cities}.py`.

Derive the `spark-sql-kafka` package string from `pyspark.__version__` inside the
Makefile so they cannot drift.

**Accept**: `make up && make topics && make test` — green, both topics listed.

### Phase 1 — Simulator + producer (~1.5 h)

`producer/simulator.py`, `producer/producer.py`, their tests,
`tests/golden/seed42.jsonl`.

**Accept**: `make test` green. Consuming `transactions` shows well-formed JSON with
the brief's exact seven fields plus `is_fraud`.

### Phase 2 — Spark core (~2.5 h) — **the graded phase**

`spark/enrich.py`, `spark/rules.py` (R1–R3), `spark/dedup.py`, `spark/sinks.py`,
`spark/job.py`, all tests, plus the three streaming tests.

At the end of this phase the project scores **20/20** without the model or the
dashboard. Commit and `git tag phase-2-complete`. Everything after is upside.

**Accept**: with `--rate 50`, alerts appear in the console within 10 seconds. Each
rule fires at least once inside 60 seconds. `data/out/` fills with partitioned
parquet. `fraud-alerts` carries alerts. No duplicate `(rule, user_id, window)`.

### Phase 3 — Model (~1.5 h) — optional

`ml/train.py`, `spark/scoring.py`, R4, their tests.

**Accept**: `make train` prints PR-AUC ≥ 0.80. `make test` green. Alerts with
`rule: ml_score` carry `p_fraud`. Job startup dies loudly if `FEATURE_ORDER`
disagrees with the booster.

### Phase 4a — API layer (~1.5 h)

`api/consumer.py` — `confluent_kafka` on a daemon thread, group
`dashboard-{uuid4()}`, `auto.offset.reset=latest` (Trap D). Validates each message,
appends to `deque(maxlen=500)`, hands off via `loop.call_soon_threadsafe`. Invalid
messages increment a counter; the thread never dies.

`api/broadcast.py` — a `Broadcaster` over `set[asyncio.Queue]`. `subscribe()` returns
`Queue(maxsize=200)`. `publish()` drops oldest on `QueueFull`, increments `dropped`,
never blocks (Trap E). Pure asyncio.

`api/main.py` — FastAPI, four routes:

| Route | Returns |
|---|---|
| `GET /healthz` | `{status, alerts_seen, dropped, consumer_alive, uptime_s}` |
| `GET /api/alerts?limit=200` | JSON snapshot, newest first |
| `GET /api/stream` | `text/event-stream`, one `data: {...}\n\n` per alert |
| `GET /` | `web/index.html` via `StaticFiles` |

`lifespan` starts the consumer thread on startup, joins it on shutdown.
**No `--reload`, ever.**

**Accept**: `curl localhost:8000/healthz` → `consumer_alive: true`.
`curl -N localhost:8000/api/stream` prints frames as the producer runs. `make test` green.

### Phase 4b — Frontend (~1.5 h)

**No build step. No npm. No CDN. No framework.** Three static files served by FastAPI.
Everything works offline.

`web/app.js`:
- On load, `fetch('/api/alerts?limit=200')` for a snapshot, render.
- Then `new EventSource('/api/stream')`, prepend each alert. `EventSource` reconnects
  by itself; show a connection pill (`live` / `reconnecting`).
- Client-side array capped at 500.
- Charts are hand-rolled inline SVG bars (~30 lines). No chart library.

`web/app.css`:

```css
:root {
  --bg: #0d1117; --surface: #161b22; --border: #262c36;
  --text: #e6edf3; --text-dim: #7d8590;
  --mono: ui-monospace, SFMono-Regular, Menlo, monospace;
  --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
  --sev-medium: #d29922; --sev-high: #db6d28; --sev-critical: #f85149;
}
```

System fonts only. No Google Fonts — that is a network call, and §3 forbids it.

Non-negotiable design rules, because they are what make it look intentional rather
than templated:
- **Severity is the only saturated colour on the page.** Everything else is grey.
- **Tabular numerals everywhere numbers appear**: `font-variant-numeric: tabular-nums`,
  monospace for amounts, IDs, probabilities, timestamps. Columns must align.
- One page. No navigation, no routing. `--surface` cards on `--bg`, 1px borders, no
  shadows, no gradients, radius ≤ 6px.
- New alerts fade in over 150ms. Nothing else animates.

**Three panels. That is the whole UI.**

1. **Vitals** (top strip) — alerts/min, total alerts, alerts-by-rule as SVG bars,
   alerts-by-severity. Connection pill.
2. **Feed** (centre, a `<table>`) — newest first. Columns: severity swatch, rule,
   `user_id`, amount + currency, location, relative time. Row click expands a
   `<details>` with the raw JSON.
3. **Metrics** (right) — precision, recall, and a 2×2 confusion matrix computed **in
   JavaScript** against `is_fraud` over the buffered alerts. A **tau slider**
   re-thresholds `p_fraud` and recomputes all four numbers on `input`. Client-side
   only. It must never call the API and must never restart Spark.

**Accept**: alerts appear in the browser < 3s after Spark emits them. Killing and
restarting `spark/job.py` needs no page refresh — `EventSource` reconnects.

### Phase 5 — Docker, docs, smoke (~1.5 h)

The brief requires Docker (Kafka + Spark). Compose brings up the whole stack:

```yaml
services:
  kafka:      # apache/kafka:3.7, KRaft, no zookeeper
  spark-job:  # build: Dockerfile.spark, depends_on kafka
  producer:   # same image, runs producer.py --rate 50
  api:        # uvicorn, no --reload, port 8000
```

`Dockerfile.spark` pre-warms the Ivy cache at build time
(`RUN spark-submit --packages ... --version`) so no jar is downloaded at container
start. That is the classic failure when venue wifi is bad.

`docker compose up` → open `http://localhost:8000`. That is the README's headline
command. `make run-local` remains for development.

`scripts/smoke_test.py`, `README.md`.

---

## 13. Smoke test (`scripts/smoke_test.py`)

Not pytest. Run this before you present.

```
1.  docker compose up -d; poll kafka until healthy (60s timeout)
2.  create topics `transactions` and `fraud-alerts`
3.  train if ml/artifacts/model.txt is missing
4.  start spark/job.py; wait for "3 queries active"
5.  start producer --rate 50 --seed <SEED> --limit 5000
6.  consume fraud-alerts for 90 seconds
7.  ASSERT >= 1 alert for EACH rule implemented
8.  ASSERT every alert validates against ALERT_SCHEMA
9.  ASSERT no duplicate (rule, user_id, window_start)        <- Trap B
10. ASSERT p95(alert_time - event_time) < 10s
11. ASSERT data/out/ contains >= 1 parquet file
12. ASSERT the spark log contains no ERROR lines
13. poll GET /healthz until consumer_alive
14. ASSERT GET /api/alerts?limit=50 returns >= 1 alert, newest first
15. ASSERT GET /api/stream yields >= 1 SSE frame within 30s
16. ASSERT /healthz reports dropped == 0                     <- Trap E
17. ASSERT GET / returns 200 and the body contains id="feed"
18. tear down; exit 0, or print exactly which assertion failed
```

`make smoke`. If it exits 0, the demo will work. If you skip it and demo live, it
won't.

**Step 5**: find a seed that produces at least one of each archetype within 5000
records. Find it once, hardcode it, write it here: `SEED = 42` (31 whale, 6 burst,
28 traveller in the first 5000 records - already the seed used everywhere else in
this project, so no new determinism to track).

---

## 14. Runtime assertions

Tests run on your laptop. These run in the job and fail loudly instead of quietly.

- `job.py` startup: `tuple(booster.feature_name()) == FEATURE_ORDER`, or die.
- `job.py` startup: the three checkpoint paths are distinct.
- `enrich.py`: no outgoing column matches `/fraud|label|target/i` before the
  DataFrame reaches `scoring.py`.
- `producer.py`: validate every record against `TRANSACTION_FIELDS` before send; if
  the reject rate exceeds 1%, exit non-zero rather than emitting garbage.
- `job.py`: if a query reports `numInputRows == 0` for 60 consecutive seconds while
  the producer runs, log `WARNING: stream starved - check watermark/offsets`.

---

## 15. Deliverables checklist (mapped to the brief)

- [ ] **Source for producer** → `producer/`
- [ ] **Source for Spark app** → `spark/`
- [ ] **Sample output or logs** → `docs/sample_alerts.jsonl` (100 real alerts from a
      smoke run) + `docs/spark_console.log` (60s of console sink output)
- [ ] **README with Docker instructions (Kafka + Spark)** → `docker compose up`,
      one command, verified from a clean clone
- [ ] **Dashboard screenshot** → `docs/dashboard.png`

---

## 16. Definition of done

- [ ] `make test` green; `make cov` ≥ 85%
- [ ] `make smoke` exits 0
- [ ] `docker compose up` works from a clean clone with no host Python
- [ ] All seven brief-mandated fields present; `from_json` with an explicit
      `StructType`; malformed rows to a dead-letter path
- [ ] Topics named exactly `transactions` and `fraud-alerts`
- [ ] At least 3 rules; each fires in the smoke test
- [ ] Parquet + Kafka + console, all from one `write_alerts`
- [ ] No duplicate alerts (Trap B)
- [ ] SSE reconnects without a page refresh after Spark restarts
- [ ] `/healthz` reports `dropped == 0` after a 90s run
- [ ] No `--reload`, no CDN, no npm, no Google Fonts
- [ ] README documents all three deviations from §2, explicitly
- [ ] README limitations section is honest: simulated data, local Spark mode, and
      the model learns the simulator's archetypes rather than real fraud

---

## 17. What was deliberately cut

Recorded so nobody re-adds it. Bitcoin mempool feed, OFAC sanctions list, live FX
APIs, the Kaggle dataset, `applyInPandasWithState`, RocksDB state store, Postgres,
Supabase, React, MapLibre, a metrics topic, `StreamingQueryListener`, SHAP
contributions, a weighted risk-score fusion layer, a geography scatter panel, and
two extra rules.

Each added a failure mode. None added a grading point.

---

## 18. If you run out of time

Cut in this exact order:

1. **The SVG bars in panel 1.** Plain numbers. Ugly, still informative.
2. **The tau slider.** Show precision/recall at the fixed threshold.
3. **The whole `api/` + `web/` layer** → fall back to a 150-line Streamlit app
   (`kafka-python` consumer on a daemon thread into `deque(maxlen=500)`,
   `st.fragment(run_every=2)`). It earns the identical +2. The brief names Streamlit
   by name. There is no shame in it.
4. **Phase 3 (the model) entirely.** R1–R3 alone satisfy "at least 3 fraud detection
   rules" for the full 6 points.

Phase 2 alone scores 20/20. Everything after it is discretionary.
