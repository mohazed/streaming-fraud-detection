# fraud-stream

Real-time fraud detection over simulated payment transactions: a seeded Kafka
producer, a PySpark Structured Streaming job with four detection rules, and a
live dashboard. Final project for `osekoo/hands-on-spark-streaming`.

## Contents

- [Quick start](#quick-start)
- [What comes up](#what-comes-up)
- [Architecture](#architecture)
- [Data model](#data-model)
- [Fraud detection rules](#fraud-detection-rules)
- [Output sinks](#output-sinks)
- [Dashboard](#dashboard-bonus)
- [Configuration](#configuration)
- [Local development without Docker](#local-development-without-docker)
- [Testing](#testing)
- [Deviations from the brief](#deviations-from-the-brief)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)
- [Project layout](#project-layout)
- [Deliverables](#deliverables)

## Quick start

```
docker compose up
```

Then open **http://localhost:8000**. That's it — Kafka (KRaft mode), the
Spark job, the producer, and the API/dashboard all come up together, no host
Python or JVM required.

First startup takes a couple of minutes longer than later ones: the Spark
image trains the LightGBM model once (`ml/train.py`, ~200k simulated
records) and caches the artifacts under `ml/artifacts/`. Every startup after
that reuses the cached model.

The producer sends 200,000 simulated transactions at 50/s (~1h6m of
runtime); restart it any time with `docker compose restart producer`. Tear
everything down with `docker compose down -v` (the `-v` also drops the
Kafka volume and topic offsets, so the next `up` starts clean).

## What comes up

`docker compose up` starts five things, in this dependency order:

| Service | Image / build | Role | Exposed |
|---|---|---|---|
| `kafka` | `apache/kafka:3.7.0` (KRaft, no Zookeeper) | The broker | `localhost:9092` |
| `kafka-init` | same image, one-shot | Creates `transactions` (3 partitions) and `fraud-alerts` (1 partition), then exits | — |
| `spark-job` | `Dockerfile.spark` | Trains the model if missing, then runs `spark/job.py` via `spark-submit` | — |
| `producer` | `Dockerfile.spark` | Runs `producer/producer.py --rate 50 --seed 42 --limit 200000` | — |
| `api` | `Dockerfile.spark` | FastAPI dashboard + SSE stream, serves `web/` | `localhost:8000` |

`spark-job`, `producer`, and `api` all wait on `kafka-init` completing
successfully, so topic partition counts are identical on every `up`, not
just the first one. All three share one Docker image (`Dockerfile.spark`) —
they only differ in the command Compose runs.

## Architecture

```
simulator.py (seeded, pure) -> producer.py -> Kafka: transactions
                                                    |
                    +-------------------+-----------------------+
                    |                   |                       |
              Q1 (stateless)     Q2 (1-min window)       Q3 (5-min window)
              R1 high_value      R2 velocity             R3 geo_hop
              R4 ml_score
                    |                   |                       |
                    +-------- write_alerts(df) --------+--------+
                                        |
                       parquet   Kafka fraud-alerts   console
                                        |
                            api/consumer.py (Kafka thread)
                                        |
                              FastAPI: /healthz /api/alerts
                                        /api/stream (SSE)
                                        |
                               web/ (vanilla JS dashboard)
```

Three streaming queries implement four rules (R1 and R4 share Q1 since both
are stateless row-level filters, so they don't need their own checkpoint or
watermark):

| Rule | Query | Type | Logic |
|---|---|---|---|
| `high_value` | Q1 | stateless filter | `amount_eur > 1000` |
| `velocity` | Q2 | 1-minute tumbling window | ≥4 transactions from one user inside 1 minute |
| `geo_hop` | Q3 | 5-minute tumbling window | ≥2 countries from one user inside 5 minutes |
| `ml_score` | Q1 | stateless filter, bonus | LightGBM `p_fraud > tau` |

`spark/enrich.py` runs before any rule: it broadcast-joins `common/cities.py`
to derive `country` from `location`, and converts `amount` to `amount_eur`
via fixed FX rates (`common/contracts.py::FX_TO_EUR`). Q2 and Q3 use
`.withWatermark("timestamp", "5 minutes")` before their `groupBy(window(...),
user_id)`, matching the brief's example exactly.

`spark/dedup.py` (`AlertDeduper`) suppresses the duplicate alerts that
`outputMode("update")` would otherwise re-emit every micro-batch a growing
window keeps matching its own filter — a user's 4th, 5th, 6th transaction in
one velocity window would otherwise all raise the same alert.

### The model (bonus)

`ml/train.py` trains a LightGBM booster on 200k simulated records (temporal
80/20 split, never shuffled). **PR-AUC (average precision) = 0.21**, well
under a "good" score — see [Limitations](#limitations) for why that's
expected here rather than a bug. `spark/scoring.py` refuses to serve a model
whose feature names don't match `common/contracts.py::FEATURE_ORDER`
exactly, so a retrained-but-reordered model fails loudly at startup instead
of scoring garbage silently.

## Data model

**Transaction** (`common/contracts.py::TRANSACTION_SCHEMA`), read from the
`transactions` topic and schema-enforced via `from_json`:

| Field | Type | Notes |
|---|---|---|
| `user_id` | string | e.g. `u1234` |
| `transaction_id` | string | e.g. `t-0000001` |
| `amount` | double | in `currency`, `5.0`-`5000.0` |
| `currency` | string | `EUR` \| `USD` \| `GBP` |
| `timestamp` | timestamp | ISO-8601, `Z` suffix, stamped at send time |
| `location` | string | a city name (`faker.city()`) |
| `method` | string | `credit_card` \| `debit_card` \| `paypal` \| `crypto` |
| `is_fraud` | int | simulator ground truth; see [deviation 3](#deviations-from-the-brief) |

The first seven fields are exactly the brief's schema, in the brief's order.

**Alert** (`common/contracts.py::ALERT_SCHEMA`), written to `fraud-alerts`,
Parquet, and console:

| Field | Type | Notes |
|---|---|---|
| `alert_id` | string | random UUID, one per emitted alert |
| `transaction_id`, `user_id` | string | from the triggering transaction |
| `event_time` | string | the transaction's `timestamp` |
| `alert_time` | string | when the rule fired |
| `rule` | string | one of `RULE_NAMES` |
| `severity` | string | `medium` / `high` / `critical`, per rule |
| `amount`, `currency`, `amount_eur` | — | from the transaction |
| `location`, `country` | string | `country` is `null` outside `geo_hop` |
| `p_fraud` | double, nullable | only set for `ml_score` |
| `detail` | string | human-readable reason, e.g. `"6 countries in window ..."` |
| `is_fraud` | int | carried through for the dashboard's live precision/recall panel |

## Output sinks

Every alert is written simultaneously to all three sinks the brief asks
for, through one `write_alerts()` function (`spark/sinks.py`) so no sink can
silently diverge from another:

- **Console** — `df.show(n=20, truncate=False)` on every micro-batch.
- **Parquet** — appended under `data/out/`, partitioned by `rule` (so
  `data/out/rule=high_value/`, `rule=velocity/`, etc).
- **Kafka** — published to the `fraud-alerts` topic.

## Dashboard (bonus)

A vanilla-JS single-page dashboard (`web/`) served by the FastAPI app
(`api/main.py`), live over Server-Sent Events — no build step, no
framework, no CDN.

| Route | Purpose |
|---|---|
| `GET /` | the dashboard itself (static files from `web/`) |
| `GET /api/stream` | SSE stream of alerts, one `data: {...}` frame per alert |
| `GET /api/alerts?limit=200` | snapshot of the most recent alerts (rolling buffer) |
| `GET /healthz` | `{status, alerts_seen, dropped, consumer_alive, uptime_s}` |

`api/consumer.py` runs a background thread (`AlertConsumer`) that subscribes
to `fraud-alerts` under a unique consumer group per process
(`dashboard-{uuid4()}`, `auto.offset.reset=latest`) and republishes each
alert to an in-process `Broadcaster` (`api/broadcast.py`), which fans it out
to every connected browser tab over bounded queues — a slow or idle tab
drops frames instead of blocking the producer thread.

The UI has three panels: **vitals** (alerts/min, running totals, breakdowns
by rule and severity), a **live feed** table, and a **precision/recall**
panel that uses the `is_fraud` ground-truth field to compute metrics against
an adjustable `p_fraud` threshold slider — entirely a debugging/demo aid,
never consulted by the detection rules themselves.

A screenshot from a real local run is at `docs/dashboard.png`. To demo the
UI without Docker or a broker, run `python scripts/demo_dashboard.py` — it
runs the real `api.main` app unmodified, with only the Kafka consumer
replaced by one that synthesizes a fake alert every ~0.5s.

## Configuration

Everything in `common/config.py` is overridable from the environment; the
defaults match `docker-compose.yml`:

| Env var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | broker address |
| `TOPIC_TRANSACTIONS` | `transactions` | input topic |
| `TOPIC_ALERTS` | `fraud-alerts` | output topic |
| `CHECKPOINT_ROOT` | `data/checkpoints` | Spark checkpoint root (one subdir per query) |
| `OUTPUT_ROOT` | `data/out` | Parquet sink root |
| `MODEL_DIR` | `ml/artifacts` | `model.txt` / `threshold.json` / `user_profiles.parquet` |

Rule thresholds and windows (`HIGH_VALUE_EUR=1000.0`,
`VELOCITY_WINDOW="1 minute"` / `VELOCITY_MIN_COUNT=4`,
`GEO_WINDOW="5 minutes"` / `GEO_MIN_COUNTRIES=2`, `WATERMARK="5 minutes"`)
are frozen constants in `common/contracts.py`, not env-configurable — they
are the brief's own numbers, deliberately not knobs.

## Local development without Docker

Needs a JDK 17 and this project's Python dependencies (`pip install -r
requirements.txt`) on the host:

```
make up          # docker compose up -d, waits for kafka to be reachable
make topics       # creates transactions + fraud-alerts if not already there
make run-local    # producer + spark-submit + api, all against that kafka
```

`pyspark`'s `pandas_udf` (used by the optional model) needs a JDK 17 JVM and
a `PYSPARK_PYTHON` pointed at the same interpreter you run pytest/make with —
export both if your default `java` is newer:

```
export JAVA_HOME=/path/to/a/jdk-17
export PYSPARK_PYTHON=$(which python3)
```

## Testing

```
make test      # pytest, ~95 tests
make test-unit  # same, skipping anything marked "spark" (fast, no JVM)
make cov        # pytest, plus an 85% coverage floor
make train      # retrain ml/artifacts/{model.txt,threshold.json,user_profiles.parquet}
make smoke      # 18-assertion end-to-end check against a real Kafka (needs Docker)
make clean      # removes __pycache__, .pytest_cache, .coverage, and local run data
```

Coverage: every rule has boundary tests (e.g. exactly 3 velocity hits does
*not* fire, exactly 1000.00 does *not* fire — the brief's ">" is strict),
the dedup/watermark/window-emission behavior has dedicated streaming tests
(`tests/streaming/`), and the dashboard's consumer/broadcaster/API layers are
tested without any real Kafka broker (fakes throughout).

## Deviations from the brief

Three deliberate deviations from the assignment brief, each because the
brief's own generator contradicts one of its own rules or framings.

**1. `location` is a city; rule 3 asks for countries.** The brief's sample
generator emits `faker.city()` for `location` — a city name alone carries no
country, but rule 3 asks for "transactions in multiple **countries** within 5
minutes." The brief's own data cannot satisfy the brief's own rule. Fix:
`common/cities.py` ships a small static `city -> (country, lat, lon)` table;
Spark broadcast-joins it to derive `country`. No geocoding API, no network —
the seven required schema fields are untouched.

**2. Timestamps are `now()`, not backdated.** The brief's sample generator
advances event time by one second per record written to a file. Replaying
that at 50 records/second would make event time run 50× faster than the wall
clock, making the 1-minute/5-minute windowed rules and "real-time" framing
meaningless. Fix: the producer stamps `timestamp` with `now()` at emission
time (ISO-8601 with a `Z` suffix, matching the brief's own example JSON).
Event time and wall clock agree, so watermarks behave and latency is
measurable.

**3. One extra field: `is_fraud`.** The simulator knows which transactions it
injected as fraud and ships that as an eighth field, purely so the dashboard
can compute live precision/recall. It is never read by any rule, never a
model feature (asserted absent from `FEATURE_ORDER` at import time and again
at runtime in `spark/enrich.py`), and the seven brief-mandated fields are
present, unmodified, and schema-enforced via `from_json`. Additive, not a
change to the contract.

## Limitations

- **The data is entirely simulated.** `producer/simulator.py` is a seeded,
  deterministic generator — there is no real payment data anywhere in this
  project, at runtime or in the tests.
- **Spark runs in local mode** (`--master local[*]`), inside a single
  container. There is no cluster, no multi-executor serialization story, and
  no distributed state store — by design, not as a shortcut that was meant
  to be temporary.
- **The model learns the simulator's fraud archetypes, not real fraud
  patterns.** Of the fraud archetypes the simulator injects, only the `whale`
  archetype (~18% of positives) is a pure amount anomaly visible to a
  row-level classifier; `burst` and `traveller` (~82% of positives) draw
  amounts from that same user's ordinary spending profile by construction —
  statistically indistinguishable from a normal transaction in every one of
  the model's nine features, since velocity/geo context is deliberately R2's
  and R3's job, not R4's. That caps the achievable PR-AUC at roughly 0.20 for
  this label mix — not a training bug, and R1-R3 alone already satisfy the
  brief's "at least 3 fraud detection rules" requirement in full.

## Troubleshooting

- **`WARN KafkaDataConsumer: ... not running in UninterruptibleThread ...
  KAFKA-1894`** in `spark-job` logs. Harmless and expected — it's Spark's
  own Kafka source connector warning that a consumer poll could theoretically
  hang if interrupted outside an `UninterruptibleThread` (a known upstream
  Kafka client quirk). It does not indicate a stuck or failed query; check
  `docker compose logs spark-job | grep -i error` (no `KAFKA-1894` hits) or
  the dashboard's `/healthz` (`alerts_seen` climbing, `consumer_alive: true`)
  to confirm the pipeline is actually healthy.
- **No alerts showing up.** Give it a minute — `velocity` and `geo_hop` only
  emit once the 5-minute watermark has passed a window's end, so the first
  alerts of those two rules lag behind `high_value`/`ml_score` by design.
  Check `docker compose logs producer` is still sending, and `docker compose
  logs spark-job` for the per-batch console sink output.
- **`spark-job` restarting in a loop.** It has `restart: on-failure:3`; check
  `docker compose logs spark-job` for the actual exception — most commonly a
  stale checkpoint from a previous run with an incompatible schema. Clearing
  checkpoints (`make clean`, or `rm -rf data/checkpoints/*` — as root if the
  files were written by the container) resolves it at the cost of reprocessing
  from the current Kafka offset.
- **First `docker compose up` slower than expected.** Expected — the Spark
  image trains the model on first boot if `ml/artifacts/model.txt` is
  missing (`make train` does the same thing manually). Subsequent starts
  reuse the volume-mounted `ml/artifacts/`.

## Project layout

```
producer/       simulator.py (pure, seeded) + producer.py (Kafka adapter)
spark/          job.py (wiring) + enrich/rules/dedup/sinks/scoring/health
common/         config.py (env) + contracts.py (frozen schemas/constants) + cities.py
ml/             train.py + artifacts/ (model.txt, threshold.json, user_profiles.parquet)
api/            main.py (FastAPI) + consumer.py (Kafka thread) + broadcast.py (SSE fan-out)
web/            vanilla-JS dashboard served by api/main.py
scripts/        smoke_test.py (end-to-end) + demo_dashboard.py (no-Docker demo)
tests/          unit + streaming tests (~95 tests)
docs/           sample_alerts.jsonl, spark_console.log, dashboard.png
docker-compose.yml, Dockerfile.spark, Makefile
```

## Deliverables

Mapped to the brief's evaluation criteria:

| Criteria | Where |
|---|---|
| Kafka producer implemented and sending | `producer/` |
| Spark pipeline correctly reading stream | `spark/job.py` |
| JSON parsing and schema enforcement | `common/contracts.py::TRANSACTION_SCHEMA`, `spark/job.py` |
| At least 3 fraud detection rules | `spark/rules.py` — 4 implemented (R1-R4) |
| Output to sink (file, Kafka, console) | `spark/sinks.py` |
| Code quality and structure | `tests/` (~95 tests), this README |
| Bonus: live dashboard or alert system | `api/`, `web/`, `docs/dashboard.png` |

Sample output from a real local run: `docs/sample_alerts.jsonl` (100 real
alerts) and `docs/spark_console.log` (console-sink output from that same
run).
