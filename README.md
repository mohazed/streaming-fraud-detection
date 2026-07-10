# fraud-stream

Real-time fraud detection over simulated payment transactions: a seeded Kafka
producer, a PySpark Structured Streaming job with four detection rules, and a
live dashboard. Final project for `osekoo/hands-on-spark-streaming`.

## Quick start

```
docker compose up
```

Then open **http://localhost:8000**. That's it — Kafka (KRaft mode), the
Spark job, the producer, and the API/dashboard all come up together, no host
Python or JVM required. First startup takes a couple of minutes longer than
later ones: the Spark image trains the LightGBM model once (`ml/train.py`,
~200k simulated records) and caches the artifacts under `ml/artifacts/`.

The producer sends 200,000 simulated transactions at 50/s (~1h6m of runtime);
restart it any time with `docker compose restart producer`. Tear everything
down with `docker compose down -v`.

For local development without Docker (once you have a JDK 17 + this
project's Python deps installed): `make up && make topics && make run-local`.

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
are stateless row-level filters):

| Rule | Query | Logic |
|---|---|---|
| `high_value` | Q1 | `amount_eur > 1000` |
| `velocity` | Q2 | ≥4 transactions from one user inside 1 minute |
| `geo_hop` | Q3 | ≥2 countries from one user inside 5 minutes |
| `ml_score` | Q1 | LightGBM `p_fraud > tau`, optional/bonus |

Every alert is written simultaneously to a Kafka topic (`fraud-alerts`),
partitioned Parquet (`data/out/`), and the console — all through one
`write_alerts()` function so no sink can silently diverge from another.
`spark/dedup.py` suppresses the duplicate alerts that `outputMode("update")`
would otherwise re-emit as a growing window keeps matching its own filter.

### The model (bonus, Phase 3)

`ml/train.py` trains a LightGBM booster on 200k simulated records (temporal
80/20 split). **PR-AUC (average precision) = 0.21**, well under a "good"
score - see [Limitations](#limitations) for why that's expected here rather
than a bug. `spark/scoring.py` refuses to serve a model whose feature names
don't match `common/contracts.py::FEATURE_ORDER` exactly.

## Deviations from the brief

Three deliberate deviations from the assignment brief, each because the
brief's own generator contradicts one of its own rules or framings.

**1. `location` is a city; rule 3 asks for countries.** The brief's sample
generator emits `faker.city()` for `location` - a city name alone carries no
country, but rule 3 asks for "transactions in multiple **countries** within 5
minutes." The brief's own data cannot satisfy the brief's own rule. Fix:
`common/cities.py` ships a small static `city -> (country, lat, lon)` table;
Spark broadcast-joins it to derive `country`. No geocoding API, no network -
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
  deterministic generator - there is no real payment data anywhere in this
  project, at runtime or in the tests.
- **Spark runs in local mode** (`--master local[*]`), inside a single
  container. There is no cluster, no multi-executor serialization story, and
  no distributed state store - by design (see `PLAN.md` §3), not as a
  shortcut that was meant to be temporary.
- **The model learns the simulator's fraud archetypes, not real fraud
  patterns.** Of the fraud PLAN.md's simulator injects, only the `whale`
  archetype (~18% of positives) is a pure amount anomaly visible to a
  row-level classifier; `burst` and `traveller` (~82% of positives) draw
  amounts from that same user's ordinary spending profile by construction -
  statistically indistinguishable from a normal transaction in every one of
  the model's nine features, since velocity/geo context is deliberately R2's
  and R3's job, not R4's. That caps the achievable PR-AUC at roughly 0.20 for
  this label mix - not a training bug, and R1-R3 alone already satisfy the
  brief's "at least 3 fraud detection rules" requirement in full.

## Deliverables (PLAN.md §15)

- `producer/`, `spark/` - source for the producer and the Spark app
- `docs/sample_alerts.jsonl` - 100 real alerts from a local pipeline run
- `docs/spark_console.log` - console-sink output from that same run
- `docs/dashboard.png` - dashboard screenshot, replaying that run's alerts

## Development

```
make test      # pytest, ~95 tests (needs JAVA_HOME pointed at a JDK 17 - see below)
make cov       # same, plus the 85% coverage floor
make train     # retrain ml/artifacts/{model.txt,threshold.json,user_profiles.parquet}
make smoke     # PLAN.md §13's 18-assertion end-to-end check (needs Docker)
```

`pyspark`'s `pandas_udf` (used by the optional model) needs a JDK 17 JVM and
a `PYSPARK_PYTHON` pointed at the same interpreter running pytest - export
both before `make test`/`make train` if your default `java` is newer:

```
export JAVA_HOME=/path/to/a/jdk-17
export PYSPARK_PYTHON=$(pwd)/.venv/bin/python3
```

See `PLAN.md` for the full design (contracts, the five known traps, phase
history) and `CODEBASE_NOTES.md` for the decisions log.
