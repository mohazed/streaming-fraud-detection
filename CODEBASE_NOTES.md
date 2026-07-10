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
| 2 | Spark core (the graded phase) | DONE |
| 3 | Model (optional) | DONE |
| 4a | API layer | DONE |
| 4b | Frontend | DONE |
| 5 | Docker, docs, smoke | DONE (Docker path unverified - see Known issues) |

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
- Phase 2: `spark/rules.py`'s windowed rules (`velocity_rule`, `geo_hop_rule`)
  build a "representative transaction" struct via `F.max_by(struct(...), col("timestamp"))`
  so the alert can carry per-transaction fields (transaction_id, amount,
  location...) that a `groupBy` aggregate would otherwise lose. That struct's
  event-time field is aliased `event_ts`, **not** `timestamp` - Spark's
  streaming analyzer fails to resolve a nested field literally named
  `timestamp` once a watermark is active on the outer column of the same
  name, raising `UNRESOLVED_COLUMN` even though every other field in the same
  struct resolves fine. Discovered via `tests/streaming/test_watermark_emission.py`.
- Phase 2: after `groupBy(F.window(...), ...)`, referencing `window_col.start`/
  `.end` again in a later `.select()` (e.g. to build the `detail` string)
  silently tries to **re-derive** `window()` from the raw `timestamp` column,
  which no longer exists post-aggregation, and fails the same way. Fix: reference
  the aggregate's own materialized `"window"` struct column
  (`F.col("window").getField("start")`), never the original `F.window(...)`
  expression object, after a `groupBy`.
- Phase 2: `velocity_rule`/`geo_hop_rule` output carries an extra `window_start`
  column beyond the 15 `ALERT_SCHEMA` fields - it's the dedup key's third
  component (Trap B) and is only needed driver-side, before `write_alerts`.
  `write_alerts` explicitly `.select(*ALERT_SCHEMA.fieldNames())` first, so
  `window_start` never reaches a sink. `R1` (`high_value_rule`) has no such
  column and skips dedup entirely - it's stateless/append-mode, so it cannot
  re-emit.
- Phase 2: `AlertDeduper` (Trap B) is applied **driver-side**, not as a Spark
  transformation - `spark/job.py::_dedup_and_write` `.collect()`s each
  micro-batch (small: one 5s trigger's worth of alerts), filters rows through
  `deduper.should_emit(rule, user_id, window_start)`, and rebuilds a DataFrame
  from only the allowed rows before calling `write_alerts`. Spark has no
  native "have I seen this key across batches" primitive short of
  `applyInPandasWithState` (explicitly cut, PLAN §17), and this is local mode
  with small per-trigger batches, so a plain Python `set` on the driver is the
  simplest correct thing.
- Phase 2: `spark/sinks.py::write_alerts` takes an injectable `sinks` sequence
  (default `None` -> `default_sinks(CONFIG)` = console + parquet + kafka) so
  tests and the acceptance demo can substitute fakes and stay off the network,
  matching `tests/fakes.py::FakeSink`'s existing shape. `spark/job.py::build_queries`
  threads the same `sinks` param through to all three queries' `foreachBatch`
  callbacks for the same reason.
- Phase 2 bug found via live demo, not a unit test: Q1/Q2/Q3 all append
  parquet to the **same** `output_root`, partitioned by `rule`. Spark's
  `FileOutputCommitter` stages each write job under a `_temporary/0/...`
  directory keyed by *path*, not by query - when two queries' micro-batch
  commits land at the same wall-clock moment (all three run on independent
  5s triggers against the same Kafka topic), one job's commit phase can
  `rename()` away a `_temporary` subdirectory the other job is still
  writing to, aborting it with `FileNotFoundException`. Fixed with a
  process-wide `threading.Lock()` in `make_parquet_sink` serializing the
  actual `.parquet()` call; console and Kafka sinks are unaffected and stay
  unlocked. Regression test: `tests/test_sinks.py::test_parquet_sink_survives_concurrent_writers`
  (three threads writing concurrently to one sink). Would have shipped broken
  without an end-to-end run - the pytest fixtures never exercise more than
  one query writing to a shared path at once.
- Phase 2: `spark/job.py::read_transactions` (the Kafka source) cannot be
  unit-tested in this sandbox at all - `spark.readStream.format("kafka")` fails
  with `Failed to find data source: kafka` unless the job is launched via
  `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_...` (the
  Makefile's `SPARK_KAFKA_PACKAGE`), which isn't wired into pytest. `main()`
  and `build_spark_session()` are similarly untested (thin entrypoint/session
  glue; `main()` also blocks forever on `awaitAnyTermination`). Per PLAN §3
  ("if a function needs a SparkSession to be tested, it is doing too much")
  these are treated as thin adapters, exercised only by the Phase 5 smoke
  test / a real `docker compose up`, not by `make test`. Everything else in
  `job.py` (`build_queries`, the three `foreachBatch` closures, the
  dedup-then-write wiring) is covered by `tests/streaming/test_job.py`,
  which drives it off a file source exactly like `test_watermark_emission.py`.
- Phase 2 acceptance demo (console alerts <10s, `data/out/` partitioning, a
  simulated `fraud-alerts` consume, no duplicate `(rule,user_id,window_start)`)
  was run with the same Kafka substitution as [[no_docker_sandbox]]: a custom
  `FileWritingProducer` (buffers `producer.producer.send()`'s real,
  now()-stamped output and flushes to JSON-lines files ~1x/sec) stood in for
  the `transactions` topic, and an in-memory list stood in for a
  `fraud-alerts` consumer. Everything else - `producer/simulator.generate`,
  `producer/producer.send`, `spark/job.py::build_queries`, the real console +
  parquet sinks writing to the project's actual `data/checkpoints/`/`data/out/`
  - was the production code path, unmodified. Result at seed 42, 3000 records,
  rate 50: first alert at t=9.1s; 100 high_value / 338 velocity / 304 geo_hop
  alerts; 2017 dedup checks, 642 approved, 1375 duplicates correctly
  suppressed, zero `(rule,user_id,window_start)` approved twice. **Verify the
  real Kafka path (`docker compose up` + `spark-submit --packages ...
  spark/job.py`) on a Docker-capable machine before trusting `read_transactions`
  end to end** - it has never actually connected to a broker.
- Phase 2 left one PLAN §14 runtime assertion unimplemented: "if a query
  reports `numInputRows == 0` for 60 consecutive seconds, log a WARNING."
  Doing this properly needs either polling `query.lastProgress` in a loop or
  a `StreamingQueryListener` - PLAN §17 explicitly cuts `StreamingQueryListener`
  as a deliberate scope cut, and nothing in §11's test spec exercises this
  assertion, so it was deferred rather than added speculatively. Revisit in
  Phase 5 if the smoke test needs it.

- Phase 4a: `api/broadcast.py::Broadcaster.publish()` is a plain synchronous
  function, not a coroutine - it never `await`s, so it structurally cannot
  block (Trap E's requirement is satisfied for free, not just tested for).
  On `asyncio.QueueFull` it `get_nowait()`s the oldest item (incrementing
  `dropped`) then `put_nowait()`s the new one; since `publish()` only ever
  runs on the event-loop thread (called directly, or scheduled onto it via
  `loop.call_soon_threadsafe` from `api/consumer.py`'s Kafka thread), there is
  no concurrent access to a single subscriber's queue to race against.
- Phase 4a: `api/consumer.py::AlertConsumer` takes an injectable
  `consumer_factory: Callable[[dict], Any]` (default
  `lambda conf: Consumer(conf)`) purely so tests can substitute
  `tests/fakes.py::FakeKafkaConsumer` - no broker exists in this sandbox
  ([[no_docker_sandbox]]). The built `conf` dict is kept on
  `self.conf` (not just passed to the factory and discarded) specifically so
  `tests/test_consumer.py::test_group_id_is_unique_per_instance` and
  `test_auto_offset_reset_is_latest` can assert on it directly instead of
  reaching into a real `confluent_kafka.Consumer`, which exposes no getter
  for its own config. `tests/fakes.py::FakeKafkaConsumer` gained a `subscribe()`
  no-op method (previously only `poll`/`close`) since `AlertConsumer.__init__`
  unconditionally calls `self._consumer.subscribe([topic])`, matching a real
  `confluent_kafka.Consumer`.
- Phase 4a: `AlertConsumer._buffer` is a `deque(maxlen=500)` and every valid
  alert is added via `appendleft` (never `append`) - `deque(maxlen=n)`
  evicts from the end *opposite* the append call, so `appendleft` gives
  newest-first ordering **and** the automatic bound in one data structure,
  with no separate reverse/sort step.
- Phase 4a: the consumer thread's inner loop wraps both `poll()` and message
  handling in broad `except Exception: continue`/`pass` blocks beyond
  `_handle`'s own specific `(JSONDecodeError, ValueError, TypeError)` catch -
  PLAN §12 Phase 4a's "the thread never dies" is a stronger requirement than
  "invalid JSON is counted", so an unexpected error (e.g. a future bug in
  `_handle` itself, or in the injected `on_alert` callback) must not kill the
  background thread either, even though only malformed-message errors
  increment `invalid_count`.
- Phase 4a: `api/main.py::create_app(config, consumer_factory)` is a factory,
  not a bare module-level `app`, purely for testability - `tests/test_api.py`
  needs a fresh `AlertConsumer`/`Broadcaster` pair with an injected fake per
  test. `api/main.py::app = create_app()` at module scope is what
  `uvicorn api.main:app` actually serves.
- Phase 4a: `GET /` is served via `StaticFiles(directory="web", html=True)`
  **mounted last**, after the three `/healthz`/`/api/alerts`/`/api/stream`
  routes are registered - Starlette matches routes in registration order, so
  the API routes take precedence and the mount only catches `/` and future
  Phase 4b static assets (`app.js`, `app.css`). A placeholder `web/index.html`
  was added now (Phase 4b builds the real dashboard) purely so this mount has
  something to serve and `test_index_served_at_root` isn't testing a 404.
- Phase 4a bug found writing `tests/test_api.py`, not by production code:
  **`fastapi.testclient.TestClient` (and `httpx.AsyncClient` +
  `httpx.ASGITransport` directly) cannot test `/api/stream` at all.**
  `httpx==0.27.2`'s `ASGITransport.handle_async_request` does a bare
  `await self.app(scope, receive, send)` and only constructs a `Response`
  *after* that call returns - there is no background task streaming chunks
  incrementally, so any ASGI route whose body generator doesn't terminate on
  its own (ours never does, by design - Trap E) hangs the test forever,
  confirmed via `faulthandler.dump_traceback_later` showing the test stuck
  inside `ASGITransport.handle_async_request` with the consumer thread alive
  and idle. Fix: `tests/test_api.py::_LiveServer` runs the real app under a
  real `uvicorn.Server` bound to a loopback socket (`port=0`, actual port read
  back via `getsockname()`) in a background thread, and the three streaming
  tests hit it over a real socket with a plain `httpx.Client`, which streams
  incrementally exactly like a real browser tab would. The non-streaming
  routes (`/healthz`, `/api/alerts`, `/`) are unaffected and still use the
  ordinary in-process `TestClient`, which is fine for any request that
  actually completes.
- Phase 4a acceptance demo, same Kafka substitution as
  [[no_docker_sandbox]]/Phase 2's: `make api` was run for real (default
  `consumer_factory`, i.e. an actual `confluent_kafka.Consumer(...)` pointed
  at `localhost:9092` with no broker present) to confirm
  `curl localhost:8000/healthz` → `consumer_alive: true` even while every
  `poll()` call fails to connect - the consumer thread's broad exception
  guard keeps it alive exactly as designed. Since no transactions/Spark/Kafka
  path exists here to actually produce alerts, `curl -N
  localhost:8000/api/stream` was then demonstrated against the same
  `create_app()` wired to a throwaway drip-feed fake consumer (one
  ALERT_SCHEMA-shaped JSON alert every ~0.7s, ~identical to
  `tests/test_api.py`'s `_DelayedFakeConsumer` but continuous) standing in
  for a live producer → Spark → `fraud-alerts` pipeline: frames streamed
  continuously, `/healthz`'s `alerts_seen` climbed live, `dropped` stayed 0.
  **The real Kafka path for the API (a live `confluent_kafka.Consumer`
  actually receiving `fraud-alerts` produced by a real `spark/job.py` run)
  has never been exercised** - verify `make up && make topics`, a real
  `spark-submit ... spark/job.py`, and `make api` together on a
  Docker-capable machine before trusting `api/consumer.py`'s Kafka options
  end to end.

- Phase 4b: the dashboard is three static files (`web/index.html`, `web/app.css`,
  `web/app.js`) served by the Phase 4a `StaticFiles` mount - no build step, no
  npm, no CDN, no framework, no Google Fonts, works offline. Exactly three panels
  (Vitals / Feed / Metrics), per PLAN §12; no map, settings page, or dark-mode
  toggle. The palette is §12's `:root` block verbatim plus one added grey
  `--bar: #484f58` for the non-severity SVG bars. Design law enforced: severity
  is the only saturated colour (feed swatches + the by-severity bars use
  `--sev-*`; the by-rule bars are grey `--bar`; the pill's green dot is the one
  connection-health accent, not a severity); every number carries
  `font-variant-numeric: tabular-nums` + `--mono`; radius <= 6px, no shadows/
  gradients; the *only* animation is a 150ms `fadein` on newly-prepended feed
  rows (snapshot rows don't get it). Relative-time cells refresh via a 1s
  `setInterval` updating `textContent` - text updates, not a CSS animation, so
  the "nothing else animates" rule holds.
- Phase 4b: the tau slider recomputes precision/recall/confusion **entirely in
  `app.js` over the buffered alerts, on `input`** - it never calls the API and
  never restarts Spark (PLAN §12). Interpretation: the confusion matrix is over
  alerts that carry a non-null `p_fraud` (i.e. `ml_score`/R4 alerts only); rule
  alerts R1/R2/R3 have `p_fraud: null` and are excluded, since you cannot
  threshold a null score - a "N scored alerts" note makes this explicit in the
  UI. Prediction = `p_fraud >= tau`, truth = `is_fraud`. This is the only reading
  where the slider does anything. Verified by running the real `renderMetrics()`
  under a stubbed DOM in node (see `docs/dashboard_demo.md` §4): τ=0.65 →
  TP2/FP1/FN1/TN2, τ=0.35 → recall 100%, τ=0.95 → precision "—", recall 0%.
- Phase 4b: `EventSource` handles its own reconnection - `app.js` has **no**
  custom reconnect loop, only `es.onopen`/`es.onerror` driving the connection
  pill (`live` / `reconnecting`). Snapshot (`/api/alerts?limit=200`) and stream
  (`/api/stream`) alerts are deduped by `alert_id` via a `Set` so the
  snapshot↔stream handoff can't double-render an alert. Client buffer capped at
  500 (feed is 2 `<tr>`s per alert - a main row + a hidden raw-JSON detail row
  toggled on click - so the trim guard is `childElementCount > CAP * 2`).
- Phase 4b: the browser reconnect demo (PLAN §12 accept criterion) was **not run
  live** - the Claude-in-Chrome extension wasn't connected this session, and this
  sandbox has no Docker/Kafka/Spark to run a real `spark/job.py` regardless.
  Important subtlety documented in `docs/dashboard_demo.md`: killing `spark/job.py`
  does NOT drop the browser↔API SSE connection (the API stays up; alerts just
  pause/resume, pill stays `live`) - the `EventSource` reconnect path is only
  exercised by restarting the **SSE server** (uvicorn/`api` container). Headless
  verification done instead: `node --check web/app.js`, the node-stubbed-DOM
  metrics run above, and a live `create_app()` (real, only the Kafka consumer
  faked by `scripts/demo_dashboard.py`'s synthetic drip-feed) serving `/`,
  `/app.css`, `/app.js`, `/api/alerts`, `/healthz`, and a continuous
  `/api/stream`. `docs/dashboard_demo.md` carries the extension-install steps and
  a ready-to-paste prompt to run the full GIF-recorded browser demo (and produce
  the Phase 5 `docs/dashboard.png` screenshot) once the extension is set up.

- Phase 5: the two remaining PLAN §14 runtime assertions were implemented.
  `spark/enrich.py::_assert_no_label_leakage` greps `enrich()`'s output
  columns against `/fraud|label|target/i`, excluding `LABEL_FIELD` itself
  (the one expected exception - ground truth carried through unchanged, not
  derived here) - pure, unit-tested directly with no SparkSession needed.
  The `numInputRows == 0` for 60s warning got its own module,
  `spark/health.py::StreamHealthMonitor` - deliberately not in `job.py`
  itself (already ~30% over its PLAN §10 line budget) and deliberately not a
  `StreamingQueryListener` (cut in PLAN §17). It's a plain injectable-clock
  class (`record(name, num_input_rows)`) so the warn/reset/re-warn logic is
  tested with a fake clock and no real thread or timing; `job.py::main()`
  wires a thin `poll_forever()` adapter reading real `query.lastProgress` on
  a daemon thread, which is not itself unit-tested (needs a live query,
  same rationale as `read_transactions`/`main` per PLAN §3.3).
- Phase 5: `Dockerfile.spark` is one image shared by `spark-job`, `producer`,
  and `api` (all three only need this project's pip requirements). It
  pre-warms the Ivy cache for the `spark-sql-kafka` connector at *build*
  time by running a trivial `SparkSession` under `spark-submit --packages
  ...` once - the package string is derived from the installed pyspark
  version inside the Dockerfile itself (`python -c 'import pyspark; ...'`),
  independently of the Makefile's identical derivation for the host path, so
  the two can never drift apart without both needing to change. `JAVA_HOME`
  is symlinked to whatever `openjdk-17-jre-headless` actually installs to
  (`/opt/java`) rather than hardcoded, since that path differs by
  amd64/arm64 under Debian.
- Phase 5: `docker-compose.yml` gained `kafka-init` (a one-shot container
  that creates both topics with the brief's exact partition counts then
  exits, via `condition: service_completed_successfully`), `spark-job`,
  `producer`, and `api`. `spark-job`'s command trains the model on first
  boot only if `ml/artifacts/model.txt` is missing (that directory is a bind
  mount, so training happens once across `docker compose down`/`up` cycles,
  not on every restart). Both `spark-job`'s and `kafka-init`'s multi-line
  commands are written as an explicit YAML list (`[/bin/bash, -c, |
  <script>]`) rather than a folded `>` string specifically to avoid relying
  on Compose's shlex-style splitting of a quoted string-form `command:` -
  the list form is passed to the process literally, no shell-quoting
  ambiguity to reason about.
- Phase 5: `scripts/smoke_test.py` implements all 18 PLAN §13 steps, but
  interprets step 1 ("docker compose up") as bringing up **only the `kafka`
  service** via Compose - `spark/job.py`, `producer.py`, and `api.main` are
  then run directly on the host (via `spark-submit`/`python -m`, pointed at
  Kafka's host-mapped `localhost:9092` listener), not through the
  `spark-job`/`producer`/`api` containers. This gives the smoke test direct
  process control (capturing spark-job's stdout for the "3 queries active"
  wait and the "no ERROR lines" assertion, precise start/stop ordering) while
  still exercising a real containerized Kafka broker and the real production
  code paths - the same "real Kafka path" pattern the Phase 2/4a acceptance
  demos used, just formalized into a script. The full `docker compose up`
  (all five services in containers) is the separate, simpler path documented
  in the README as the headline command; `make smoke` is a maintainer's
  pre-demo check, not the end-user path.
- Phase 5: PLAN §13 step 9 ("no duplicate (rule, user_id, window_start)")
  can't check `window_start` directly - it's Trap B's dedup key, computed
  driver-side in `spark/job.py`, and deliberately never reaches
  `ALERT_SCHEMA`/a sink (see the Phase 2 decisions above). `smoke_test.py`
  recomputes it from each alert's `event_time` instead: Spark's tumbling
  windows align to the epoch with no offset, so `velocity`'s window_start is
  `event_time` floored to the minute and `geo_hop`'s is floored to the
  nearest 5-minute epoch boundary - both derivable with no access to Spark
  internals. High_value/ml_score are stateless (Trap B doesn't apply to
  them) and are excluded from this check.
- Phase 5 SEED search (PLAN §13 step 5): `generate(42, 5000, ...)` - the seed
  already used everywhere else in this project - already produces 31 whale,
  6 burst, and 28 traveller archetypes within the first 5000 records (verified
  by classifying runs of consecutive same-user `is_fraud=1` records by
  length, the same convention `test_simulator.py` uses). No search was
  actually needed; the archetype injection probabilities (whale 0.4%, burst
  0.15%, traveller 0.3% per step) make zero occurrences of any one archetype
  in 5000 draws astronomically unlikely for *any* seed. `PLAN.md` §13 records
  `SEED = 42` with these counts.
- Phase 5 local acceptance run (same no-Docker substitution as Phase 2/4a -
  see [[no_docker_sandbox]] - real `producer/simulator.py` +
  `producer/producer.py::send` + `spark/job.py::build_queries` + the real
  console/parquet sinks, Kafka swapped for a file-source directory fed by an
  in-memory buffered "producer" standing in for the topic): 5000 records at
  the brief's rate ceiling (100/s, ~50s), then ~90s of `processAllAvailable()`
  polling. Captured 1302 alerts total - 192 `high_value`, 572 `velocity`, 504
  `geo_hop`, 34 `ml_score` - all four rules fired, zero `ERROR` lines in the
  captured console-sink log, 92 parquet part-files written under
  `data/out/rule=*`. `docs/sample_alerts.jsonl` (first 100),
  `docs/spark_console.log` (the full captured run), and `docs/dashboard.png`
  (a `create_app()` instance with only the Kafka consumer swapped for a
  replay of these same 1302 real captured alerts, screenshotted via a
  headless `google-chrome --screenshot` CLI call - the claude-in-chrome
  extension was not connected this session, same as the Phase 4b note below)
  are all real output from this run, not synthetic placeholders. Note:
  `geo_hop`'s 504 alerts (mostly `is_fraud=0`) are inflated relative to a
  naturally-paced deployment - running 5000 records' worth of *event time*
  (each stamped with real `now()`) inside ~50 real seconds packs far more
  transactions per real 5-minute window than the archetype design assumed,
  so ordinary users occasionally rack up several incidental country changes
  by chance within one window. This is a demo-speed artifact of compressing
  the run, not a rule defect - R3 is doing exactly what PLAN §7 specifies.
- Phase 5, `--screenshot` via headless Chrome CLI: `--virtual-time-budget`
  combined with a page holding an open `EventSource`/SSE connection
  (`/api/stream`) hangs indefinitely rather than forcing the screenshot after
  the budget - the flag appears to wait for network idle before considering
  virtual time "spent," which a permanently-open SSE stream never reaches.
  Dropping `--virtual-time-budget` entirely (plain `--headless=new
  --screenshot=... --window-size=... <url>`, with a dedicated
  `--user-data-dir` so it doesn't collide with the user's real Chrome
  profile/singleton lock) captures immediately once the page has rendered.

## Known issues

- This dev sandbox has no `docker`/`docker compose`/Colima/Podman installed, so
  `make up` and `make topics` could not be exercised end-to-end here — `make up`
  fails immediately with `docker: No such file or directory`. The compose file and
  Makefile targets are written and reviewed against the standard KRaft
  single-node pattern for `apache/kafka:3.7.0`, but neither has run to green.
  Verify on a machine with Docker before trusting `make up && make topics`.
- Phase 5, same root cause: `docker compose up` (the README's headline
  command, bringing up `kafka`/`kafka-init`/`spark-job`/`producer`/`api`
  together) and `make smoke` (`scripts/smoke_test.py`, PLAN §13's 18
  assertions) have **never been run** in this sandbox - both need Docker.
  `Dockerfile.spark`'s Ivy pre-warm step and the five-service
  `docker-compose.yml` were reviewed line-by-line and validated for YAML
  correctness (`yaml.safe_load` round-tripped every service's `command:`
  string, including the `$$`-escaping needed for the compose-variable
  interpolation pass), but neither has actually built or run a container.
  The 18 smoke-test assertions were instead exercised individually against a
  real local Spark pipeline (see the Phase 5 local acceptance run entry
  above) - real rules, real dedup, real parquet, real console log, real
  dashboard - with only the Kafka broker and the `docker compose` boundary
  itself substituted. **Verify `docker compose up` and `make smoke` on a
  Docker-capable machine before trusting them** - particularly whether the
  Ivy cache pre-warmed at build time actually avoids a jar download at
  `spark-job` container start, and whether `kafka-init`'s
  `service_completed_successfully` dependency ordering behaves as expected
  on the grader's Compose version.
- `make test`/`make test-unit`/`make cov` were verified green (14 passed, 96%
  coverage) using a project-local `.venv` (`python3 -m venv .venv && .venv/bin/pip
  install -r requirements.txt`), not the bare `python3` on PATH, which has no
  packages installed. The Makefile's `PYTHON` var defaults to plain `python3`;
  override with `PYTHON=.venv/bin/python3` or activate the venv first.
- As of Phase 2: `make test` is green (60 passed) and `make cov` passes the
  85% floor at 92% total, using the same `.venv`. `spark/job.py::read_transactions`
  and `main()` are genuinely untested here - the `spark-sql-kafka` connector
  jar (`SPARK_KAFKA_PACKAGE` in the Makefile) is never fetched by plain
  pytest, only by `spark-submit --packages ...`, and there's no broker to
  connect to regardless. The Phase 2 acceptance run (see Decisions log) used
  a file-source + in-memory-list substitution for Kafka, the same limitation
  noted in [[no_docker_sandbox]]. **The real Kafka path has never actually
  run** - verify `docker compose up` + `spark-submit --packages
  $(SPARK_KAFKA_PACKAGE) spark/job.py` end to end on a Docker-capable machine
  before trusting it, especially `read_transactions`'s Kafka options and the
  Kafka sink in `spark/sinks.py::make_kafka_sink`.
- `spark/rules.py` implements `collect_set` inside the R3 streaming
  aggregation exactly as PLAN §7 specifies (the "known risk" callout there
  says to drop it if it raises) - it did not raise on pyspark 3.5.8 in this
  sandbox; no fallback to `approx_count_distinct`-only was needed.
- As of Phase 3: `make train`/`make test` need two extra env vars beyond the
  `.venv` requirement above - `JAVA_HOME` pointed at a JDK 17 (this sandbox's
  default JDK 22 breaks the pandas_udf's Arrow transfer) and
  `PYSPARK_PYTHON` pointed at `.venv/bin/python3` (Spark's pandas_udf worker
  subprocess otherwise resolves plain `python3`, which lacks `lightgbm`).
  See the Decisions log's "Phase 3 environment" entry for the full
  explanation and exact commands. `make train` (200k records, temporal
  80/20 split) and `make test` (68 passed, 93.55% coverage) were both
  verified green with these set.
- PLAN §14's "warn if a query goes 60s with `numInputRows == 0`" runtime
  assertion is not implemented (see Decisions log) - deliberately deferred,
  not an oversight.
- Phase 3: `make train` result at seed 42, 200k records, temporal 80/20 split
  (160k train / 40k valid): **PR-AUC (average_precision_score) = 0.2088**,
  ROC-AUC = 0.6156 (footnote only), tau = 0.6505 (argmax-F1 on the validation
  PR curve). This is well below PLAN §9's estimate of 0.85-0.95 and below
  §12's 0.80 acceptance floor - `ml/train.py::main()` prints an explicit NOTE
  rather than asserting a floor, and does not fail the build. Root cause,
  verified by grouping validation records by run-length: of the 4611 fraud
  records in the full 200k-record generation, only the `whale` archetype
  (18.4% of fraud) is an amount anomaly against the user's own profile
  (`amount_z`) - `burst` and `traveller` (81.6% of fraud, see PLAN §6) draw
  amounts from that same user's ordinary log-normal profile by construction,
  so they are statistically identical to a normal transaction in every one of
  the nine FEATURE_ORDER columns. A row-level classifier with no
  velocity/geo context (deliberately excluded from FEATURE_ORDER - that
  context is R2/R3's job, not R4's) cannot separate them from noise; a
  best-case back-of-envelope bound (perfect ranking of whale records, random
  ranking of the rest) puts the achievable PR-AUC ceiling at ~0.20, which
  matches what training actually reaches. This is a property of the frozen
  Phase 1 archetype design plus the frozen FEATURE_ORDER contract, not a
  training bug - R1-R3 alone already satisfy the brief's "3+ rules"
  requirement in full (PLAN §12 Phase 2), and R4 is explicitly upside.
  `scale_pos_weight` was tried and measurably *hurt* ranking (AP 0.14 vs
  0.19-0.21 without it in tuning runs) since it only affects the loss's class
  balance, not ranking quality, and `tau` is chosen directly from the
  validation PR curve regardless of calibration - dropped it. LightGBM's
  built-in `"average_precision"` eval metric (not just `"auc"`) is used for
  early stopping so the stopping criterion matches what is reported.
- Phase 3: R4 (`ml_score`) is split the same way R1-R3 are: `spark/rules.py::
  ml_score_rule(df, tau)` is a pure filter+shape - it expects `p_fraud`
  already present on `df` and only does `filter(p_fraud > tau)` plus the
  ALERT_SCHEMA `select`. Feature assembly (`add_ml_features`) and scoring
  (`add_p_fraud`, wrapping `score_udf`) live in `spark/scoring.py`, since both
  need FEATURE_ORDER/the booster and neither belongs in a "pure filter" rule.
  This means test_rules.py's R4 cases construct a `p_fraud` column directly
  and never need a real trained model - matching how R1's tests use
  `amount_eur` directly without concern for how `enrich()` computed it. A
  stale docstring comment in `rules.py` from Phase 2 ("R4 lives in
  spark/scoring.py") suggested the opposite split; superseded now that Phase
  3 actually implements R4 - see the current docstring.
- Phase 3: `spark/scoring.py::add_ml_features` joins the enriched stream
  against `user_profiles` (`amt_mean`/`amt_std`/`tx_count` per `user_id`,
  built from the TRAIN split only) via the same `create_map`-lookup idiom
  `enrich.py::_fx_map` already uses for FX rates - not
  `F.array_position(array, col)`, whose needle argument must be a Python
  scalar, not a per-row `Column` (confirmed by hitting
  `PySparkTypeError: [NOT_ITERABLE]` when tried). A user absent from
  `user_profiles` (`is_new_user=1`) gets `amount_z` centered on its own
  amount with unit spread (z=0) rather than a division by a nonexistent
  profile; `ml/train.py::build_features` applies the *identical* fallback in
  pandas so train and serve never disagree on an unseen user. `ml/train.py`
  also reimplements Spark's `dayofweek()` convention by hand
  (`_spark_dayofweek`: Sunday=1..Saturday=7) since pandas' native
  `.dt.dayofweek` is Monday=0..Sunday=6 - a silent mismatch here would have
  trained the model on a feature it never sees identically at serve time.
- Phase 3: `spark/job.py::build_queries` now unions R1 and R4 into one Q1
  DataFrame (`high_value_rule(enriched).unionByName(ml_score_rule(...))`)
  before the single `row_batch`/checkpoint/query - matching PLAN §4's "R4
  must never get its own query or checkpoint." It also calls
  `scoring._load()` once at the top of `build_queries` (before any query
  starts) to satisfy PLAN §14's "job.py startup: `feature_name()` ==
  `FEATURE_ORDER`, or die" - a mismatched retrained model now fails before a
  single batch runs, not silently inside the first `foreachBatch` call.
- Phase 3 environment: getting `spark/scoring.py`'s `pandas_udf` to actually
  execute in this sandbox required three fixes beyond `pip install`, none of
  which are code changes to the production path:
  1. **JDK version.** This machine's default `java` is JDK 22.
     PySpark 3.5.8's bundled Arrow (used for the pandas_udf's Arrow IPC
     transfer to the Python worker) fails on it with
     `UnsupportedOperationException: sun.misc.Unsafe or
     java.nio.DirectByteBuffer.<init>(long, int) not available`, even with
     every plausible `--add-opens` JVM flag tried. **Every Phase 0-2 test
     passed fine on JDK 22** because nothing before Phase 3 used a
     pandas_udf/Arrow codepath. Fix: run with
     `JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home`
     (JDK 17 was already installed on this machine; confirmed working)
     prepended onto `PATH`. **Verify which JDK the real Docker image
     (Dockerfile.spark) uses before trusting this on another machine** - if
     it's JDK 17/11, this is a non-issue there.
  2. **Worker interpreter.** Spark's pandas_udf executes in a *separate*
     Python worker subprocess, which otherwise resolves to plain `python3`
     on `PATH` (no `lightgbm`/`pandas` installed there), not the project's
     `.venv`. Fix: `PYSPARK_PYTHON=<abs path to>/.venv/bin/python3` must be
     exported before running `make train`/`make test`/`spark-submit`.
  3. **`distutils`.** PySpark 3.5.8's `pandas_udf` machinery imports
     `distutils.version.LooseVersion` at decoration time, and `distutils`
     was removed from the stdlib in Python 3.12 (this project's venv). Added
     `setuptools<81` to `requirements.txt` - its install hook restores
     `distutils` as an importable shim automatically (no explicit `import
     setuptools` needed anywhere in this project's own code).
  4. `spark/scoring.py::score_udf` is decorated `@pandas_udf(DoubleType())`,
     not `@pandas_udf("double")` - the string form is parsed via Spark's SQL
     parser, which requires an active `SparkContext`, so it would break
     `import spark.scoring` (and therefore plain `pytest` collection) with
     no session running yet. `DoubleType()` sidesteps the parser entirely.
- As of Phase 4a: `make test` is green (88 passed, ~67s) and `make cov`
  passes the 85% floor at 93.81% total (`api/main.py` 100%, `api/broadcast.py`
  92%, `api/consumer.py` 92%), using the same `.venv` plus the JDK
  17/`PYSPARK_PYTHON` env vars from the Phase 3 environment note (still
  needed - Phase 2/3's Spark tests are in the same `make test` run).
  `api/consumer.py`'s uncovered lines are the two `except Exception` safety
  nets in `_run()` (deliberately defensive code paths that no test forces)
  and `api/broadcast.py`'s uncovered lines are the `QueueEmpty` guard inside
  `publish()`'s `QueueFull` branch (dead in practice - a full queue is never
  simultaneously empty - kept only because emptying and re-filling a queue
  isn't provably atomic across asyncio implementations).
- Phase 3: `spark/scoring.py` is ~100 lines against PLAN §10's ~40-line
  budget (over the "more than 50%" reconsider-the-design line). Deliberate,
  not a miss: the budget only accounted for `_load`/`score_frame`/the
  pandas_udf wrapper (PLAN §9's literal code sketch); it didn't anticipate
  that *something* still has to turn an enriched transaction row into the
  nine FEATURE_ORDER columns (`add_ml_features`, joining `user_profiles`)
  before scoring can run at all. Considered splitting that half into its own
  `spark/features.py`; kept it in one file instead since both halves are
  short, share `FEATURE_ORDER`/`user_profiles`, and only exist to serve R4 -
  a second file would be two ~50-line halves of one concern, not two
  concerns.

### Grader review 2026-07-10

Adversarial pass against PLAN §16 / the brief. One HIGH found and fixed; the
rest recorded here.

- **[FIXED - was HIGH] Kafka sink dropped null fields, silently discarding
  every rule-based alert at the dashboard.** `spark/sinks.py::make_kafka_sink`
  serialized alerts with `F.to_json(struct)`, whose default
  `ignoreNullFields=true` **omits** null-valued keys. `p_fraud` is null for
  R1/R2/R3 (the vast majority of alerts) and `country` is null for unknown
  cities, so those keys were absent from the `fraud-alerts` JSON - and
  `api/consumer.py::_handle` rejects any message where
  `_ALERT_FIELDS.issubset(payload.keys())` is false, marking every such alert
  `invalid` and dropping it. The dashboard would show only `ml_score` alerts
  (rare) and nothing else, violating the §5 alert contract (which keeps
  `p_fraud` present-with-null). Never caught because every test injects
  fully-formed dicts via `json.dumps({... "p_fraud": None ...})` (keys always
  present) instead of the real `to_json` path - exactly the "real Kafka path
  never exercised" gap noted above. Fix: `to_json(..., {"ignoreNullFields":
  "false"})`. Regression test `tests/test_sinks.py::test_kafka_json_keeps_null_fields`
  asserts all 15 ALERT_SCHEMA keys survive with null `p_fraud`/`country`.
  Verified: default drops both keys (13/15); fixed sink keeps all 15.
- **[LOW - PLAN §16 check #16] Three files exceed their §10 line budget by
  more than 50%:** `spark/rules.py` 170 vs ~90 (+89%), `spark/scoring.py` 101
  vs ~40 (+153%, already documented above), `api/consumer.py` 106 vs ~70
  (+51%). Each is internally cohesive (rules.py is four parallel rule
  transforms sharing helpers; consumer.py is one thread class); no split was
  made, but §10 says exceeding by >50% means "stop and reconsider," so it is
  recorded rather than waved off.
- **[LOW] The graded core (R1-R3) hard-depends on the optional Phase-3 model.**
  `spark/job.py::build_queries` unconditionally calls `scoring._load()` and
  reads `user_profiles.parquet` before starting any query, so the job cannot
  run at all if `ml/artifacts/` is missing - even though PLAN §18 frames the
  model as the first thing to cut. In Docker `spark-job` trains on first boot
  so this never bites there, but the "20/20 without the model" claim (PLAN
  §12) is not literally runnable from this entrypoint without the artifacts.
- **[LOW] `api/consumer.py::_handle` requires every ALERT_SCHEMA field
  present, including the nullable `p_fraud`/`country`.** The sink fix above
  makes all keys present, so this no longer drops anything, but the validation
  is stricter than the schema's own nullability and would re-break if any
  producer emitted a legitimately sparse record. Defense-in-depth: consider
  validating only the non-nullable fields.
- **[INFO - PLAN §16 check #5] `docker compose up` and `make smoke` remain
  unrun** (no Docker in this sandbox) - see the two Known-issues entries
  above. Every other §16 check passes: seven fields spelled correctly in an
  explicit `from_json`/StructType (#1); topics exactly `transactions`/
  `fraud-alerts` (#2); all three sinks reached via one `write_alerts` (#3,
  after the fix); four rules, all firing in the acceptance run (#4); `is_fraud`
  reaches only the alert *output* and the training *label*, never a feature,
  rule condition, or model input (#6, grep-verified); no windowed query on
  `append` (#7); both windowed paths go through `AlertDeduper` (#8); three
  distinct checkpoints (#9); no `--reload` (#10); `publish()` never blocks
  (#11); persist-before-write / unpersist-in-finally present (#12); no network
  imports in `simulator.py` (#13); README PR-AUC 0.21 matches train's 0.2088
  (#14); no magic strings in any `.py` outside `common/`+`tests/` (#15).