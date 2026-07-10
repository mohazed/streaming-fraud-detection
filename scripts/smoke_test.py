"""Not pytest - PLAN.md §13. Run this before you present:

    make smoke

Brings up the real Kafka broker via `docker compose`, then drives the real
`producer/producer.py` and `spark/job.py` (via `spark-submit`, on the host,
against Kafka's host-mapped PLAINTEXT listener - the same pattern
CODEBASE_NOTES calls "the real Kafka path") and the real `api/main.py`
against a live alerts topic. All 18 assertions from PLAN.md §13.
Tears the stack down on the way out, whether it passed or failed, and prints
exactly which assertion failed if it did.

Requires: Docker running, and this project's venv active with
JAVA_HOME pointed at a JDK 17 (see CODEBASE_NOTES "Phase 3 environment") -
`make smoke` documents both.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.config import CONFIG  # noqa: E402
from common.contracts import ALERT_SCHEMA, RULE_NAMES  # noqa: E402

SEED = 42  # produces >=1 whale/burst/traveller within 5000 records - see PLAN.md §13
RATE = 50
LIMIT = 5000
CONSUME_SECONDS = 90
API_PORT = 8000

# Kafka's host-mapped PLAINTEXT listener (docker-compose.yml), reached from
# host-run subprocesses (spark-submit/producer/api) - not CONFIG's own default,
# which containers reach via the DOCKER listener instead. Same value as that
# default in this project's compose file, but kept as its own name since the
# two are conceptually different listeners that happen to coincide.
HOST_BOOTSTRAP = CONFIG.kafka_bootstrap_servers

SPARK_LOG_PATH = ROOT / "docs" / "spark_console.log"
_ALERT_FIELDS = ALERT_SCHEMA.fieldNames()


class SmokeFailure(Exception):
    pass


def _step(msg: str) -> None:
    print(f"[smoke] {msg}", flush=True)


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    _step(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, **kwargs)


# ---- 1-2: bring up Kafka, create topics -----------------------------------

def compose_up_kafka() -> None:
    _run(["docker", "compose", "up", "-d", "kafka"], cwd=ROOT)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", HOST_BOOTSTRAP],
            cwd=ROOT, capture_output=True,
        )
        if result.returncode == 0:
            return
        time.sleep(2)
    raise SmokeFailure("kafka did not become healthy within 60s")


def create_topics() -> None:
    _run(["docker", "compose", "exec", "-T", "kafka",
          "/opt/kafka/bin/kafka-topics.sh", "--create", "--if-not-exists",
          "--topic", CONFIG.topic_transactions, "--bootstrap-server", HOST_BOOTSTRAP,
          "--partitions", "3", "--replication-factor", "1"], cwd=ROOT)
    _run(["docker", "compose", "exec", "-T", "kafka",
          "/opt/kafka/bin/kafka-topics.sh", "--create", "--if-not-exists",
          "--topic", CONFIG.topic_alerts, "--bootstrap-server", HOST_BOOTSTRAP,
          "--partitions", "1", "--replication-factor", "1"], cwd=ROOT)
    listed = subprocess.run(
        ["docker", "compose", "exec", "-T", "kafka",
         "/opt/kafka/bin/kafka-topics.sh", "--list", "--bootstrap-server", HOST_BOOTSTRAP],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout
    for topic in (CONFIG.topic_transactions, CONFIG.topic_alerts):
        if topic not in listed:
            raise SmokeFailure(f"topic {topic!r} not listed after creation: {listed!r}")


# ---- 3: train if missing ----------------------------------------------------

def ensure_model_trained() -> None:
    if Path(CONFIG.model_path).exists():
        _step(f"model already present at {CONFIG.model_path}, skipping train")
        return
    _run([sys.executable, "-m", "ml.train"], cwd=ROOT)


# ---- 4: start spark/job.py, wait for "3 queries active" --------------------

def _host_env() -> dict:
    env = dict(os.environ)
    env["KAFKA_BOOTSTRAP_SERVERS"] = HOST_BOOTSTRAP
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    return env


def _spark_kafka_package() -> str:
    import pyspark
    return f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"


def start_spark_job(log_path: Path) -> subprocess.Popen:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        ["spark-submit", "--packages", _spark_kafka_package(), str(ROOT / "spark" / "job.py")],
        cwd=ROOT, env=_host_env(), stdout=log_file, stderr=subprocess.STDOUT,
    )
    proc._log_file = log_file  # closed in teardown
    return proc


def wait_for_queries_active(log_path: Path, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if log_path.exists() and "3 queries active" in log_path.read_text(errors="ignore"):
            return
        time.sleep(2)
    raise SmokeFailure(f"spark/job.py never logged '3 queries active' within {timeout}s")


# ---- 5: start the producer --------------------------------------------------

def start_producer() -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "producer.producer",
         "--rate", str(RATE), "--seed", str(SEED), "--limit", str(LIMIT)],
        cwd=ROOT, env=_host_env(),
    )


# ---- 6: consume fraud-alerts for 90s ---------------------------------------

def consume_alerts(duration_s: float) -> list[dict]:
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": HOST_BOOTSTRAP,
        "group.id": f"smoke-test-{int(time.time())}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([CONFIG.topic_alerts])
    alerts: list[dict] = []
    deadline = time.monotonic() + duration_s
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                alerts.append(json.loads(msg.value()))
            except json.JSONDecodeError:
                continue
    finally:
        consumer.close()
    return alerts


# ---- 7-12: assertions over the consumed alerts + parquet + spark log -------

def assert_every_rule_fired(alerts: list[dict]) -> None:
    seen = {a.get("rule") for a in alerts}
    missing = set(RULE_NAMES) - seen
    if missing:
        raise SmokeFailure(f"no alert for rule(s): {sorted(missing)} (got {sorted(seen)})")


def assert_schema_conformance(alerts: list[dict]) -> None:
    for a in alerts:
        missing = set(_ALERT_FIELDS) - a.keys()
        if missing:
            raise SmokeFailure(f"alert missing ALERT_SCHEMA field(s) {missing}: {a}")


def _window_start(rule: str, event_time: datetime) -> datetime | None:
    # window_start itself never reaches a sink (it's driver-side dedup state,
    # PLAN.md §8 Trap B) - tumbling windows align to the epoch with no offset,
    # so it can be recomputed here from event_time for the two windowed rules.
    if rule == "velocity":
        return event_time.replace(second=0, microsecond=0)
    if rule == "geo_hop":
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        minutes = (event_time - epoch) // timedelta(minutes=5)
        return epoch + minutes * timedelta(minutes=5)
    return None  # R1/R4 are stateless - Trap B does not apply


def assert_no_duplicate_windowed_alerts(alerts: list[dict]) -> None:
    seen: set[tuple] = set()
    for a in alerts:
        event_time = datetime.strptime(a["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        win = _window_start(a["rule"], event_time)
        if win is None:
            continue
        key = (a["rule"], a["user_id"], win)
        if key in seen:
            raise SmokeFailure(f"duplicate windowed alert for key {key} (Trap B)")
        seen.add(key)


def assert_p95_latency_under(alerts: list[dict], max_seconds: float) -> None:
    deltas = []
    for a in alerts:
        event_time = datetime.strptime(a["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        alert_time = datetime.strptime(a["alert_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        deltas.append((alert_time - event_time).total_seconds())
    deltas.sort()
    p95 = deltas[int(0.95 * (len(deltas) - 1))]
    if p95 >= max_seconds:
        raise SmokeFailure(f"p95(alert_time - event_time) = {p95:.1f}s >= {max_seconds}s")


def assert_parquet_written() -> None:
    files = list(Path(CONFIG.output_root).rglob("*.parquet"))
    if not files:
        raise SmokeFailure(f"no parquet files under {CONFIG.output_root}")


def assert_no_error_lines(log_path: Path) -> None:
    text = log_path.read_text(errors="ignore")
    error_lines = [ln for ln in text.splitlines() if "ERROR" in ln]
    if error_lines:
        raise SmokeFailure(f"spark log contains ERROR line(s): {error_lines[:5]}")


# ---- 13-17: the API layer ---------------------------------------------------

def start_api() -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "api.main:app",
         "--host", "127.0.0.1", "--port", str(API_PORT)],
        cwd=ROOT, env=_host_env(),
    )


def _get(path: str, timeout: float = 5.0) -> tuple[int, bytes]:
    req = urllib.request.Request(f"http://127.0.0.1:{API_PORT}{path}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read()


def wait_for_consumer_alive(timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, body = _get("/healthz")
            if status == 200 and json.loads(body).get("consumer_alive"):
                return
        except (URLError, ConnectionError):
            pass
        time.sleep(1)
    raise SmokeFailure(f"/healthz never reported consumer_alive within {timeout}s")


def assert_alerts_snapshot() -> None:
    status, body = _get("/api/alerts?limit=50")
    if status != 200:
        raise SmokeFailure(f"GET /api/alerts returned {status}")
    alerts = json.loads(body)
    if not alerts:
        raise SmokeFailure("GET /api/alerts?limit=50 returned zero alerts")
    times = [a["alert_time"] for a in alerts]
    if times != sorted(times, reverse=True):
        raise SmokeFailure("GET /api/alerts is not newest-first")


def assert_stream_yields_frame(timeout: float = 30.0) -> None:
    req = urllib.request.Request(f"http://127.0.0.1:{API_PORT}/api/stream")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = resp.readline()
            if not line:
                continue
            if line.startswith(b"data: "):
                json.loads(line[len(b"data: "):])  # must be valid JSON
                return
    raise SmokeFailure(f"GET /api/stream yielded no frame within {timeout}s")


def assert_dropped_is_zero() -> None:
    status, body = _get("/healthz")
    dropped = json.loads(body)["dropped"]
    if dropped != 0:
        raise SmokeFailure(f"/healthz reports dropped={dropped}, expected 0 (Trap E)")


def assert_index_served() -> None:
    status, body = _get("/")
    if status != 200:
        raise SmokeFailure(f"GET / returned {status}")
    if b'id="feed"' not in body:
        raise SmokeFailure('GET / body does not contain id="feed"')


# ---- 18: teardown ------------------------------------------------------------

def teardown(procs: list[subprocess.Popen]) -> None:
    for proc in procs:
        if proc is None or proc.poll() is not None:
            continue
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        log_file = getattr(proc, "_log_file", None)
        if log_file is not None:
            log_file.close()
    subprocess.run(["docker", "compose", "down", "-v"], cwd=ROOT)


def main() -> int:
    spark_proc = producer_proc = api_proc = None
    try:
        compose_up_kafka()
        create_topics()
        ensure_model_trained()

        spark_proc = start_spark_job(SPARK_LOG_PATH)
        wait_for_queries_active(SPARK_LOG_PATH)

        producer_proc = start_producer()
        alerts = consume_alerts(CONSUME_SECONDS)
        producer_proc.wait(timeout=30)

        assert_every_rule_fired(alerts)
        assert_schema_conformance(alerts)
        assert_no_duplicate_windowed_alerts(alerts)
        assert_p95_latency_under(alerts, max_seconds=10.0)
        assert_parquet_written()
        assert_no_error_lines(SPARK_LOG_PATH)

        api_proc = start_api()
        wait_for_consumer_alive()
        assert_alerts_snapshot()
        assert_stream_yields_frame()
        assert_dropped_is_zero()
        assert_index_served()

        _step(f"PASS - {len(alerts)} alerts, rules seen: {Counter(a['rule'] for a in alerts)}")
        return 0
    except SmokeFailure as exc:
        print(f"[smoke] FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        teardown([p for p in (spark_proc, producer_proc, api_proc) if p is not None])


if __name__ == "__main__":
    raise SystemExit(main())
