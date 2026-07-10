"""fastapi.testclient.TestClient; the Kafka consumer is always replaced by a
fake - no broker, no network.
"""
from __future__ import annotations

import json
import socket
import threading
import time
from pathlib import Path

import httpx
import uvicorn
from fastapi.testclient import TestClient

from api.main import create_app
from common.config import load_config
from common.contracts import ALERT_SCHEMA
from tests.fakes import FakeKafkaConsumer, FakeMessage

CONFIG = load_config({})
REPO_ROOT = Path(__file__).parents[1]


def _alert(transaction_id: str = "t-0000001") -> dict:
    return {
        "alert_id": "a-1", "transaction_id": transaction_id, "user_id": "u0001",
        "event_time": "2026-07-10T10:12:33Z", "alert_time": "2026-07-10T10:12:35Z",
        "rule": "velocity", "severity": "high",
        "amount": 185.20, "currency": "EUR", "amount_eur": 185.20,
        "location": "Paris", "country": "FR",
        "p_fraud": None,
        "detail": "5 transactions in window 10:12:00-10:13:00",
        "is_fraud": 1,
    }


def _alert_bytes(transaction_id: str = "t-0000001") -> bytes:
    return json.dumps(_alert(transaction_id)).encode("utf-8")


class _DelayedFakeConsumer:
    """Like FakeKafkaConsumer, but each message is only returned after a
    short delay - gives a test time to subscribe to the SSE stream before
    the consumer thread delivers anything."""

    def __init__(self, messages: list[bytes], delay: float = 0.1) -> None:
        self._messages = list(messages)
        self._delay = delay

    def subscribe(self, topics: list) -> None:
        pass

    def poll(self, timeout: float = 1.0):
        if self._messages:
            time.sleep(self._delay)
            return FakeMessage(self._messages.pop(0))
        return None

    def close(self) -> None:
        pass


class _LiveServer:
    """Runs `app` under a real uvicorn server on a real loopback socket, in a
    background thread.

    /api/stream never terminates by design (Trap E), and httpx's
    ASGITransport (used by both `TestClient` and `AsyncClient`) always
    `await self.app(...)`s to completion before returning a response - it
    has no support for an in-process generator that never finishes, so it
    hangs forever on this route. A real socket streams bytes as they
    arrive instead, exactly like a real browser tab would see it."""

    def __init__(self, app) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self.port = self._sock.getsockname()[1]
        self._server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
        self._thread = threading.Thread(
            target=self._server.run, kwargs={"sockets": [self._sock]}, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> "_LiveServer":
        self._thread.start()
        while not self._server.started:
            time.sleep(0.01)
        return self

    def __exit__(self, *exc_info) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5.0)


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    assert predicate(), "condition never became true within timeout"


def _client(messages: list[bytes] | None = None) -> TestClient:
    app = create_app(config=CONFIG,
                      consumer_factory=lambda conf: FakeKafkaConsumer(messages or []))
    return TestClient(app)


def test_healthz_shape():
    with _client() as client:
        resp = client.get("/healthz")
        assert resp.status_code == 200
        body = resp.json()
        assert set(body.keys()) == {"status", "alerts_seen", "dropped",
                                     "consumer_alive", "uptime_s"}
        assert body["consumer_alive"] is True


def test_alerts_snapshot_respects_limit():
    messages = [_alert_bytes(f"t-{i}") for i in range(5)]
    with _client(messages) as client:
        _wait_until(lambda: client.app.state.consumer.alerts_seen == 5)
        resp = client.get("/api/alerts", params={"limit": 3})
        assert resp.status_code == 200
        assert len(resp.json()) == 3


def test_alerts_snapshot_newest_first():
    messages = [_alert_bytes("t-1"), _alert_bytes("t-2"), _alert_bytes("t-3")]
    with _client(messages) as client:
        _wait_until(lambda: client.app.state.consumer.alerts_seen == 3)
        resp = client.get("/api/alerts")
        body = resp.json()
        assert body[0]["transaction_id"] == "t-3"
        assert body[-1]["transaction_id"] == "t-1"


def test_stream_content_type():
    app = create_app(config=CONFIG, consumer_factory=lambda conf: FakeKafkaConsumer([]))
    with _LiveServer(app) as server, httpx.Client() as client:
        with client.stream("GET", f"{server.base_url}/api/stream") as resp:
            assert resp.headers["content-type"].startswith("text/event-stream")


def test_stream_emits_frames():
    app = create_app(config=CONFIG,
                      consumer_factory=lambda conf: _DelayedFakeConsumer(
                          [_alert_bytes("t-1"), _alert_bytes("t-2")], delay=0.1))
    frames: list[str] = []
    with _LiveServer(app) as server, httpx.Client() as client:
        with client.stream("GET", f"{server.base_url}/api/stream") as resp:
            buf = ""
            for chunk in resp.iter_text():
                buf += chunk
                while "\n\n" in buf:
                    frame, buf = buf.split("\n\n", 1)
                    frames.append(frame)
                if len(frames) >= 2:
                    break

    assert len(frames) == 2
    for frame in frames:
        assert frame.startswith("data: ")
        payload = json.loads(frame[len("data: "):])
        assert set(payload.keys()) == set(ALERT_SCHEMA.fieldNames())


def test_stream_frame_format():
    app = create_app(config=CONFIG,
                      consumer_factory=lambda conf: _DelayedFakeConsumer(
                          [_alert_bytes("t-1")], delay=0.05))
    raw = ""
    with _LiveServer(app) as server, httpx.Client() as client:
        with client.stream("GET", f"{server.base_url}/api/stream") as resp:
            for chunk in resp.iter_text():
                raw += chunk
                if "\n\n" in raw:
                    break

    assert raw.endswith("\n\n")
    assert raw.count("\n\n") == 1


def test_index_served_at_root():
    with _client() as client:
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]


def test_no_reload_in_makefile():
    makefile = (REPO_ROOT / "Makefile").read_text()
    lines = makefile.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("api:"))
    recipe = []
    for line in lines[start + 1:]:
        if line.startswith("\t"):
            recipe.append(line)
        else:
            break
    assert recipe, "api target has no recipe"
    assert "--reload" not in "\n".join(recipe)
