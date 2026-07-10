"""Phase 4b dashboard demo launcher (no Docker / no Kafka required).

Runs the REAL api.main.create_app() unmodified. The ONLY substitution is the
Kafka consumer_factory: dev sandboxes here have no broker, so instead of a
live confluent_kafka.Consumer receiving `fraud-alerts` produced by a real
spark/job.py, we inject a fake consumer whose poll() synthesizes one
ALERT_SCHEMA-shaped alert every ~0.5s. Everything downstream (Broadcaster,
the /api/stream SSE route, StaticFiles serving web/) is production code.

This exists purely to preview and demo the frontend (web/) and to exercise
EventSource reconnection without standing up the whole Kafka+Spark stack. The
real end-to-end path is `docker compose up` (Phase 5).

    PYTHONPATH=. python scripts/demo_dashboard.py [port]     # default 8000

Then open http://localhost:<port>/ .
"""
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import uvicorn

from api.main import create_app
from common.contracts import CURRENCIES, RULE_NAMES, SEVERITY

CITIES = [("Paris", "FR"), ("London", "GB"), ("New York", "US"),
          ("Berlin", "DE"), ("Tokyo", "JP"), ("Lagos", "NG")]
RATES = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17}


def _make_alert() -> dict:
    rule = random.choice(RULE_NAMES)
    city, country = random.choice(CITIES)
    currency = random.choice(CURRENCIES)
    amount = round(random.uniform(20, 4800), 2)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    is_fraud = 1 if random.random() < 0.55 else 0
    # ml_score alerts carry a p_fraud; rule alerts (R1/R2/R3) leave it null.
    if rule == "ml_score":
        p = random.betavariate(5, 2) if is_fraud else random.betavariate(2, 4)
        p_fraud = round(p, 4)
    else:
        p_fraud = None
    return {
        "alert_id": str(uuid.uuid4()),
        "transaction_id": f"t-{random.randint(0, 999999):07d}",
        "user_id": f"u{random.randint(1, 9999):04d}",
        "event_time": ts, "alert_time": ts,
        "rule": rule, "severity": SEVERITY[rule],
        "amount": amount, "currency": currency,
        "amount_eur": round(amount * RATES[currency], 2),
        "location": city, "country": country,
        "p_fraud": p_fraud,
        "detail": f"{rule} alert for {city}",
        "is_fraud": is_fraud,
    }


class _Msg:
    def __init__(self, v: bytes) -> None:
        self._v = v

    def value(self) -> bytes:
        return self._v

    def error(self):
        return None


class DripFeedConsumer:
    """Structurally ~ confluent_kafka.Consumer: subscribe/poll/close.
    poll() blocks ~0.5s then returns a fresh synthetic alert, forever."""

    def subscribe(self, topics) -> None:
        pass

    def poll(self, timeout: float = 1.0) -> _Msg:
        time.sleep(0.5)
        return _Msg(json.dumps(_make_alert()).encode("utf-8"))

    def close(self) -> None:
        pass


app = create_app(consumer_factory=lambda conf: DripFeedConsumer())

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    random.seed(42)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
