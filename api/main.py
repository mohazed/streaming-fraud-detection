"""FastAPI dashboard service: four routes over a Kafka consumer thread and an
asyncio broadcaster.

Never run this with `--reload` (Trap D): the reloader forks a
second process, both run this module's `lifespan` and join the same consumer
group, and `fraud-alerts` has one partition - only one of the two processes
ever receives anything, and which one is a coin flip.
"""
from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from api.broadcast import Broadcaster
from api.consumer import AlertConsumer, ConsumerFactory
from common.config import CONFIG, Config

WEB_DIR = Path(__file__).resolve().parents[1] / "web"


def create_app(config: Config = CONFIG,
               consumer_factory: Optional[ConsumerFactory] = None) -> FastAPI:
    broadcaster = Broadcaster()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        loop = asyncio.get_running_loop()
        consumer = AlertConsumer(config, broadcaster.publish, loop=loop,
                                  consumer_factory=consumer_factory)
        consumer.start()
        app.state.consumer = consumer
        app.state.start_time = time.monotonic()
        try:
            yield
        finally:
            consumer.stop()

    app = FastAPI(lifespan=lifespan)
    app.state.broadcaster = broadcaster
    app.state.consumer = None
    app.state.start_time = time.monotonic()

    @app.get("/healthz")
    async def healthz():
        consumer = app.state.consumer
        return {
            "status": "ok",
            "alerts_seen": consumer.alerts_seen if consumer is not None else 0,
            "dropped": broadcaster.dropped,
            "consumer_alive": consumer.alive if consumer is not None else False,
            "uptime_s": time.monotonic() - app.state.start_time,
        }

    @app.get("/api/alerts")
    async def alerts(limit: int = 200):
        consumer = app.state.consumer
        return consumer.snapshot(limit) if consumer is not None else []

    @app.get("/api/stream")
    async def stream():
        async def event_source():
            queue = broadcaster.subscribe()
            try:
                while True:
                    item = await queue.get()
                    yield f"data: {json.dumps(item)}\n\n"
            finally:
                broadcaster.unsubscribe(queue)
        return StreamingResponse(event_source(), media_type="text/event-stream")

    app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")

    return app


app = create_app()
