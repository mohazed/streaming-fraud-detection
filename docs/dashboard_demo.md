# Dashboard demo — Claude-in-Chrome setup + demo prompt

The Phase 4b dashboard (`web/index.html`, `web/app.css`, `web/app.js`) is served
by the FastAPI app at `GET /`. This note explains how to let Claude Code drive a
real browser to demo it — specifically the accept criterion from PLAN.md §12
Phase 4b:

> Killing and restarting the alert source needs no page refresh — `EventSource`
> reconnects (pill `live` → `reconnecting` → `live`).

## What "reconnect" actually means here

Killing `spark/job.py` does **not** drop the browser↔API SSE connection — the API
keeps running, so alerts merely pause and resume and the pill stays `live`. The
code path the criterion cares about (`EventSource` auto-reconnecting) is only
exercised when the **SSE server itself** (the FastAPI/uvicorn process) goes away
and comes back. So the demo restarts the server, not Spark. `web/app.js` has **no
custom reconnect loop** — it only reflects `EventSource`'s own `onopen`/`onerror`
into the connection pill.

## 1. Install the Claude-in-Chrome extension

1. Open <https://claude.ai/chrome> in Google Chrome and install the extension.
2. Sign into claude.ai in Chrome with the **same account** as this Claude Code
   session.
3. Restart Chrome once after installing (first-time installs need it).
4. In the extension, grant site permission for `http://localhost` (the extension
   requires per-site permission before it can act on a page).

Verify from Claude Code: it should be able to call `tabs_context_mcp` without the
"Browser extension is not connected" error.

## 2. Start the dashboard

No Docker/Kafka on this machine? Use the demo launcher — it runs the **real**
`api.main.create_app()` and only fakes the Kafka consumer with a synthetic
alert drip-feed (see `scripts/demo_dashboard.py`):

```bash
PYTHONPATH=. .venv/bin/python3 scripts/demo_dashboard.py 8077
# open http://localhost:8077/
```

Full stack (Phase 5, on a Docker-capable machine): `docker compose up`, then open
`http://localhost:8000/`. In that case the "kill the source" step is a real
`spark/job.py` restart, but as noted above that alone won't flip the pill — to see
the reconnect, restart the `api` container / uvicorn process.

## 3. Prompt to paste into Claude Code once the extension is connected

> The fraud-stream dashboard is being served at http://localhost:8077/ by
> `scripts/demo_dashboard.py` (real `create_app()`, faked Kafka drip-feed).
> Open it in a new Chrome tab and record a GIF while you:
> 1. Confirm the three panels render — Vitals (alerts/min, total, by-rule and
>    by-severity SVG bars), the Feed table (newest first, severity swatches), and
>    Metrics (precision/recall + 2×2 confusion). Confirm the connection pill reads
>    `live`.
> 2. Drag the τ slider from left to right and confirm precision, recall, and the
>    four confusion-matrix cells recompute on `input` with no network request
>    (check the Network tab stays quiet — the slider must never call the API).
> 3. Click a feed row and confirm it expands to show the raw alert JSON.
> 4. In a terminal, kill the demo server (`pkill -f demo_dashboard.py`) and
>    confirm the pill flips to `reconnecting` within a few seconds — WITHOUT
>    refreshing the page.
> 5. Restart it (`PYTHONPATH=. .venv/bin/python3 scripts/demo_dashboard.py 8077`)
>    and confirm the pill returns to `live` and new alerts resume prepending — still
>    no page refresh.
> Export the GIF to `docs/dashboard_demo.gif` and take a full-page screenshot to
> `docs/dashboard.png` (the Phase 5 deliverable).

## 4. Headless verification already done (no browser needed)

- `node --check web/app.js` — syntax OK.
- The real `renderMetrics()`/`renderVitals()` from `web/app.js` were run under a
  stubbed DOM in node against a controlled buffer:
  - τ=0.65 → TP=2 FP=1 FN=1 TN=2, precision/recall 66.7%.
  - τ=0.35 → recall 100%, precision 75% (threshold lowered).
  - τ=0.95 → nothing flagged, recall 0%, precision "—".
  - Rule alerts (`p_fraud: null`) are excluded from the confusion matrix; only
    `ml_score` alerts are scored.
- `create_app()` served `/`, `/app.css`, `/app.js`, `/api/alerts`, `/healthz`, and
  streamed `/api/stream` correctly with the drip-feed consumer.
