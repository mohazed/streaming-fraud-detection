/* fraud-stream dashboard. PLAN.md §12 Phase 4b.
 *
 * No build step, no framework, no CDN. Vanilla ES2015+.
 *
 *  - On load: fetch('/api/alerts?limit=200') for a snapshot, render.
 *  - Then new EventSource('/api/stream'), prepend each alert. EventSource
 *    reconnects by ITSELF - there is no custom reconnect loop here, only a
 *    connection pill driven by its onopen/onerror.
 *  - Client-side buffer capped at 500.
 *  - The tau slider recomputes precision/recall/confusion in this file, over
 *    the buffered alerts, on `input`. It NEVER calls the API.
 */
"use strict";

var CAP = 500;
var RATE_WINDOW_MS = 60 * 1000;

var state = {
  alerts: [],            // newest first, capped at CAP
  seen: new Set(),       // alert_id dedup across snapshot + stream
  tau: 0.65,
};

// ---- element handles ----
var el = {
  pill: document.getElementById("pill"),
  rate: document.getElementById("rate"),
  total: document.getElementById("total"),
  byRule: document.getElementById("by-rule"),
  bySev: document.getElementById("by-sev"),
  feed: document.getElementById("feed"),
  tau: document.getElementById("tau"),
  tauVal: document.getElementById("tau-val"),
  scoredNote: document.getElementById("scored-note"),
  precision: document.getElementById("precision"),
  recall: document.getElementById("recall"),
  cmTp: document.getElementById("cm-tp"),
  cmFp: document.getElementById("cm-fp"),
  cmFn: document.getElementById("cm-fn"),
  cmTn: document.getElementById("cm-tn"),
};

var RULES = ["high_value", "velocity", "geo_hop", "ml_score"];
var SEVERITIES = ["medium", "high", "critical"];
var SEV_COLOR = {
  medium: "var(--sev-medium)",
  high: "var(--sev-high)",
  critical: "var(--sev-critical)",
};

// ---- formatting helpers ----
function fmtAmount(a) {
  var v = typeof a.amount === "number" ? a.amount.toFixed(2) : a.amount;
  return v + " " + a.currency;
}

function parseMs(iso) {
  var t = Date.parse(iso);
  return isNaN(t) ? null : t;
}

function relTime(iso) {
  var t = parseMs(iso);
  if (t === null) return "";
  var s = Math.round((Date.now() - t) / 1000);
  if (s < 0) s = 0;
  if (s < 60) return s + "s ago";
  var m = Math.floor(s / 60);
  if (m < 60) return m + "m ago";
  var h = Math.floor(m / 60);
  if (h < 24) return h + "h ago";
  return Math.floor(h / 24) + "d ago";
}

function fmtPct(x) {
  if (x === null || isNaN(x)) return "—";      // em dash
  return (x * 100).toFixed(1) + "%";
}

// ---- inline SVG bars (hand-rolled, no chart library) ----
function barsSvg(entries) {
  var width = 260, rowH = 20, gap = 6, labelW = 66;
  var max = 1;
  entries.forEach(function (e) { if (e.count > max) max = e.count; });
  var h = entries.length * (rowH + gap);
  var parts = [];
  var y = 0;
  entries.forEach(function (e) {
    var w = Math.round((e.count / max) * (width - labelW - 34));
    parts.push('<text x="0" y="' + (y + rowH / 2 + 4) + '" class="bar-label">' + e.label + "</text>");
    parts.push('<rect x="' + labelW + '" y="' + (y + 2) + '" width="' + w +
               '" height="' + (rowH - 4) + '" rx="2" fill="' + e.color + '"/>');
    parts.push('<text x="' + (labelW + w + 6) + '" y="' + (y + rowH / 2 + 4) +
               '" class="bar-val">' + e.count + "</text>");
    y += rowH + gap;
  });
  return '<svg viewBox="0 0 ' + width + " " + h + '" width="100%" height="' + h + '">' +
         parts.join("") + "</svg>";
}

// ---- vitals ----
function renderVitals() {
  el.total.textContent = state.alerts.length;

  // alerts/min: count within RATE_WINDOW_MS of the newest alert's alert_time.
  var rate = 0;
  if (state.alerts.length) {
    var ref = parseMs(state.alerts[0].alert_time);
    if (ref !== null) {
      for (var i = 0; i < state.alerts.length; i++) {
        var t = parseMs(state.alerts[i].alert_time);
        if (t !== null && ref - t <= RATE_WINDOW_MS) rate++;
        else if (t !== null && ref - t > RATE_WINDOW_MS) break; // sorted newest-first
      }
    }
  }
  el.rate.textContent = rate;

  var byRule = {}, bySev = {};
  state.alerts.forEach(function (a) {
    byRule[a.rule] = (byRule[a.rule] || 0) + 1;
    bySev[a.severity] = (bySev[a.severity] || 0) + 1;
  });

  el.byRule.innerHTML = barsSvg(RULES.map(function (r) {
    return { label: r, count: byRule[r] || 0, color: "var(--bar)" };  // rules are grey
  }));
  el.bySev.innerHTML = barsSvg(SEVERITIES.map(function (s) {
    return { label: s, count: bySev[s] || 0, color: SEV_COLOR[s] };   // severity is the colour
  }));
}

// ---- feed ----
function buildRow(a, live) {
  var main = document.createElement("tr");
  main.className = "alert-row" + (live ? " fade" : "");

  var sev = document.createElement("td");
  sev.className = "swatch sev-" + a.severity;
  sev.innerHTML = "<span></span>";

  var rule = document.createElement("td");
  rule.className = "rule";
  rule.textContent = a.rule;

  var user = document.createElement("td");
  user.className = "mono-cell";
  user.textContent = a.user_id;

  var amt = document.createElement("td");
  amt.className = "amt";
  amt.textContent = fmtAmount(a);

  var loc = document.createElement("td");
  loc.textContent = (a.location || "") + (a.country ? " · " + a.country : "");

  var time = document.createElement("td");
  time.className = "rel";
  time.dataset.t = a.alert_time;
  time.textContent = relTime(a.alert_time);

  main.appendChild(sev);
  main.appendChild(rule);
  main.appendChild(user);
  main.appendChild(amt);
  main.appendChild(loc);
  main.appendChild(time);

  // click expands a <details>-style raw-JSON row
  var detail = document.createElement("tr");
  detail.className = "detail-row hidden";
  var cell = document.createElement("td");
  cell.colSpan = 6;
  var pre = document.createElement("pre");
  pre.textContent = JSON.stringify(a, null, 2);
  cell.appendChild(pre);
  detail.appendChild(cell);

  main.addEventListener("click", function () {
    detail.classList.toggle("hidden");
  });

  return [main, detail];
}

function prependRow(a, live) {
  var rows = buildRow(a, live);
  el.feed.insertBefore(rows[1], el.feed.firstChild);
  el.feed.insertBefore(rows[0], el.feed.firstChild);
}

function trimFeed() {
  // each alert is two <tr>s (main + detail); keep CAP alerts.
  while (el.feed.childElementCount > CAP * 2) {
    el.feed.removeChild(el.feed.lastChild);
  }
}

// ---- metrics (client-side only; never calls the API) ----
function renderMetrics() {
  var tau = state.tau;
  var tp = 0, fp = 0, fn = 0, tn = 0, scored = 0;

  state.alerts.forEach(function (a) {
    if (a.p_fraud === null || a.p_fraud === undefined) return; // R1/R2/R3: no score
    scored++;
    var pred = a.p_fraud >= tau ? 1 : 0;
    var truth = a.is_fraud ? 1 : 0;
    if (pred && truth) tp++;
    else if (pred && !truth) fp++;
    else if (!pred && truth) fn++;
    else tn++;
  });

  el.cmTp.textContent = tp;
  el.cmFp.textContent = fp;
  el.cmFn.textContent = fn;
  el.cmTn.textContent = tn;

  var precision = tp + fp ? tp / (tp + fp) : NaN;
  var recall = tp + fn ? tp / (tp + fn) : NaN;
  el.precision.textContent = fmtPct(precision);
  el.recall.textContent = fmtPct(recall);

  el.scoredNote.textContent = scored
    ? scored + " scored alert" + (scored === 1 ? "" : "s") + " (ml_score) in buffer"
    : "no scored alerts yet — rule alerts carry no p_fraud";
}

// ---- ingest ----
function addSnapshot(list) {
  // snapshot arrives newest-first from /api/alerts (deque appendleft).
  var frag = document.createDocumentFragment();
  list.forEach(function (a) {
    if (!a || !a.alert_id || state.seen.has(a.alert_id)) return;
    state.seen.add(a.alert_id);
    state.alerts.push(a);
    var rows = buildRow(a, false);
    frag.appendChild(rows[0]);
    frag.appendChild(rows[1]);
  });
  el.feed.appendChild(frag);
  if (state.alerts.length > CAP) state.alerts.length = CAP;
  trimFeed();
  renderVitals();
  renderMetrics();
}

function addLive(a) {
  if (!a || !a.alert_id || state.seen.has(a.alert_id)) return;
  state.seen.add(a.alert_id);
  state.alerts.unshift(a);
  if (state.alerts.length > CAP) state.alerts.length = CAP;
  prependRow(a, true);
  trimFeed();
  renderVitals();
  renderMetrics();
}

// ---- connection pill ----
function setPill(cls, text) {
  el.pill.className = "pill pill-" + cls;
  el.pill.textContent = text;
}

// ---- wiring ----
el.tau.addEventListener("input", function () {
  state.tau = parseFloat(el.tau.value);
  el.tauVal.textContent = state.tau.toFixed(2);
  renderMetrics();                       // client-side only
});

// keep relative times fresh without any CSS animation
setInterval(function () {
  var cells = el.feed.querySelectorAll(".rel");
  for (var i = 0; i < cells.length; i++) {
    cells[i].textContent = relTime(cells[i].dataset.t);
  }
}, 1000);

function connect() {
  var es = new EventSource("/api/stream");
  es.onopen = function () { setPill("live", "live"); };
  es.onmessage = function (ev) {
    try { addLive(JSON.parse(ev.data)); } catch (e) { /* ignore malformed frame */ }
  };
  // EventSource retries on its own; we only reflect the state in the pill.
  es.onerror = function () {
    setPill(es.readyState === EventSource.CLOSED ? "reconnecting" : "reconnecting",
            "reconnecting");
  };
}

function init() {
  el.tauVal.textContent = state.tau.toFixed(2);
  fetch("/api/alerts?limit=200")
    .then(function (r) { return r.json(); })
    .then(function (list) { addSnapshot(Array.isArray(list) ? list : []); })
    .catch(function () { /* snapshot optional; stream will fill in */ })
    .then(connect);   // open the stream whether or not the snapshot succeeded
}

init();
