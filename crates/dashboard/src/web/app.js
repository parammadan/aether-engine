// Aether Dashboards SPA chassis.
//
// One WebSocket carries the whole cluster snapshot as JSON; each panel is a pure render
// of a slice of it. The snapshot contract (see the dashboard crate's `snapshot` docs):
//   { v, coordinator:{addr,reachable}, shard_count, vshard_group,
//     nodes:[{node_id,shard_id,role,millis_since_seen,draining}],
//     query:{ok,err,last:{ok,total_matched,answered,queried,ms,provenance:{summary,...}}},
//     aggregate:{by_origin:[{origin,count}]},
//     events:[{at_ms,msg}] }
// Panels added in later sub-tasks (geo-map, time-series) subscribe to the same snapshot.

const $ = (id) => document.getElementById(id);

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onmessage = (e) => {
    let snap;
    try { snap = JSON.parse(e.data); } catch { return; }
    render(snap);
  };
  ws.onclose = () => {
    setConn(false, "disconnected — retrying");
    setTimeout(connect, 1000);
  };
  ws.onerror = () => ws.close();
}

function setConn(ok, text) {
  $("conn-dot").className = "dot " + (ok ? "good" : "critical");
  $("conn-text").textContent = text;
}

function render(snap) {
  const coord = snap.coordinator || {};
  setConn(!!coord.reachable, coord.reachable ? `coordinator ${coord.addr}` : "coordinator unreachable");

  const q = snap.query || {};
  $("q-ok").textContent = q.ok ?? 0;
  $("q-err").textContent = q.err ?? 0;
  const last = q.last || {};
  $("q-matched").textContent = last.total_matched ?? 0;
  $("q-ms").textContent = last.ms ?? 0;
  const prov = last.provenance;
  $("q-prov").textContent = prov && prov.summary ? `provenance: ${prov.summary}` : "";
  if (prov && prov.placement_version != null) {
    $("placement").textContent = `placement v${prov.placement_version}`;
  }

  const agg = snap.aggregate || {};
  renderBars("by-aircraft", agg.by_aircraft, "aircraft_type");
  renderAltitude(agg.altitude_pcts || [], agg.altitude_hist || []);
  renderGeo(agg.geo_cells || []);
  renderSeries(snap.series || []);
  renderNodes(snap.nodes || []);
  renderEvents(snap.events || []);
}

// Altitude: percentile hero numbers (from the t-digest) + a distribution histogram.
function renderAltitude(pcts, hist) {
  const p = $("alt-pcts");
  p.innerHTML = pcts.length
    ? pcts.map((x) => `<div class="stat"><div class="num">${Math.round(x.value)}</div><div class="label">p${x.p} m</div></div>`).join("")
    : '<div class="muted">no data</div>';
  const rows = hist.map((h) => ({ k: `${Math.round(h.bucket)}–${Math.round(h.bucket + 2000)}m`, count: h.count }));
  renderBarRows("alt-hist", rows);
}

// Time-series: query latency as a line, with error ticks marked. A node kill shows as a
// latency spike + an error mark, then the line settling back — the failover as a picture.
const SW = 720, SH = 140;
function renderSeries(pts) {
  const svg = $("series");
  if (pts.length < 2) { svg.innerHTML = ""; $("series-note").textContent = "collecting…"; return; }
  const maxMs = Math.max(10, ...pts.map((p) => p.ms));
  const x = (i) => (i / (pts.length - 1)) * SW;
  const y = (ms) => SH - (ms / maxMs) * (SH - 10) - 5;
  const line = pts.map((p, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(p.ms).toFixed(1)}`).join(" ");
  const errs = pts
    .map((p, i) => (p.errored ? `<line x1="${x(i).toFixed(1)}" y1="0" x2="${x(i).toFixed(1)}" y2="${SH}" stroke="var(--critical)" stroke-width="1.5" opacity="0.7"/>` : ""))
    .join("");
  svg.innerHTML =
    errs +
    `<path d="${line}" fill="none" stroke="var(--series-1)" stroke-width="2"/>` +
    `<text x="4" y="12">${maxMs}ms</text>`;
  const errCount = pts.filter((p) => p.errored).length;
  $("series-note").textContent = `${pts.length}s window · peak ${maxMs}ms` + (errCount ? ` · ${errCount} error ticks (failover)` : " · no errors");
}

// Geo-density: each aggregate cell is a 10° grid square, shaded on a single-hue sequential
// ramp (light→dark = low→high count), per the dataviz rule for magnitude. lat/lon are the
// cell's lower-left corner; project equirectangularly into the 720×300 viewBox.
const GEO_W = 720, GEO_H = 300, CELL = 10;
function renderGeo(cells) {
  const svg = $("geomap");
  const proj = (lat, lon) => [((lon + 180) / 360) * GEO_W, ((90 - lat) / 180) * GEO_H];
  const cw = (CELL / 360) * GEO_W, ch = (CELL / 180) * GEO_H;
  const max = Math.max(1, ...cells.map((c) => c.count));
  // Sequential blue ramp (validated steps), light at low magnitude → dark at high.
  const ramp = ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"];
  const shade = (n) => ramp[Math.min(ramp.length - 1, Math.floor((n / max) * ramp.length))];

  let rects = "";
  for (const c of cells) {
    const [x, y] = proj(c.lat + CELL, c.lon); // top-left of the cell
    rects += `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${cw.toFixed(1)}" height="${ch.toFixed(1)}" `
      + `fill="${shade(c.count)}" stroke="var(--surface-1)" stroke-width="0.5"><title>${c.count} at ${c.lat},${c.lon}</title></rect>`;
  }
  // Equator + prime meridian guides so the projection reads as a map.
  const [, eqY] = proj(0, 0);
  const [pmX] = proj(0, 0);
  svg.innerHTML =
    `<line x1="0" y1="${eqY}" x2="${GEO_W}" y2="${eqY}" stroke="var(--border)" stroke-width="0.5"/>` +
    `<line x1="${pmX}" y1="0" x2="${pmX}" y2="${GEO_H}" stroke="var(--border)" stroke-width="0.5"/>` +
    rects;
  $("geo-note").textContent = cells.length
    ? `${cells.length} populated cells · darkest = ${max} flights`
    : "no geo data yet";
}

// Node health tiles grouped by shard. Status is color + LABEL, never color alone.
function statusOf(node) {
  if (node.draining) return ["warning", "draining"];
  if ((node.millis_since_seen ?? 0) > 6000) return ["critical", "stale"];
  if (node.role === "Leader") return ["good", "leader"];
  return ["neutral", "follower"];
}

function renderNodes(nodes) {
  const byShard = new Map();
  for (const n of nodes) {
    if (!byShard.has(n.shard_id)) byShard.set(n.shard_id, []);
    byShard.get(n.shard_id).push(n);
  }
  const el = $("nodes");
  if (nodes.length === 0) { el.innerHTML = '<div class="muted">no nodes registered</div>'; return; }
  const shards = [...byShard.keys()].sort((a, b) => a - b);
  el.innerHTML = shards.map((sid) => {
    const tiles = byShard.get(sid).map((n) => {
      const [cls, label] = statusOf(n);
      return `<div class="tile">
        <span class="dot ${cls}"></span>
        <span class="id">${esc(n.node_id)}</span>
        <span class="role">${label}</span>
        <button data-kill="${esc(n.node_id)}">kill</button>
      </div>`;
    }).join("");
    return `<div class="shard"><div class="shard-label">shard ${sid}</div><div class="tiles">${tiles}</div></div>`;
  }).join("");
  el.querySelectorAll("button[data-kill]").forEach((b) => {
    b.onclick = () => fetch(`/api/kill/${encodeURIComponent(b.dataset.kill)}`, { method: "POST" });
  });
}

// Value-counts bars from aggregate rows keyed by `keyName`.
function renderBars(id, rows, keyName) {
  renderBarRows(id, (rows || []).map((r) => ({ k: r[keyName], count: r.count })));
}

// Direct-labeled horizontal bars — identity is the text label, never color alone.
function renderBarRows(id, rows) {
  const el = $(id);
  if (!el) return;
  if (!rows || rows.length === 0) { el.innerHTML = '<div class="muted">no data</div>'; return; }
  const max = Math.max(...rows.map((r) => r.count), 1);
  el.innerHTML = rows.map((r) =>
    `<div class="bar-row">
       <span class="k" title="${esc(r.k)}">${esc(r.k)}</span>
       <span class="track"><span class="fill" style="width:${(r.count / max) * 100}%"></span></span>
       <span class="v">${r.count}</span>
     </div>`
  ).join("");
}

function renderEvents(events) {
  const el = $("events");
  el.innerHTML = events.slice().reverse().map((e) =>
    `<div class="ev"><span class="t">${(e.at_ms / 1000).toFixed(1)}s</span>${esc(e.msg)}</div>`
  ).join("");
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

// NLQ search bar: POST the question to /api/ask, show the composed answer + evidence.
const askForm = $("ask-form");
if (askForm) {
  askForm.onsubmit = async (e) => {
    e.preventDefault();
    const q = $("ask-input").value.trim();
    if (!q) return;
    const out = $("ask-answer");
    out.textContent = "thinking…";
    try {
      const r = await fetch(`/api/ask?q=${encodeURIComponent(q)}`);
      const a = await r.json();
      if (a.error) { out.textContent = a.error; return; }
      let text = a.answer || "";
      if (a.provenance && a.provenance.length) {
        text += "\n\n— evidence —\n" + a.provenance.map((p, i) => `  [${i + 1}] ${p}`).join("\n");
      }
      if (a.budget_exhausted) text += "\n\n(budget reached — partial answer)";
      out.textContent = text;
    } catch {
      out.textContent = "request failed";
    }
  };
}

connect();
