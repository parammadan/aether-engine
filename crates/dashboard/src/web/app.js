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

  renderByOrigin(snap.aggregate && snap.aggregate.by_origin);
  renderNodes(snap.nodes || []);
  renderEvents(snap.events || []);
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

function renderByOrigin(rows) {
  const el = $("by-origin");
  if (!rows || rows.length === 0) { el.innerHTML = '<div class="muted">no data</div>'; return; }
  const max = Math.max(...rows.map((r) => r.count), 1);
  el.innerHTML = rows.map((r) =>
    `<div class="bar-row">
       <span class="k" title="${esc(r.origin)}">${esc(r.origin)}</span>
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

connect();
