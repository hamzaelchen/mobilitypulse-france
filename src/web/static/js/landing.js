/* Landing page: entry orchestration, live network canvas, count-up numbers, reveals. */
(function () {
  "use strict";
  const { STATE_COLORS, STATE_LABELS, reduced, nf, fmtTime, agoSec, stationKind, countTo, getJson } = MP;
  const $ = (s) => document.querySelector(s);
  const DEMO = MP.DEMO;

  // Frozen demo: swap every sentence that claims live data for its frozen wording.
  if (DEMO) {
    document.querySelectorAll("[data-demo]").forEach((el) => { el.textContent = el.dataset.demo; });
    $("#demo-note").hidden = false;
  }

  /* ---------- entry: split the headline into words, then let CSS stage them ---------- */
  const h1 = $("h1");
  const words = h1.textContent.trim().split(/\s+/);
  h1.setAttribute("aria-label", h1.textContent.trim());
  h1.innerHTML = words.map((w, i) => `<span class="w" aria-hidden="true" style="--i:${i}">${w}</span>`).join(" ");
  document.querySelectorAll("[data-in]").forEach((el, i) => el.style.setProperty("--i", words.length + i));
  const ready = () => document.body.classList.add("ready");
  if (document.fonts && document.fonts.ready) Promise.race([document.fonts.ready, new Promise((r) => setTimeout(r, 400))]).then(ready); else ready();

  /* ---------- scroll reveals ---------- */
  const revealEls = document.querySelectorAll("[data-reveal], .station");
  if ("IntersectionObserver" in window && !reduced()) {
    const io = new IntersectionObserver((entries) => {
      for (const e of entries) if (e.isIntersecting) { e.target.classList.add("in"); io.unobserve(e.target); }
    }, { rootMargin: "0px 0px -12% 0px", threshold: 0.08 });
    revealEls.forEach((el) => io.observe(el));
  } else revealEls.forEach((el) => el.classList.add("in"));

  /* ---------- transit line: packet length follows the rail ---------- */
  const rail = $(".rail"), line = $(".line");
  function sizeRail() {
    if (!rail) return;
    const vertical = getComputedStyle(rail).width === "2px" && rail.clientHeight > rail.clientWidth;
    line.style.setProperty("--rail-len", (vertical ? rail.clientHeight : rail.clientWidth) + "px");
  }
  sizeRail();
  window.addEventListener("resize", sizeRail);
  if (window.ResizeObserver && line) new ResizeObserver(sizeRail).observe(line);

  /* ---------- live network canvas ---------- */
  const canvas = $("#net"), ctx = canvas.getContext("2d");
  let W = 0, H = 0, dpr = 1;
  let nodes = [], edges = [], packets = [];
  let running = false, visible = true, raf = 0, t0 = performance.now(), lastSpawn = 0;
  const rgb = (hex) => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
  const mix = (a, b, t) => a.map((v, i) => Math.round(v + (b[i] - v) * t));

  function layout() {
    const r = canvas.getBoundingClientRect();
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    W = r.width; H = r.height;
    canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    if (!nodes.length) return;
    const narrow = W < 760;
    const box = narrow ? { x0: W * 0.06, x1: W * 0.94, y0: H * 0.1, y1: H * 0.92 } : { x0: W * 0.52, x1: W * 0.97, y0: H * 0.1, y1: H * 0.9 };
    const k = Math.cos((47.2 * Math.PI) / 180);
    const xs = nodes.map((n) => n.lon * k), ys = nodes.map((n) => -n.lat);
    // Robust bounds (5th-95th percentile): a few far-out stations must not flatten the dense centre.
    const q = (arr, p) => { const a = [...arr].sort((m, n) => m - n); return a[Math.min(a.length - 1, Math.max(0, Math.round(p * (a.length - 1))))]; };
    const minX = q(xs, 0.05), maxX = q(xs, 0.95), minY = q(ys, 0.05), maxY = q(ys, 0.95);
    const scale = Math.min((box.x1 - box.x0) / (maxX - minX || 1), (box.y1 - box.y0) / (maxY - minY || 1));
    const offX = box.x0 + ((box.x1 - box.x0) - (maxX - minX) * scale) / 2;
    const offY = box.y0 + ((box.y1 - box.y0) - (maxY - minY) * scale) / 2;
    // Outliers are compressed softly into a thin margin instead of being clamped onto the box edge,
    // which would draw them as a straight line of dots.
    const soft = (v, lo, hi) => {
      const pad = (hi - lo) * 0.07;
      if (v < lo) return lo - pad * (1 - Math.exp((v - lo) / (hi - lo)));
      if (v > hi) return hi + pad * (1 - Math.exp(-(v - hi) / (hi - lo)));
      return v;
    };
    nodes.forEach((n, i) => { n.x = soft(offX + (xs[i] - minX) * scale, box.x0, box.x1); n.y = soft(offY + (ys[i] - minY) * scale, box.y0, box.y1); });
    const cx = nodes.reduce((s, n) => s + n.x, 0) / nodes.length, cy = nodes.reduce((s, n) => s + n.y, 0) / nodes.length;
    nodes.forEach((n) => { n.delay = 300 + Math.hypot(n.x - cx, n.y - cy) * 2.2; });
  }

  function buildGraph() {
    const set = new Set();
    edges = [];
    nodes.forEach((a, i) => {
      const near = nodes.map((b, j) => ({ j, d: Math.hypot(a.x - b.x, a.y - b.y) })).filter((o) => o.j !== i).sort((p, q) => p.d - q.d).slice(0, 2);
      for (const { j } of near) {
        const key = i < j ? i + "-" + j : j + "-" + i;
        if (!set.has(key) && Math.hypot(a.x - nodes[j].x, a.y - nodes[j].y) < Math.max(W, H) * 0.16) { set.add(key); edges.push([i, j]); }
      }
    });
  }

  function setStations(stations) {
    const valid = stations.filter((s) => Math.abs(s.latitude) > 0.001 || Math.abs(s.longitude) > 0.001);
    const byId = new Map(nodes.map((n) => [n.id, n]));
    const fresh = !nodes.length;
    nodes = valid.map((s) => {
      const kind = stationKind(s), prev = byId.get(s.station_id);
      const to = rgb(STATE_COLORS[kind]);
      return { id: s.station_id, lat: s.latitude, lon: s.longitude, kind, cap: s.capacity || 12, to, from: prev ? prev.cur : to, cur: prev ? prev.cur : to, ct: prev ? 0 : 1, phase: Math.random(), x: 0, y: 0, delay: 0 };
    });
    layout();
    if (fresh || !edges.length) buildGraph();
    t0 = performance.now();
    if (reduced()) draw(performance.now()); else start();
  }

  function draw(now) {
    ctx.clearRect(0, 0, W, H);
    const t = now - t0, animated = !reduced();
    ctx.lineWidth = 1;
    ctx.strokeStyle = "rgba(141,180,255,0.10)";
    ctx.beginPath();
    for (const [a, b] of edges) { ctx.moveTo(nodes[a].x, nodes[a].y); ctx.lineTo(nodes[b].x, nodes[b].y); }
    ctx.stroke();

    for (const n of nodes) {
      const appear = animated ? Math.min(1, Math.max(0, (t - n.delay) / 500)) : 1;
      if (appear <= 0) continue;
      if (n.ct < 1) { n.ct = Math.min(1, n.ct + 0.03); n.cur = mix(n.from, n.to, n.ct); } else n.cur = n.to;
      const r = (3 + Math.min(2.2, n.cap / 12)) * (0.4 + 0.6 * appear);
      const [cr, cg, cb] = n.cur;
      if (animated && (n.kind === "empty" || n.kind === "full")) {
        const ph = ((t / 2400) + n.phase) % 1;
        ctx.beginPath();
        ctx.arc(n.x, n.y, r + 3 + ph * 11, 0, 6.2832);
        ctx.strokeStyle = `rgba(${cr},${cg},${cb},${(1 - ph) * 0.55 * appear})`;
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }
      ctx.beginPath();
      ctx.arc(n.x, n.y, r, 0, 6.2832);
      ctx.fillStyle = `rgba(${cr},${cg},${cb},${0.92 * appear})`;
      ctx.fill();
      ctx.lineWidth = 1;
    }

    if (animated && edges.length) {
      if (now - lastSpawn > 380 && packets.length < 9 && t > 1200) {
        const e = edges[(Math.random() * edges.length) | 0];
        packets.push({ a: nodes[e[0]], b: nodes[e[1]], born: now, life: 1300 + Math.random() * 1400, rev: Math.random() < 0.5 });
        lastSpawn = now;
      }
      packets = packets.filter((p) => now - p.born < p.life);
      for (const p of packets) {
        const u = (now - p.born) / p.life, e = u < 0.5 ? 2 * u * u : 1 - Math.pow(-2 * u + 2, 2) / 2;
        const [from, to] = p.rev ? [p.b, p.a] : [p.a, p.b];
        const x = from.x + (to.x - from.x) * e, y = from.y + (to.y - from.y) * e;
        const tx = from.x + (to.x - from.x) * Math.max(0, e - 0.16), ty = from.y + (to.y - from.y) * Math.max(0, e - 0.16);
        ctx.beginPath(); ctx.moveTo(tx, ty); ctx.lineTo(x, y);
        ctx.strokeStyle = `rgba(141,180,255,${0.7 * Math.sin(Math.PI * u)})`; ctx.lineWidth = 1.5; ctx.stroke();
        ctx.beginPath(); ctx.arc(x, y, 2, 0, 6.2832); ctx.fillStyle = `rgba(200,220,255,${0.95 * Math.sin(Math.PI * u)})`; ctx.fill();
      }
    }
  }

  function frame(now) {
    if (!running) return;
    draw(now);
    raf = requestAnimationFrame(frame);
  }
  function start() { if (running || reduced() || !visible || document.hidden) return; running = true; raf = requestAnimationFrame(frame); }
  function stop() { running = false; cancelAnimationFrame(raf); }
  document.addEventListener("visibilitychange", () => (document.hidden ? stop() : start()));
  if ("IntersectionObserver" in window) new IntersectionObserver((es) => { visible = es[0].isIntersecting; visible ? start() : stop(); }).observe($(".hero"));
  window.addEventListener("resize", () => { layout(); if (reduced()) draw(performance.now()); });
  if (window.matchMedia) window.matchMedia("(prefers-reduced-motion: reduce)").addEventListener("change", () => { stop(); if (reduced()) draw(performance.now()); else start(); });

  /* ---------- live numbers, honest about their age ---------- */
  const WHEN = { live: "en direct", delayed: "données en retard", stale: "données périmées" };
  async function refresh() {
    try {
      const [o, stations] = await Promise.all([getJson("/api/overview"), getJson("/api/stations")]);
      countTo($("#n-empty"), o.empty); countTo($("#n-full"), o.full); countTo($("#n-stations"), o.stations);
      $("#now").hidden = false; $("#now-error").hidden = true;
      const f = o.freshness;
      if (DEMO) {
        $("#dot").className = "dot";
        $("#live-when").textContent = `démo figée · relevé du ${MP.fmtDay(o.observed_at)}`;
        $("#now-label").textContent = `Relevé du ${MP.fmtDay(o.observed_at)}`;
      } else {
        $("#dot").className = "dot " + (f || "");
        if (f === "live") $("#dot").classList.add("beat");
        const age = o.data_age_seconds;
        $("#live-when").textContent = o.observed_at
          ? `${WHEN[f] || "état inconnu"} · relevé de ${fmtTime(o.observed_at)}${age != null ? " (" + MP.agoSec(age) + ")" : ""}`
          : "aucune donnée pour le moment";
      }
      setStations(stations);
    } catch (e) {
      console.error(e);
      $("#now").hidden = true; $("#now-error").hidden = false;
      $("#dot").className = "dot";
      $("#live-when").textContent = "indisponible";
    }
  }
  refresh();
  if (!DEMO) setInterval(refresh, 15000);

  (async () => {
    try {
      const c = await getJson("/api/config");
      if (c.repo_url) document.querySelectorAll("[data-second]").forEach((a) => { a.href = c.repo_url; a.textContent = "Code source et README"; });
      else if (DEMO) document.querySelectorAll("[data-second]").forEach((a) => { a.hidden = true; }); // no API docs in the demo
    } catch (e) { console.error(e); }
  })();
})();
