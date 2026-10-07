/* Application: live stream (SSE) with polling fallback, map with in-place marker transitions,
   transfer trucks, FLIP-reordered priorities, drawn charts, toasts. Honest about data freshness. */
(function () {
  "use strict";
  const { STATE_COLORS: COLORS, STATE_LABELS: LABELS, STATE_ORDER, reduced, esc, nf, fmtTime, fmtTimeSec, fmtDay, pct, dec, agoSec, plural, hoursLabel, stationKind, countTo, getJson: api } = MP;
  const $ = (s) => document.querySelector(s);
  const DEMO = MP.DEMO; // frozen demo: no stream, no polling, no "live" wording
  const HOME = DEMO ? (DEMO.base || "./") : "/";
  const NEED = { needs_bikes: "Besoin de vélos", needs_docks: "Besoin de places" };
  const ICON_X = '<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M5 5l10 10M15 5L5 15"/></svg>';
  const ICON_ARROW = '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M3 10h13M12 5l5 5-5 5"/></svg>';

  const state = { stations: [], priorities: [], transfers: [], filter: "all", query: "", showTransfers: true, selected: null };
  const live = { kpis: null, offsetMs: 0, link: "connecting", dbDown: false, source: null, retryMs: 3000, pollTimer: null, lastFreshness: null, lastMessageAt: null };
  let firstRender = true;

  /* =============================== toasts =============================== */
  const toasts = $("#toasts");
  function toast(message, { detail = "", color = "var(--flow)", ms = 6500 } = {}) {
    while (toasts.children.length >= 3) dismiss(toasts.firstElementChild);
    const el = document.createElement("div");
    el.className = "toast";
    el.style.setProperty("--c", color);
    el.innerHTML = `<i></i><p>${esc(message)}${detail ? `<small>${esc(detail)}</small>` : ""}</p><button type="button" aria-label="Fermer la notification">${ICON_X}</button>`;
    el.querySelector("button").addEventListener("click", () => dismiss(el));
    toasts.appendChild(el);
    requestAnimationFrame(() => requestAnimationFrame(() => el.classList.add("in")));
    el._timer = setTimeout(() => dismiss(el), ms);
  }
  function dismiss(el) {
    if (!el || el._gone) return;
    el._gone = true; clearTimeout(el._timer);
    el.classList.remove("in");
    setTimeout(() => el.remove(), reduced() ? 0 : 320);
  }

  /* =============================== KPIs =============================== */
  const KPI_DEFS = [
    { key: "active", label: "Stations actives", c: "var(--flow)" },
    { key: "empty", label: "Vides", c: "var(--empty)" },
    { key: "full", label: "Saturées", c: "var(--full)" },
    { key: "oos", label: "Hors service", c: "var(--oos)" },
    { key: "bikes", label: "Vélos disponibles", c: "var(--ok)" },
    { key: "fill", label: "Remplissage moyen", c: "var(--high)" },
  ];
  $("#kpis").innerHTML = KPI_DEFS.map((d) => `<div class="kpi" id="k-${d.key}"><div class="label"><i style="--c:${d.c}"></i>${d.label}</div><div class="value"><span class="v">–</span><span class="delta" aria-hidden="true"></span></div><div class="sub">&nbsp;</div></div>`).join("");
  const prevKpi = {};
  function renderKpis(o) {
    const active = o.stations - o.out_of_service;
    const vals = { active, empty: o.empty, full: o.full, oos: o.out_of_service, bikes: o.bikes, fill: o.avg_fill_rate == null ? null : o.avg_fill_rate * 100 };
    const subs = {
      active: `sur ${nf.format(o.stations)} au total`,
      empty: `${pct(o.empty / Math.max(active, 1), 1)} des stations actives`,
      full: `${pct(o.full / Math.max(active, 1), 1)} des stations actives`,
      oos: "non opérationnelles",
      bikes: plural(o.docks, "place libre", "places libres"),
      fill: "stations opérationnelles",
    };
    for (const d of KPI_DEFS) {
      const root = $("#k-" + d.key), v = root.querySelector(".v"), delta = root.querySelector(".delta");
      const val = vals[d.key];
      root.querySelector(".sub").textContent = subs[d.key];
      if (val == null) { v.textContent = "–"; v.dataset.value = ""; continue; }
      countTo(v, val, { format: d.key === "fill" ? (x) => `${Math.round(x)} %` : (x) => nf.format(Math.round(x)) });
      const prev = prevKpi[d.key];
      if (prev != null && Math.round(val) !== Math.round(prev)) {
        const diff = Math.round(val) - Math.round(prev);
        delta.textContent = (diff > 0 ? "+" : "−") + Math.abs(diff);
        delta.classList.add("on");
        clearTimeout(delta._t);
        delta._t = setTimeout(() => delta.classList.remove("on"), 7000);
      }
      prevKpi[d.key] = val;
    }
  }

  /* =============================== live status =============================== */
  const LIVE_MAX_S = 150, DELAYED_MAX_S = 600; // same thresholds as the server (src/web/status.py)
  const CAUSE_MSG = {
    collection_stopped: "La collecte est interrompue : aucune nouvelle donnée du flux vélos (GBFS) n'est arrivée récemment. Les chiffres affichés datent de l'heure indiquée en haut.",
    updater_down: "La mise à jour automatique est arrêtée (service live-updater). Les chiffres affichés datent de l'heure indiquée en haut.",
    updater_error: "La mise à jour automatique rencontre une erreur. Les chiffres affichés datent de l'heure indiquée en haut.",
    no_data: "Aucune donnée disponible pour le moment.",
    database_unavailable: "Base de données indisponible : les chiffres affichés sont les derniers reçus et ne sont plus à jour.",
  };
  const noteServerTime = (k) => { if (k && k.server_time) live.offsetMs = Date.parse(k.server_time) - Date.now(); };

  function renderDemoStatus() {
    const k = live.kpis, wrap = $("#bannerWrap");
    $("#dot").className = "dot";
    if (live.link === "offline") $("#liveText").textContent = "Données de démonstration introuvables";
    else if (!k) $("#liveText").textContent = "Chargement…";
    else {
      $("#liveText").innerHTML = `<b>Démo figée</b> · données du <span class="t">${esc(fmtDay(k.observed_at))}</span>`;
      $("#banner").textContent = "Démo figée : capture de données réelles. Rien ne se met à jour ici ; le projet complet collecte et met ces données à jour en continu (voir le dépôt).";
      wrap.classList.add("show");
    }
  }

  function renderLiveStatus() {
    if (DEMO) { renderDemoStatus(); return; }
    const k = live.kpis, dot = $("#dot"), text = $("#liveText"), banner = $("#banner");
    let freshness = "unknown", cause = null;
    if (k && k.observed_at) {
      const age = (Date.now() + live.offsetMs - Date.parse(k.observed_at)) / 1000;
      freshness = age <= LIVE_MAX_S ? "live" : age <= DELAYED_MAX_S ? "delayed" : "stale";
      cause = freshness === "live" ? null : (k.cause || "collection_stopped");
      const label = { live: "En direct", delayed: "Données en retard", stale: "Données périmées" }[freshness];
      text.innerHTML = `<b>${label}</b> · <span class="t">${esc(fmtTime(k.observed_at))}</span> (${esc(agoSec(Math.max(age, 0)))})${live.link === "polling" ? " · actualisation toutes les 30 s" : ""}`;
    } else {
      text.textContent = live.link === "offline" ? "Hors ligne" : live.dbDown ? "Base de données indisponible" : k ? "Aucune donnée" : "Connexion…";
      if (k && !k.observed_at) cause = "no_data";
    }
    if (live.dbDown) { freshness = "unknown"; cause = "database_unavailable"; }
    dot.className = "dot " + (freshness === "unknown" ? "" : freshness);
    let message = "", bad = false;
    if (live.link === "offline") { message = "Impossible de joindre l'API. Vérifiez que les services web et postgres tournent (docker compose ps). Les chiffres affichés, s'il y en a, ne sont plus à jour."; bad = true; }
    else if (cause) { message = CAUSE_MSG[cause] || ""; bad = freshness !== "delayed"; }
    banner.textContent = message || banner.textContent;
    const wrap = $("#bannerWrap");
    wrap.classList.toggle("show", !!message);
    wrap.classList.toggle("bad", bad);

    if (live.lastFreshness && live.lastFreshness !== freshness && freshness !== "unknown") {
      if (freshness === "live") toast("Les données sont de nouveau à jour.", { color: "var(--ok)" });
      else if (live.lastFreshness === "live") toast(freshness === "delayed" ? "Données en retard" : "Données périmées", { detail: CAUSE_MSG[cause] ? "La collecte ou la mise à jour a pris du retard." : "", color: "var(--warn)" });
    }
    if (freshness !== "unknown") live.lastFreshness = freshness;
  }

  function beat() {
    live.lastMessageAt = Date.now();
    const dot = $("#dot");
    $("#live").title = "Dernier message du flux : " + fmtTimeSec(live.lastMessageAt);
    if (reduced() || !dot.classList.contains("live")) return;
    dot.classList.remove("beat"); void dot.offsetWidth; dot.classList.add("beat");
  }

  /* =============================== map =============================== */
  const map = L.map("map", { zoomControl: true, attributionControl: true }).setView([47.2184, -1.5536], 13);
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 19, attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>' }).addTo(map);
  const layerLinks = L.layerGroup().addTo(map), layerHalo = L.layerGroup().addTo(map), layerStations = L.layerGroup().addTo(map);

  $("#legend").innerHTML = STATE_ORDER.map((k) => `<div><i style="background:${COLORS[k]}"></i>${LABELS[k]}</div>`).join("") +
    `<div class="legend-truck"><svg viewBox="0 0 34 24" aria-hidden="true"><rect x="1" y="3" width="19" height="14" rx="2.5" fill="#e8eefb"/><path d="M21 7h6.5l5.5 5.5V17H21z" fill="#a8b5cf"/><circle cx="8" cy="19" r="3.2" fill="#0a1020" stroke="#e8eefb" stroke-width="2"/><circle cx="27" cy="19" r="3.2" fill="#0a1020" stroke="#e8eefb" stroke-width="2"/></svg>Transfert suggéré (n = vélos)</div>`;

  const hasPosition = (s) => Math.abs(s.latitude) > 0.001 || Math.abs(s.longitude) > 0.001; // the feed has stations at (0, 0)
  function visible(s) {
    if (!hasPosition(s)) return false;
    if (state.filter !== "all" && stationKind(s) !== state.filter) return false;
    return !state.query || s.station_name.toLowerCase().includes(state.query);
  }

  const markers = new Map(); // station_id -> { m, halo, kind }
  function dropMarker(id) {
    const e = markers.get(id); if (!e) return;
    layerStations.removeLayer(e.m); if (e.halo) layerHalo.removeLayer(e.halo);
    markers.delete(id);
  }
  function syncMarkers(fit) {
    const seen = new Set(), pts = [];
    for (const s of state.stations) {
      if (!visible(s)) continue;
      const id = s.station_id, kind = stationKind(s), color = COLORS[kind];
      const r = 5 + Math.min(7, (s.capacity || 12) / 6);
      let e = markers.get(id);
      if (!e) {
        const m = L.circleMarker([s.latitude, s.longitude], { radius: r, color: "#0a1020", weight: 1.5, fillColor: color, fillOpacity: 0.95, className: "stn" });
        m.on("click", () => openStation(id, false));
        m.addTo(layerStations);
        e = { m, halo: null, kind };
        markers.set(id, e);
      } else if (e.kind !== kind) {
        e.m.setStyle({ fillColor: color });
        const path = e.m.getElement();
        if (path && !reduced() && !firstRender) { path.classList.remove("flash"); void path.getBoundingClientRect(); path.classList.add("flash"); path.addEventListener("animationend", () => path.classList.remove("flash"), { once: true }); }
        e.kind = kind;
      }
      e.m.setRadius(r);
      e.m.unbindTooltip().bindTooltip(`<b>${esc(s.station_name)}</b><br>${plural(s.num_bikes_available, "vélo", "vélos")} · ${plural(s.num_docks_available, "place", "places")}`, { direction: "top", offset: [0, -r] });
      const critical = (kind === "empty" || kind === "full") && s.priority_rank && s.priority_rank <= 10;
      if (critical && !e.halo) { e.halo = L.circleMarker([s.latitude, s.longitude], { radius: r + 7, color, weight: 1, fillColor: color, fillOpacity: 0.18, className: "halo", interactive: false }).addTo(layerHalo); }
      else if (!critical && e.halo) { layerHalo.removeLayer(e.halo); e.halo = null; }
      else if (e.halo) e.halo.setStyle({ color, fillColor: color });
      seen.add(id); pts.push([s.latitude, s.longitude]);
    }
    for (const id of [...markers.keys()]) if (!seen.has(id)) dropMarker(id);
    if (fit && pts.length) map.fitBounds(pts, { padding: [30, 30], maxZoom: 14 });
  }

  /* ---- transfer trucks: one per suggested transfer, driving along its segment ---- */
  const links = new Map(); // "from>to" -> { line, truck, ms, off }
  const TRUCK_SVG = '<svg viewBox="0 0 34 24" aria-hidden="true"><rect x="1" y="3" width="19" height="14" rx="2.5" fill="#e8eefb"/><path d="M21 7h6.5l5.5 5.5V17H21z" fill="#a8b5cf"/><circle cx="8" cy="19" r="3.2" fill="#0a1020" stroke="#e8eefb" stroke-width="2"/><circle cx="27" cy="19" r="3.2" fill="#0a1020" stroke="#e8eefb" stroke-width="2"/></svg>';
  const truckIcon = (n) => L.divIcon({ className: "truck", html: `<div class="tk">${TRUCK_SVG}<span class="cargo">${n}</span></div>`, iconSize: [34, 24], iconAnchor: [17, 12] });

  function syncTransfers() {
    const want = state.showTransfers ? state.transfers.slice(0, 10) : [];
    const keys = new Set(want.map((t) => t.from_station_id + ">" + t.to_station_id));
    for (const [k, e] of links) if (!keys.has(k)) { layerLinks.removeLayer(e.line); map.removeLayer(e.truck); links.delete(k); }
    want.forEach((t, i) => {
      const k = t.from_station_id + ">" + t.to_station_id, a = [t.from_latitude, t.from_longitude], b = [t.to_latitude, t.to_longitude];
      let e = links.get(k);
      const tip = `${esc(t.from_station_name)} → ${esc(t.to_station_name)}<br><b>${plural(t.bikes, "vélo", "vélos")}</b> · ${dec(t.distance_km)} km`;
      if (!e) {
        const line = L.polyline([a, b], { color: "#8db4ff", weight: 1.6 + Math.min(2.5, t.bikes / 5), opacity: 0.55, dashArray: "6 8", className: "link" }).addTo(layerLinks);
        const truck = L.marker(a, { icon: truckIcon(t.bikes), interactive: false, keyboard: false, zIndexOffset: 600 }).addTo(map);
        e = { line, truck, bikes: t.bikes, ms: 5200 + t.distance_km * 2400, off: (i * 0.173) % 1 };
        links.set(k, e);
      } else {
        e.line.setLatLngs([a, b]);
        if (e.bikes !== t.bikes) { e.truck.setIcon(truckIcon(t.bikes)); e.bikes = t.bikes; }
      }
      e.line.unbindTooltip().bindTooltip(tip, { sticky: true });
      e.a = a; e.b = b;
      const el = e.truck.getElement();
      if (el) el.style.setProperty("--flip", b[1] < a[1] ? -1 : 1);
    });
    if (reduced() || !links.size) { stopTrucks(); for (const e of links.values()) { e.truck.setLatLng([(e.a[0] + e.b[0]) / 2, (e.a[1] + e.b[1]) / 2]); const el = e.truck.getElement(); if (el) el.style.opacity = 1; } }
    else startTrucks();
  }
  let truckRaf = 0;
  const ease = (u) => (u < 0.5 ? 2 * u * u : 1 - Math.pow(-2 * u + 2, 2) / 2);
  function truckFrame(now) {
    for (const e of links.values()) {
      const ph = (now / e.ms + e.off) % 1;
      let u, op = 1;
      if (ph < 0.1) { u = 0; op = ph / 0.1; } else if (ph > 0.9) { u = 1; op = (1 - ph) / 0.1; } else u = ease((ph - 0.1) / 0.8);
      e.truck.setLatLng([e.a[0] + (e.b[0] - e.a[0]) * u, e.a[1] + (e.b[1] - e.a[1]) * u]);
      const el = e.truck.getElement(); if (el) el.style.opacity = op;
    }
    truckRaf = requestAnimationFrame(truckFrame);
  }
  function startTrucks() { if (!truckRaf && !document.hidden) truckRaf = requestAnimationFrame(truckFrame); }
  function stopTrucks() { cancelAnimationFrame(truckRaf); truckRaf = 0; }
  document.addEventListener("visibilitychange", () => { if (document.hidden) stopTrucks(); else if (state.showTransfers && !reduced()) startTrucks(); });
  if (window.matchMedia) window.matchMedia("(prefers-reduced-motion: reduce)").addEventListener("change", syncTransfers);

  /* =============================== side panels =============================== */
  function scoreColor(s) { return s >= 75 ? "var(--empty)" : s >= 50 ? "var(--low)" : "var(--warn)"; }
  const hoursText = (p) => hoursLabel(p.hours_in_current_state, p.state_age_is_lower_bound);

  // Priorities: rows are keyed by station and reordered with FLIP so a rank change reads as movement.
  function renderPriorities() {
    const el = $("#p-prio"), list = state.priorities;
    $("#n-prio").textContent = list.length || "";
    if (!list.length) { el.innerHTML = `<div class="empty-state">Aucune station à rééquilibrer pour le moment.</div>`; return; }
    let ul = el.querySelector("ul.rows");
    if (!ul) { el.innerHTML = '<ul class="rows"></ul>'; ul = el.querySelector("ul.rows"); }
    const prev = new Map();
    ul.querySelectorAll("li").forEach((li) => prev.set(li.dataset.id, li.getBoundingClientRect().top));
    const byId = new Map([...ul.children].map((li) => [li.dataset.id, li]));
    const frag = document.createDocumentFragment(), fresh = [];
    for (const p of list) {
      let li = byId.get(p.station_id);
      if (!li) { li = document.createElement("li"); li.dataset.id = p.station_id; li.innerHTML = '<button type="button" class="row enter"><span class="rank"></span><span><span class="name"></span><span class="meta"></span></span><span class="score"><b></b><span class="bar"><i></i></span></span></button>'; fresh.push(li); }
      byId.delete(p.station_id);
      const c = p.need_type === "needs_docks" ? "var(--full)" : "var(--empty)", btn = li.firstElementChild;
      btn.style.setProperty("--c", c);
      btn.querySelector(".rank").textContent = p.priority_rank;
      const nm = btn.querySelector(".name"); nm.textContent = p.station_name; nm.title = p.station_name;
      btn.querySelector(".meta").textContent = `${NEED[p.need_type] || ""} · écart ${plural(p.bikes_gap, "vélo", "vélos")} · ${LABELS[p.state] || p.state} depuis ${hoursText(p)}`;
      btn.querySelector(".score b").textContent = Math.round(p.priority_score);
      const bar = btn.querySelector(".bar"); bar.style.setProperty("--c", scoreColor(p.priority_score));
      requestAnimationFrame(() => bar.firstElementChild.style.setProperty("--v", Math.min(1, p.priority_score / 100)));
      btn.setAttribute("aria-label", `Priorité ${p.priority_rank} : ${p.station_name}, score ${Math.round(p.priority_score)}`);
      frag.appendChild(li);
    }
    byId.forEach((li) => li.remove());
    ul.appendChild(frag);
    fresh.forEach((li) => requestAnimationFrame(() => requestAnimationFrame(() => li.firstElementChild.classList.remove("enter"))));
    if (reduced() || el.hidden) return;
    ul.querySelectorAll("li").forEach((li) => {
      const before = prev.get(li.dataset.id); if (before == null) return;
      const dy = before - li.getBoundingClientRect().top; if (Math.abs(dy) < 2) return;
      li.style.transition = "none"; li.style.transform = `translateY(${dy}px)`;
      requestAnimationFrame(() => requestAnimationFrame(() => {
        li.style.transition = "transform 420ms cubic-bezier(0.23, 1, 0.32, 1)"; li.style.transform = "";
        li.addEventListener("transitionend", () => { li.style.transition = ""; }, { once: true });
      }));
    });
  }

  function renderTransferList() {
    const el = $("#p-trans");
    $("#n-trans").textContent = state.transfers.length || "";
    if (!state.transfers.length) { el.innerHTML = `<div class="empty-state">Aucun transfert proposé.</div>`; return; }
    el.innerHTML = `<ul class="rows">${state.transfers.map((t) => `<li><button type="button" class="transfer" data-from="${esc(t.from_station_id)}" data-to="${esc(t.to_station_id)}">
      <span class="path"><span>${esc(t.from_station_name)}</span>${ICON_ARROW}<span>${esc(t.to_station_name)}</span></span>
      <span class="meta"><span><b>${nf.format(t.bikes)}</b> ${t.bikes < 2 ? "vélo" : "vélos"}</span><span>${dec(t.distance_km)} km</span><span>${esc(LABELS[t.from_state] || t.from_state)} → ${esc(LABELS[t.to_state] || t.to_state)}</span><span>score ${Math.round(t.priority_score)}</span></span></button></li>`).join("")}</ul>`;
  }

  function renderQuality(rows) {
    const el = $("#p-qual"), counts = { PASS: 0, WARN: 0, FAIL: 0 };
    rows.forEach((r) => counts[r.status]++);
    $("#n-qual").textContent = counts.FAIL ? `${counts.FAIL} FAIL` : counts.WARN ? `${counts.WARN} WARN` : rows.length ? "OK" : "";
    if (!rows.length) { el.innerHTML = `<div class="empty-state">Aucun contrôle qualité enregistré. Lancez le job <code>quality</code>.</div>`; return; }
    const col = { PASS: "var(--ok)", WARN: "var(--warn)", FAIL: "var(--empty)" };
    el.innerHTML = `<div class="q-sum">${["PASS", "WARN", "FAIL"].map((k) => `<span class="pill" style="--c:${col[k]}"><i></i>${counts[k]} ${k}</span>`).join("")}<span class="hint" style="margin-left:auto">${esc(fmtDay(rows[0].checked_at))}</span></div>` +
      rows.map((r) => `<div class="q-item"><div class="status" style="--c:${col[r.status]}">${r.status}</div><div><div class="t">${esc(r.name.replaceAll("_", " "))} <span style="color:var(--text-3);font-weight:400">· ${r.layer === "lake" ? "lac" : "service"}</span></div><div class="d">${esc(r.detail || "")}${r.threshold ? ` <span style="color:var(--text-3)">(seuil : ${esc(r.threshold)})</span>` : ""}</div></div></div>`).join("");
  }

  /* ---- tabs (arrow keys, sliding indicator) ---- */
  const tabs = [...document.querySelectorAll(".tab")];
  function selectTab(t, focus) {
    tabs.forEach((x, i) => { const on = x === t; x.setAttribute("aria-selected", on); x.tabIndex = on ? 0 : -1; $("#p-" + x.dataset.p).hidden = !on; if (on) $("#tabInd").style.setProperty("--x", i * 100 + "%"); });
    if (focus) t.focus();
  }
  document.querySelector(".tabs").addEventListener("click", (e) => { const t = e.target.closest(".tab"); if (t) selectTab(t, false); });
  document.querySelector(".tabs").addEventListener("keydown", (e) => {
    const i = tabs.indexOf(document.activeElement); if (i < 0) return;
    const next = { ArrowRight: (i + 1) % tabs.length, ArrowLeft: (i + tabs.length - 1) % tabs.length, Home: 0, End: tabs.length - 1 }[e.key];
    if (next != null) { e.preventDefault(); selectTab(tabs[next], true); }
  });

  /* =============================== charts =============================== */
  function lineChart(el, { series, yMax, yFmt, height = 260, gapMinutes = 90, xDomain, animate = false }) {
    const W = 760, H = height, m = { l: 44, r: 12, t: 12, b: 26 };
    const pts = series.flatMap((s) => s.data);
    if (!pts.length) { el.innerHTML = `<div class="empty-state">Pas encore assez de données.</div>`; return; }
    const [x0, x1] = xDomain || [Math.min(...pts.map((p) => p.t)), Math.max(...pts.map((p) => p.t))];
    const X = (t) => m.l + ((t - x0) / Math.max(x1 - x0, 1)) * (W - m.l - m.r);
    const Y = (v) => H - m.b - (v / yMax) * (H - m.t - m.b);
    let g = "";
    for (let i = 0; i <= 4; i++) { const v = (yMax * i) / 4, y = Y(v); g += `<line x1="${m.l}" x2="${W - m.r}" y1="${y}" y2="${y}" stroke="#1b2a47" stroke-width="1"/><text x="${m.l - 8}" y="${y + 4}" text-anchor="end">${esc(yFmt(v))}</text>`; }
    const ticks = 6;
    for (let i = 0; i <= ticks; i++) { const t = x0 + ((x1 - x0) * i) / ticks; g += `<text x="${X(t)}" y="${H - 6}" text-anchor="${i === 0 ? "start" : i === ticks ? "end" : "middle"}">${esc(fmtTime(t))}</text>`; }
    let paths = "";
    series.forEach((s, si) => {
      const segs = []; let cur = [];
      s.data.forEach((p, i) => { if (i && (p.t - s.data[i - 1].t) / 60000 > gapMinutes) { segs.push(cur); cur = []; } cur.push(p); });
      if (cur.length) segs.push(cur);
      for (const seg of segs) {
        const d = seg.map((p, i) => `${i ? "L" : "M"}${X(p.t).toFixed(1)},${Y(p.v).toFixed(1)}`).join("");
        if (s.area && seg.length > 1) paths += `<path class="${animate ? "reveal-area" : ""}" d="${d}L${X(seg[seg.length - 1].t).toFixed(1)},${Y(0)}L${X(seg[0].t).toFixed(1)},${Y(0)}Z" fill="url(#ga${si}${el.id})"/>`;
        if (seg.length === 1) paths += `<circle cx="${X(seg[0].t)}" cy="${Y(seg[0].v)}" r="2.5" fill="${s.color}"/>`;
        else paths += `<path ${animate && !s.dash ? 'class="draw" pathLength="1"' : ""} d="${d}" fill="none" stroke="${s.color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" ${s.dash ? 'stroke-dasharray="5 5"' : ""}/>`;
      }
    });
    const defs = series.map((s, i) => `<linearGradient id="ga${i}${el.id}" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="${s.color}" stop-opacity=".32"/><stop offset="1" stop-color="${s.color}" stop-opacity="0"/></linearGradient>`).join("");
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(el.dataset.label || "Graphique")}"><defs>${defs}</defs>${g}${paths}<line class="cur" y1="${m.t}" y2="${H - m.b}" stroke="#2a3d63" style="display:none"/></svg><div class="tip"></div>`;
    const svg = el.querySelector("svg"), tip = el.querySelector(".tip"), cur = el.querySelector(".cur");
    const times = [...new Set(pts.map((p) => p.t))].sort((a, b) => a - b);
    svg.addEventListener("pointermove", (ev) => {
      const r = svg.getBoundingClientRect(), px = ((ev.clientX - r.left) / r.width) * W, t = x0 + ((px - m.l) / (W - m.l - m.r)) * (x1 - x0);
      const near = times.reduce((a, b) => (Math.abs(b - t) < Math.abs(a - t) ? b : a), times[0]);
      const rows = series.map((s) => [s, s.data.find((p) => p.t === near)]).filter(([, p]) => p);
      if (!rows.length || Math.abs(near - t) > gapMinutes * 60000) { tip.style.display = "none"; cur.style.display = "none"; return; }
      cur.setAttribute("x1", X(near)); cur.setAttribute("x2", X(near)); cur.style.display = "";
      tip.innerHTML = `<b>${esc(fmtDay(near))}</b>` + rows.map(([s, p]) => `<div><i style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${s.color};margin-right:6px"></i>${esc(s.name)} : <b style="display:inline">${esc(s.fmt(p.v))}</b></div>`).join("");
      tip.style.display = "block";
      tip.style.left = Math.min(Math.max((X(near) / W) * r.width + 14, 0), r.width - 190) + "px"; tip.style.top = "10px";
    });
    svg.addEventListener("pointerleave", () => { tip.style.display = "none"; cur.style.display = "none"; });
  }

  let hourlyDrawn = false;
  function renderHourly(rows) {
    const done = rows.filter((r) => r.is_complete);
    const mk = (key) => done.filter((r) => r[key] != null).map((r) => ({ t: +new Date(r.hour_start), v: r[key] }));
    const yMax = Math.max(0.1, Math.ceil(Math.max(0, ...done.map((r) => Math.max(r.empty_rate || 0, r.full_rate || 0))) * 20) / 20);
    const el = $("#hourly"); el.dataset.label = "Part de stations vides et saturées par heure sur 48 heures";
    lineChart(el, { yMax, yFmt: (v) => pct(v), gapMinutes: 150, animate: !hourlyDrawn && !reduced(), series: [
      { name: "Vides", color: COLORS.empty, data: mk("empty_rate"), fmt: (v) => pct(v, 1) },
      { name: "Saturées", color: COLORS.full, data: mk("full_rate"), fmt: (v) => pct(v, 1) }] });
    if (done.length) hourlyDrawn = true;
    $("#hourlyRows").innerHTML = done.length ? `<table><thead><tr><th scope="col">Heure</th><th scope="col">Vides</th><th scope="col">Saturées</th></tr></thead><tbody>${[...done].reverse().map((r) => `<tr><th scope="row">${esc(fmtDay(r.hour_start))}</th><td>${pct(r.empty_rate, 1)}</td><td>${pct(r.full_rate, 1)}</td></tr>`).join("")}</tbody></table>` : "";
  }

  function renderDist() {
    const c = Object.fromEntries(STATE_ORDER.map((k) => [k, 0]));
    state.stations.forEach((s) => c[stationKind(s)]++);
    const total = state.stations.length || 1;
    $("#c-empty").textContent = c.empty; $("#c-full").textContent = c.full; $("#c-oos").textContent = c.oos;
    let stack = $("#dist .stack");
    if (!stack) {
      $("#dist").innerHTML = `<div class="stack" role="img" aria-label="Répartition des stations par état">${STATE_ORDER.map((k) => `<i data-k="${k}" style="--c:${COLORS[k]}" title="${LABELS[k]}"></i>`).join("")}</div><div class="dist-list">${STATE_ORDER.map((k) => `<div><i style="--c:${COLORS[k]}"></i>${LABELS[k]}<b data-k="${k}"></b></div>`).join("")}</div>`;
      stack = $("#dist .stack");
    }
    for (const k of STATE_ORDER) { stack.querySelector(`i[data-k="${k}"]`).style.flexGrow = c[k] / total; $(`#dist b[data-k="${k}"]`).textContent = c[k]; }
  }

  /* =============================== station detail =============================== */
  const pop = $("#pop");
  function popBody(s) {
    const kind = stationKind(s), fill = s.capacity ? Math.min(1, s.num_bikes_available / s.capacity) : s.fill_rate;
    return { kind, html: `<button class="pop-close" type="button" aria-label="Fermer le détail de la station">${ICON_X}</button>
      <h3>${esc(s.station_name)}</h3>
      <span class="pill" data-k style="--c:${COLORS[kind]}"><i></i>${esc(LABELS[kind])}</span>
      ${s.need_type ? `<span class="pill" style="--c:var(--flow);margin-left:6px"><i></i>${esc(NEED[s.need_type])}</span>` : ""}
      <div class="pop-stats"><div><b data-b>${s.num_bikes_available}</b><span>vélos</span></div><div><b data-d>${s.num_docks_available}</b><span>places</span></div><div><b>${s.capacity ?? "–"}</b><span>capacité</span></div></div>
      <div class="chart" id="stChart" style="padding:0" data-label="Vélos disponibles sur 24 heures"></div>
      <div class="pop-meta" data-meta>Remplissage ${pct(fill)} · ${esc(LABELS[kind])} depuis ${hoursText(s)}${s.priority_rank ? `<br>Priorité n°${s.priority_rank} · score ${Math.round(s.priority_score)} · tendance 30 min ${s.trend_30min == null ? "–" : (s.trend_30min > 0 ? "+" : "") + dec(s.trend_30min) + " vélos"}` : ""}</div>` };
  }
  function closePop() { pop.classList.remove("open"); pop.setAttribute("aria-hidden", "true"); state.selected = null; }
  async function openStation(id, pan = true) {
    const s = state.stations.find((x) => x.station_id === id); if (!s) return;
    const reopening = state.selected !== id;
    state.selected = id;
    if (reopening) {
      const { kind, html } = popBody(s);
      pop.innerHTML = html;
      pop.querySelector(".pop-close").addEventListener("click", closePop);
      pop.classList.add("open"); pop.setAttribute("aria-hidden", "false");
      if (pan) map.flyTo([s.latitude, s.longitude], Math.max(map.getZoom(), 15), { duration: reduced() ? 0 : 0.6 });
      try {
        const rows = await api(`/api/stations/${encodeURIComponent(id)}/series?hours=24`);
        if (state.selected !== id) return;
        const data = rows.map((r) => ({ t: +new Date(r.slot_start), v: r.bikes_available }));
        const cap = s.capacity ? rows.map((r) => ({ t: +new Date(r.slot_start), v: s.capacity })) : [];
        lineChart($("#stChart"), { height: 190, yMax: Math.max(4, s.capacity || 0, ...data.map((d) => d.v)), yFmt: (v) => Math.round(v), gapMinutes: 45, animate: !reduced(),
          series: [{ name: "Vélos", color: COLORS[kind] === COLORS.oos ? "#a8b5cf" : COLORS[kind], data, area: true, fmt: (v) => `${v} vélos` }, ...(cap.length ? [{ name: "Capacité", color: "#8493b0", data: cap, dash: true, fmt: (v) => `${v}` }] : [])] });
        if (!data.length) $("#stChart").innerHTML = `<div class="empty-state">Pas d'historique sur 24 h.</div>`;
      } catch (e) { $("#stChart").innerHTML = `<div class="empty-state">Historique indisponible.</div>`; }
    } else refreshPop(s);
  }
  // Live update of an open panel: change the numbers in place, never rebuild (keeps focus and the chart).
  function refreshPop(s) {
    if (!s) return;
    const { kind, html } = popBody(s), tmp = document.createElement("div"); tmp.innerHTML = html;
    const set = (sel) => { const a = pop.querySelector(sel), b = tmp.querySelector(sel); if (a && b && a.innerHTML !== b.innerHTML) a.innerHTML = b.innerHTML; };
    set("[data-b]"); set("[data-d]"); set("[data-meta]");
    const pill = pop.querySelector(".pill[data-k]"), np = tmp.querySelector(".pill[data-k]");
    if (pill && np && pill.innerHTML !== np.innerHTML) { pill.innerHTML = np.innerHTML; pill.style.setProperty("--c", COLORS[kind]); }
  }
  document.addEventListener("keydown", (e) => { if (e.key === "Escape" && pop.classList.contains("open")) { closePop(); } });

  /* =============================== events =============================== */
  $("#chips").addEventListener("click", (e) => {
    const b = e.target.closest(".chip"); if (!b) return;
    if (b.id === "toggleTransfers") { state.showTransfers = !state.showTransfers; b.setAttribute("aria-pressed", state.showTransfers); syncTransfers(); return; }
    state.filter = b.dataset.f;
    document.querySelectorAll(".chip[data-f]").forEach((x) => x.setAttribute("aria-pressed", x === b));
    syncMarkers(true);
  });
  $("#search").addEventListener("input", (e) => { state.query = e.target.value.trim().toLowerCase(); syncMarkers(!!state.query); });
  $("#search").addEventListener("keydown", (e) => { if (e.key === "Enter") { const s = state.stations.find(visible); if (s) openStation(s.station_id); } });
  $("#p-prio").addEventListener("click", (e) => { const li = e.target.closest("li[data-id]"); if (li) openStation(li.dataset.id); });
  $("#p-trans").addEventListener("click", (e) => {
    const r = e.target.closest(".transfer"); if (!r) return;
    const a = state.stations.find((s) => s.station_id === r.dataset.from), b = state.stations.find((s) => s.station_id === r.dataset.to);
    if (a && b) map.flyToBounds([[a.latitude, a.longitude], [b.latitude, b.longitude]], { padding: [80, 80], maxZoom: 16, duration: reduced() ? 0 : 0.6 });
  });
  $("#refresh").addEventListener("click", () => { const b = $("#refresh"); b.classList.remove("spin"); void b.offsetWidth; b.classList.add("spin"); load(); });

  /* =============================== data flow =============================== */
  const prevKinds = new Map();
  function notifyStateChanges() {
    if (prevKinds.size) {
      const went = { empty: [], full: [] };
      for (const s of state.stations) {
        const k = stationKind(s), before = prevKinds.get(s.station_id);
        if (before && before !== k && (k === "empty" || k === "full")) went[k].push(s.station_name);
      }
      const say = (names, verb, color) => names.length && toast(`${names.length === 1 ? "1 station vient" : names.length + " stations viennent"} de ${verb}`, { detail: names.slice(0, 3).join(", ") + (names.length > 3 ? ` et ${names.length - 3} autre(s)` : ""), color });
      say(went.empty, "se vider", "var(--empty)"); say(went.full, "se saturer", "var(--full)");
    }
    prevKinds.clear(); state.stations.forEach((s) => prevKinds.set(s.station_id, stationKind(s)));
  }

  function renderFooter(o) {
    const noPos = state.stations.filter((s) => !hasPosition(s));
    const heur = Object.keys(o.risk_sources || {}).every((k) => k.startsWith("heuristic"));
    $("#footer").innerHTML = `<div><b>Lecture des scores.</b> ${heur ? "Les priorités reposent sur une <b>heuristique de tendance</b> : aucun modèle prédictif n'est entraîné à ce stade (historique insuffisant)." : "Les priorités combinent heuristique et modèle prédictif."} Les transferts sont des suggestions, pas des ordres.</div>
      <div><b>Historique.</b> ${o.history_since ? `Séries disponibles depuis le ${esc(fmtDay(o.history_since))}.` : "Pas d'historique."} Le champ <code>last_reported</code> du flux GBFS n'est pas fiable : l'axe du temps est l'heure d'interrogation.</div>
      ${noPos.length ? `<div><b>Position invalide.</b> ${noPos.length} station(s) du flux source sont à la position (0, 0) et ne sont pas affichées sur la carte : ${noPos.map((s) => esc(s.station_name)).join(", ")}.</div>` : ""}
      <div>${DEMO ? "<b>Démo figée.</b> Cette page est une capture de données réelles : rien ne s'y met à jour. Le projet complet les met à jour en continu (collecte toutes les 60 s)." : "<b>Fraîcheur.</b> Les stations et les priorités sont mises à jour automatiquement à chaque nouvelle interrogation du flux (toutes les 60 s) ; les séries sur 15 minutes et les taux horaires, plus lourds à calculer, le sont environ toutes les 15 min. L'indicateur en haut montre l'âge réel de la dernière donnée et la cause d'un éventuel retard."} <a href="${HOME}">Retour à la présentation</a>.</div>`;
  }

  function renderAll(o) {
    live.kpis = o; noteServerTime(o);
    renderKpis(o); notifyStateChanges(); syncMarkers(firstRender); syncTransfers();
    renderPriorities(); renderTransferList(); renderDist(); renderFooter(o); renderLiveStatus();
    if (state.selected) { const s = state.stations.find((x) => x.station_id === state.selected); s ? refreshPop(s) : closePop(); }
    firstRender = false;
  }

  function applySnapshot(d) { state.stations = d.stations; state.priorities = d.priorities; state.transfers = d.transfers; live.dbDown = false; renderAll(d.kpis); }
  function applyUpdate(d) {
    const byId = new Map(state.stations.map((s) => [s.station_id, s]));
    for (const s of d.changed) byId.set(s.station_id, { ...byId.get(s.station_id), ...s });
    for (const id of d.removed) byId.delete(id);
    state.stations = [...byId.values()].sort((a, b) => a.station_name.localeCompare(b.station_name));
    if (d.priorities) state.priorities = d.priorities;
    if (d.transfers) state.transfers = d.transfers;
    live.dbDown = false; renderAll(d.kpis);
  }

  function setLink(next) {
    const prev = live.link;
    if (prev === next) return;
    live.link = next;
    if (prev === "stream" && next !== "stream") toast("Connexion temps réel perdue", { detail: "Actualisation toutes les 30 s en attendant.", color: "var(--warn)" });
    else if (next === "stream" && live.hadStream && (prev === "polling" || prev === "offline")) toast("Flux temps réel rétabli", { color: "var(--ok)" });
    if (next === "stream") live.hadStream = true;
  }
  const startPolling = () => { if (!live.pollTimer) live.pollTimer = setInterval(load, 30000); };
  const stopPolling = () => { if (live.pollTimer) { clearInterval(live.pollTimer); live.pollTimer = null; } };

  function connectStream() {
    if (!("EventSource" in window)) { live.link = "polling"; startPolling(); return; }
    const es = new EventSource("/api/stream");
    live.source = es;
    const opened = () => { setLink("stream"); live.retryMs = 3000; stopPolling(); beat(); };
    es.addEventListener("snapshot", (e) => { opened(); applySnapshot(JSON.parse(e.data)); });
    es.addEventListener("update", (e) => { opened(); applyUpdate(JSON.parse(e.data)); });
    es.addEventListener("status", () => { opened(); live.dbDown = true; renderLiveStatus(); });
    es.onerror = () => {
      if (live.link === "stream" || live.link === "connecting") setLink("polling");
      startPolling(); renderLiveStatus();
      if (es.readyState === EventSource.CLOSED) { setTimeout(connectStream, live.retryMs); live.retryMs = Math.min(live.retryMs * 2, 30000); }
    };
  }

  // Full refresh over plain HTTP: first paint, polling fallback, and the refresh button.
  async function load() {
    try {
      const [o, stations, prio, trans, hourly, quality] = await Promise.all([api("/api/overview"), api("/api/stations"), api("/api/priorities?limit=30"), api("/api/transfers?limit=30"), api("/api/hourly?hours=48"), api("/api/quality")]);
      state.stations = stations; state.priorities = prio; state.transfers = trans;
      if (live.link === "offline" || live.link === "connecting") setLink("polling");
      live.dbDown = false;
      renderAll(o); renderQuality(quality); renderHourly(hourly);
    } catch (e) {
      console.error(e);
      if (live.link !== "stream") { setLink("offline"); renderLiveStatus(); }
    }
  }
  // Hourly rates and quality checks are not part of the stream: refresh them on their own.
  async function loadSlow() {
    try { const [hourly, quality] = await Promise.all([api("/api/hourly?hours=48"), api("/api/quality")]); renderQuality(quality); renderHourly(hourly); }
    catch (e) { console.error(e); }
  }

  selectTab(tabs[0], false);
  load();
  if (!DEMO) { connectStream(); setInterval(loadSlow, 60000); setInterval(renderLiveStatus, 1000); }
  else $("#refresh").hidden = true;
})();
