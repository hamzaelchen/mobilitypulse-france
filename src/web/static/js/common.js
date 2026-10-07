/* Shared helpers: formatting, state vocabulary, motion primitives. Exposes window.MP. */
(function () {
  "use strict";

  const STATE_COLORS = { empty: "#e66767", full: "#9085e9", low: "#c98500", high: "#3987e5", ok: "#199e70", oos: "#6b7a90" };
  const STATE_LABELS = { empty: "Vide", full: "Saturée", low: "Faible", high: "Élevée", ok: "Normale", oos: "Hors service", unknown: "Capacité inconnue" };
  const STATE_ORDER = ["empty", "full", "low", "high", "ok", "oos"];
  const TZ = "Europe/Paris";

  const mq = window.matchMedia ? window.matchMedia("(prefers-reduced-motion: reduce)") : null;
  const reduced = () => !!(mq && mq.matches);

  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const nf = new Intl.NumberFormat("fr-FR");
  const fmtTime = (d) => new Intl.DateTimeFormat("fr-FR", { timeZone: TZ, hour: "2-digit", minute: "2-digit" }).format(new Date(d));
  const fmtTimeSec = (d) => new Intl.DateTimeFormat("fr-FR", { timeZone: TZ, hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date(d));
  const fmtDay = (d) => new Intl.DateTimeFormat("fr-FR", { timeZone: TZ, weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }).format(new Date(d));
  const pct = (x, d = 0) => (x == null ? "–" : (x * 100).toFixed(d).replace(".", ",") + " %");
  const dec = (x, d = 1) => (x == null ? "–" : Number(x).toFixed(d).replace(".", ","));
  const agoSec = (s) => s < 5 ? "à l'instant" : s < 90 ? `il y a ${Math.round(s)} s` : s < 5400 ? `il y a ${Math.round(s / 60)} min` : `il y a ${dec(s / 3600)} h`;

  // "1 vélo", "2 vélos": French uses the singular for 0 and 1. Counts use the locale's separators.
  const plural = (n, one, many) => `${nf.format(n)} ${Math.abs(n) < 2 ? one : many}`;
  // Time spent in a state. The unit never splits from its number; long spans read as days.
  const hoursLabel = (h, lowerBound) => (h == null ? "–" : `${lowerBound ? "≥ " : ""}${h >= 48 ? Math.round(h / 24) + " j" : dec(h) + " h"}`);

  // Station state shown on the map: counters first, then the heuristic level.
  function stationKind(s) {
    if (!s.is_operational) return "oos";
    if (s.is_empty) return "empty";
    if (s.is_full) return "full";
    return ["low", "high", "ok"].includes(s.state) ? s.state : "ok";
  }

  const easeOut = (t) => 1 - Math.pow(1 - t, 4);

  // Tween a number. Jumps straight to the end under reduced motion. Returns a cancel function.
  function tween(from, to, ms, onFrame, onDone) {
    if (reduced() || from === to || !isFinite(from)) { onFrame(to); if (onDone) onDone(); return () => {}; }
    const t0 = performance.now();
    let raf = requestAnimationFrame(function step(now) {
      const t = Math.min(1, (now - t0) / ms);
      onFrame(from + (to - from) * easeOut(t));
      if (t < 1) raf = requestAnimationFrame(step); else if (onDone) onDone();
    });
    return () => cancelAnimationFrame(raf);
  }

  // Animate the number shown in `el`, starting from whatever it currently shows.
  function countTo(el, to, { ms = 700, format = (v) => nf.format(Math.round(v)) } = {}) {
    if (el._cancel) el._cancel();
    const from = Number(el.dataset.value ?? NaN);
    el.dataset.value = to;
    el._cancel = tween(isNaN(from) ? 0 : from, to, ms, (v) => { el.textContent = format(v); });
  }

  // Frozen demo: window.MP_DEMO is defined by the static build. API calls then read the snapshot
  // files ("/api/stations/12/series?hours=24" -> "<base>api/stations/12/series.json").
  const DEMO = window.MP_DEMO || null;

  async function getJson(path) {
    const url = DEMO ? DEMO.base + path.split("?")[0].replace(/^\//, "") + ".json" : path;
    const r = await fetch(url, DEMO ? {} : { cache: "no-store" });
    if (!r.ok) throw new Error(url + " → HTTP " + r.status);
    return r.json();
  }

  window.MP = { DEMO, STATE_COLORS, STATE_LABELS, STATE_ORDER, TZ, reduced, esc, nf, fmtTime, fmtTimeSec, fmtDay, pct, dec, agoSec, plural, hoursLabel, stationKind, easeOut, tween, countTo, getJson };
})();
