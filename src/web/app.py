"""Read-only web API + single-page UI for MobilityPulse (serves ``static/``).

It only talks to PostgreSQL through the read-only role, with fixed queries: the only
user-supplied values (station id, limits) are bound as parameters.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
from collections.abc import AsyncIterable, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.sse import EventSourceResponse, ServerSentEvent
from fastapi.staticfiles import StaticFiles
from psycopg import sql
from psycopg.rows import dict_row

from common.logging_utils import configure_logging
from web.status import find_gaps, pipeline_status
from web.stream import DB_DOWN, Broadcaster

logger = logging.getLogger("web")
STATIC_DIR = Path(__file__).parent / "static"
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
GAP_MIN_SECONDS = 1800  # a collection gap is more than 30 minutes without any data
MAX_STREAM_CLIENTS = 50
SNAPSHOT_LIMIT = 30


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing environment variable: {name}")
    return value.strip()


class WebSettings:
    def __init__(self) -> None:
        self.host = _env("POSTGRES_HOST", "localhost")
        self.port = int(_env("POSTGRES_PORT", "5432"))
        self.database = _env("POSTGRES_DB")
        self.user = _env("GRAFANA_DB_USER", "grafana_ro")  # the read-only role
        self.password = _env("GRAFANA_DB_PASSWORD")
        self.schema = _env("POSTGRES_SCHEMA", "mobility")
        if not _IDENTIFIER.match(self.schema):
            raise RuntimeError(f"Invalid POSTGRES_SCHEMA: {self.schema!r}")
        self.stream_interval = float(_env("STREAM_POLL_SECONDS", "2"))
        if self.stream_interval < 0.1:
            raise RuntimeError("STREAM_POLL_SECONDS must be at least 0.1")

    def __repr__(self) -> str:  # never leak the password
        return f"WebSettings(host={self.host!r}, db={self.database!r}, user={self.user!r})"


_settings: WebSettings | None = None


def settings() -> WebSettings:
    global _settings
    if _settings is None:
        _settings = WebSettings()
    return _settings


def table(name: str) -> sql.Identifier:
    return sql.Identifier(settings().schema, name)


@contextmanager
def connect() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    s = settings()
    with psycopg.connect(
        host=s.host,
        port=s.port,
        dbname=s.database,
        user=s.user,
        password=s.password,
        row_factory=dict_row,
        connect_timeout=5,
        options="-c default_transaction_read_only=on -c statement_timeout=5000",
    ) as conn:
        yield conn


def fetch(query: sql.Composable, params: tuple = ()) -> list[dict[str, Any]]:
    try:
        with connect() as conn:
            return conn.execute(query, params).fetchall()
    except psycopg.Error as exc:
        logger.error("Database query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database unavailable") from exc


@contextmanager
def database() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    """A read-only connection; any database failure becomes HTTP 503."""
    try:
        with connect() as conn:
            yield conn
    except psycopg.Error as exc:
        logger.error("Database query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database unavailable") from exc


def _rows(conn, query: sql.Composable, params: tuple = ()) -> list[dict[str, Any]]:
    return conn.execute(query, params).fetchall()


def status_data(conn) -> dict[str, Any]:
    """Freshness of the displayed data, from the data itself and the updater heartbeat."""
    row = _rows(
        conn,
        sql.SQL(
            """
            SELECT now() AS server_time,
                   (SELECT max(observed_at) FROM {c}) AS observed_at,
                   (SELECT beat_at FROM {p} WHERE component = 'live-updater') AS heartbeat_at,
                   (SELECT detail ->> 'error' FROM {p} WHERE component = 'live-updater') AS err
            """
        ).format(c=table("station_current"), p=table("pipeline_status")),
    )[0]
    status = pipeline_status(
        row["server_time"], row["observed_at"], row["heartbeat_at"], row["err"]
    )
    status["server_time"] = row["server_time"]
    return status


def overview_data(conn) -> dict[str, Any]:
    result = _rows(
        conn,
        sql.SQL(
            """
            SELECT count(*)                                         AS stations,
                   count(*) FILTER (WHERE is_operational AND is_empty) AS empty,
                   count(*) FILTER (WHERE is_operational AND is_full)  AS full,
                   count(*) FILTER (WHERE NOT is_operational)          AS out_of_service,
                   coalesce(sum(num_bikes_available), 0)::int          AS bikes,
                   coalesce(sum(num_docks_available), 0)::int          AS docks,
                   avg(fill_rate) FILTER (WHERE is_operational)        AS avg_fill_rate,
                   max(observed_at)                                    AS observed_at,
                   (extract(epoch FROM now() - max(observed_at)) / 60)::float8 AS data_age_minutes
            FROM {t}
            """
        ).format(t=table("station_current")),
    )[0]
    risk = _rows(
        conn,
        sql.SQL(
            "SELECT risk_source, count(*) AS n FROM {t} GROUP BY risk_source ORDER BY n DESC"
        ).format(t=table("rebalancing_priority")),
    )
    quality = _rows(
        conn,
        sql.SQL("SELECT status, count(*) AS n FROM {t} GROUP BY status").format(
            t=table("v_data_quality_latest")
        ),
    )
    series = _rows(
        conn,
        sql.SQL("SELECT min(slot_start) AS first_slot FROM {t}").format(
            t=table("station_series_15min")
        ),
    )
    status = status_data(conn)
    result["risk_sources"] = {r["risk_source"]: r["n"] for r in risk}
    result["quality"] = {r["status"]: r["n"] for r in quality}
    result["history_since"] = series[0]["first_slot"]
    for key in ("freshness", "cause", "data_age_seconds", "heartbeat_age_seconds", "updater_error"):
        result[key] = status[key]
    result["server_time"] = status["server_time"]
    return result


def stations_data(conn) -> list[dict[str, Any]]:
    # hours_in_current_state is rounded to the 0.1 h the UI shows: unrounded, it would change on
    # every live cycle and every station would look "changed" in every stream update.
    return _rows(
        conn,
        sql.SQL(
            """
            SELECT c.station_id, c.station_name, c.latitude, c.longitude, c.capacity,
                   c.num_bikes_available, c.num_docks_available, c.fill_rate,
                   c.is_operational, c.is_empty, c.is_full, c.observed_at,
                   round(c.hours_in_current_state::numeric, 1)::float8 AS hours_in_current_state,
                   c.state_age_is_lower_bound,
                   p.state, p.priority_score, p.priority_rank, p.need_type, p.bikes_gap,
                   p.trend_30min, p.projected_fill, p.risk_source
            FROM {c} c
            LEFT JOIN {p} p ON p.source = c.source AND p.station_id = c.station_id
            ORDER BY c.station_name
            """
        ).format(c=table("station_current"), p=table("rebalancing_priority")),
    )


def priorities_data(conn, limit: int) -> list[dict[str, Any]]:
    return _rows(
        conn,
        sql.SQL(
            """
            SELECT station_id, station_name, state, priority_rank, priority_score, need_type,
                   bikes_gap, planned_in, planned_out, unplanned_gap, num_bikes_available,
                   capacity, fill_rate, projected_fill, trend_30min, hours_in_current_state,
                   state_age_is_lower_bound, risk_source, computed_at
            FROM {t}
            WHERE need_type IS NOT NULL
            ORDER BY priority_rank
            LIMIT %s
            """
        ).format(t=table("rebalancing_priority")),
        (limit,),
    )


def transfers_data(conn, limit: int) -> list[dict[str, Any]]:
    return _rows(
        conn,
        sql.SQL(
            """
            SELECT from_station_id, to_station_id, from_station_name, to_station_name,
                   from_latitude, from_longitude, to_latitude, to_longitude, bikes,
                   distance_km, from_state, to_state, priority_score, transfer_rank, computed_at
            FROM {t}
            ORDER BY transfer_rank
            LIMIT %s
            """
        ).format(t=table("rebalancing_transfers")),
        (limit,),
    )


def read_state() -> dict[str, Any]:
    """Light snapshot for the stream, read in one consistent read-only transaction.

    Raises psycopg errors (the broadcaster turns them into a "database unavailable" status).
    ``observed_at`` is left out of the per-station payload: it changes for every station at
    every poll and is carried once by the KPIs.
    """
    with connect() as conn, conn.transaction():
        kpis = overview_data(conn)
        stations = {
            row["station_id"]: {k: v for k, v in row.items() if k != "observed_at"}
            for row in stations_data(conn)
        }
        priorities = priorities_data(conn, SNAPSHOT_LIMIT)
        transfers = transfers_data(conn, SNAPSHOT_LIMIT)
    return {"kpis": kpis, "stations": stations, "priorities": priorities, "transfers": transfers}


@asynccontextmanager
async def lifespan(app: FastAPI):
    broadcaster = Broadcaster(read_state, interval=settings().stream_interval)
    app.state.broadcaster = broadcaster
    task = asyncio.create_task(broadcaster.run())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="MobilityPulse France", docs_url="/api/docs", redoc_url=None, lifespan=lifespan)


@app.middleware("http")
async def cache_policy(request: Request, call_next):
    """API: never cached. Fonts: one day. Pages, scripts and styles: revalidated (ETag) every time,
    so an update is never hidden behind a stale copy while unchanged files still answer 304."""
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    elif path.startswith("/fonts/"):
        response.headers["Cache-Control"] = "public, max-age=86400"
    elif "Cache-Control" not in response.headers:
        response.headers["Cache-Control"] = "no-cache"
    return response


@app.get("/healthz")
def healthz() -> dict[str, str]:
    fetch(sql.SQL("SELECT 1"))
    return {"status": "ok"}


@app.get("/api/status")
def status() -> dict[str, Any]:
    """Is the displayed data fresh, and if not, why. Answers even when the database is down."""
    try:
        with connect() as conn:
            result = status_data(conn)
            slots = _rows(
                conn,
                sql.SQL(
                    """
                    SELECT DISTINCT slot_start FROM {t}
                    WHERE slot_start > now() - interval '24 hours' ORDER BY slot_start
                    """
                ).format(t=table("station_series_15min")),
            )
    except psycopg.Error as exc:
        logger.error("Status query failed: %s", exc)
        return {**DB_DOWN, "database": "unavailable", "gaps": []}
    result["database"] = "ok"
    result["gaps"] = find_gaps([r["slot_start"] for r in slots], GAP_MIN_SECONDS)
    return result


def stream_slot(request: Request) -> Broadcaster:
    broadcaster: Broadcaster = request.app.state.broadcaster
    if broadcaster.subscribers >= MAX_STREAM_CLIENTS:
        raise HTTPException(status_code=503, detail="Too many live clients")
    return broadcaster


@app.get("/api/stream", response_class=EventSourceResponse)
async def stream(
    broadcaster: Annotated[Broadcaster, Depends(stream_slot)],
) -> AsyncIterable[ServerSentEvent]:
    """Light live snapshot: KPIs + only the stations that changed (see ``web.stream``)."""
    sub = broadcaster.subscribe()
    try:
        yield ServerSentEvent(comment="mobilitypulse stream", retry=3000)
        while True:
            name, data = await sub.queue.get()
            yield ServerSentEvent(data=data, event=name)
    finally:
        broadcaster.unsubscribe(sub)


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    with database() as conn:
        return overview_data(conn)


@app.get("/api/stations")
def stations() -> list[dict[str, Any]]:
    with database() as conn:
        return stations_data(conn)


@app.get("/api/priorities")
def priorities(limit: int = Query(15, ge=1, le=100)) -> list[dict[str, Any]]:
    with database() as conn:
        return priorities_data(conn, limit)


@app.get("/api/transfers")
def transfers(limit: int = Query(20, ge=1, le=100)) -> list[dict[str, Any]]:
    with database() as conn:
        return transfers_data(conn, limit)


@app.get("/api/hourly")
def hourly(hours: int = Query(48, ge=1, le=336)) -> list[dict[str, Any]]:
    return fetch(
        sql.SQL(
            """
            SELECT hour_start, empty_rate, full_rate, non_operational_rate,
                   station_coverage, is_complete
            FROM {t}
            WHERE hour_start > now() - make_interval(hours => %s)
            ORDER BY hour_start
            """
        ).format(t=table("station_hourly_rates")),
        (hours,),
    )


@app.get("/api/stations/{station_id}/series")
def station_series(station_id: str, hours: int = Query(24, ge=1, le=168)) -> list[dict[str, Any]]:
    rows = fetch(
        sql.SQL(
            """
            SELECT slot_start, bikes_available, docks_available, capacity, is_complete
            FROM {t}
            WHERE station_id = %s AND slot_start > now() - make_interval(hours => %s)
            ORDER BY slot_start
            """
        ).format(t=table("station_series_15min")),
        (station_id, hours),
    )
    return rows


@app.get("/api/quality")
def quality() -> list[dict[str, Any]]:
    return fetch(
        sql.SQL(
            """
            SELECT layer, name, status, value, threshold, detail, checked_at
            FROM {t}
            ORDER BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END, layer, name
            """
        ).format(t=table("v_data_quality_latest"))
    )


@app.get("/api/config")
def config() -> dict[str, str | None]:
    """Public, non-secret settings of the UI: the optional repository link (http/https only)."""
    url = os.environ.get("PROJECT_REPO_URL", "").strip()
    return {"repo_url": url if re.match(r"^https?://\S+$", url) else None}


@app.get("/", include_in_schema=False)
def landing() -> FileResponse:
    return FileResponse(STATIC_DIR / "landing.html")


@app.get("/app", include_in_schema=False)
def application() -> FileResponse:
    return FileResponse(STATIC_DIR / "app.html")


# Registered last so that the routes above win.
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


def main() -> None:  # pragma: no cover - convenience entrypoint
    import uvicorn

    configure_logging()
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
