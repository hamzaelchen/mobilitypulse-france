"""Web API against a real PostgreSQL (throw-away schema) and a real uvicorn server.

Covers honest freshness (/api/status, /api/overview) and the SSE stream (/api/stream).
Skipped when the database, FastAPI or uvicorn is not available.
"""

import http.client
import json
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

psycopg = pytest.importorskip("psycopg")
pytest.importorskip("fastapi")
uvicorn = pytest.importorskip("uvicorn")

from common.config import ConfigError, PostgresSettings  # noqa: E402
from helpers import skip_or_fail  # noqa: E402
from live import store  # noqa: E402
from live.state import Reading, apply_readings  # noqa: E402
from loader.migrations import apply_migrations  # noqa: E402
from web import app as web_app  # noqa: E402

MIGRATIONS = Path(__file__).resolve().parents[2] / "sql" / "migrations"


@pytest.fixture(scope="module")
def pg():
    try:
        return PostgresSettings.from_env()
    except ConfigError as exc:
        skip_or_fail(str(exc))


@pytest.fixture
def conn(pg):
    try:
        connection = psycopg.connect(
            host=pg.host, port=pg.port, dbname=pg.database,
            user=pg.user, password=pg.password, connect_timeout=5,
        )  # fmt: skip
    except psycopg.OperationalError as exc:
        skip_or_fail(f"PostgreSQL not reachable: {exc}")
    yield connection
    connection.close()


@pytest.fixture
def schema(conn):
    name = f"it_{uuid.uuid4().hex[:10]}"
    apply_migrations(conn, MIGRATIONS, name)
    yield name
    conn.rollback()
    conn.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    conn.commit()


def web_settings(pg, schema, *, port=None):
    return SimpleNamespace(
        host=pg.host, port=port or pg.port, database=pg.database, user=pg.user,
        password=pg.password, schema=schema, stream_interval=0.2,
    )  # fmt: skip


@pytest.fixture
def server(monkeypatch, pg, schema):
    monkeypatch.setattr(web_app, "_settings", web_settings(pg, schema))
    srv = start_server()
    yield srv
    stop_server(srv)


def start_server():
    config = uvicorn.Config(web_app.app, host="127.0.0.1", port=0, log_level="warning")
    instance = uvicorn.Server(config)
    thread = threading.Thread(target=instance.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not instance.started:
        assert time.time() < deadline, "server did not start"
        time.sleep(0.05)
    port = instance.servers[0].sockets[0].getsockname()[1]
    return SimpleNamespace(port=port, instance=instance, thread=thread)


def stop_server(srv):
    srv.instance.should_exit = True
    srv.thread.join(10)


def get(srv, path):
    connection = http.client.HTTPConnection("127.0.0.1", srv.port, timeout=10)
    connection.request("GET", path)
    response = connection.getresponse()
    body = response.read()
    connection.close()
    return response.status, json.loads(body)


def now_utc():
    return datetime.now(UTC)


def reading(station, bikes, observed_at, capacity=10):
    return Reading(
        source="s", station_id=station, station_name=f"S{station}", latitude=47.2, longitude=-1.55,
        capacity=capacity, bikes=bikes, docks=capacity - bikes, bikes_disabled=0, docks_disabled=0,
        is_installed=True, is_renting=True, is_returning=True, last_reported_at=None,
        observed_at=observed_at,
    )  # fmt: skip


def seed(conn, schema, stations, *, observed_ago, heartbeat_ago=None, error=None):
    now = now_utc()
    rows = apply_readings(
        {}, [reading(s, b, now - timedelta(seconds=observed_ago)) for s, b in stations]
    )
    store.upsert_rows(conn, schema, list(rows.values()), now)
    if heartbeat_ago is not None:
        detail = {"error": error} if error else {}
        store.write_heartbeat(
            conn, schema, "live-updater", now - timedelta(seconds=heartbeat_ago), detail
        )
    conn.commit()


def test_live_data_is_reported_live(conn, schema, server):
    seed(conn, schema, [("1", 3)], observed_ago=30, heartbeat_ago=5)
    status_code, body = get(server, "/api/status")
    assert status_code == 200
    assert body["freshness"] == "live" and body["cause"] is None
    assert 25 < body["data_age_seconds"] < 60 and body["database"] == "ok"


def test_old_data_is_never_presented_as_live(conn, schema, server):
    seed(conn, schema, [("1", 3)], observed_ago=1200, heartbeat_ago=5)
    _, body = get(server, "/api/status")
    assert body["freshness"] == "stale" and body["cause"] == "collection_stopped"


def test_a_silent_updater_is_distinguished_from_a_stopped_collection(conn, schema, server):
    seed(conn, schema, [("1", 3)], observed_ago=1200, heartbeat_ago=600)
    _, body = get(server, "/api/status")
    assert body["freshness"] == "stale" and body["cause"] == "updater_down"


def test_an_updater_error_is_surfaced(conn, schema, server):
    seed(conn, schema, [("1", 3)], observed_ago=400, heartbeat_ago=5, error="ValueError")
    _, body = get(server, "/api/status")
    assert body["cause"] == "updater_error" and body["updater_error"] == "ValueError"


def test_an_empty_database_says_no_data(schema, server):
    _, body = get(server, "/api/status")
    assert body["freshness"] == "unknown" and body["cause"] == "no_data"


def test_overview_carries_the_same_freshness_and_keeps_its_old_fields(conn, schema, server):
    seed(conn, schema, [("1", 0), ("2", 3)], observed_ago=30, heartbeat_ago=5)
    _, body = get(server, "/api/overview")
    assert body["freshness"] == "live" and body["stations"] == 2 and body["empty"] == 1
    assert body["data_age_minutes"] < 2 and "risk_sources" in body and "quality" in body


def test_collection_gaps_of_the_last_day_are_listed(conn, schema, server):
    now = now_utc().replace(second=0, microsecond=0)
    slots = [now - timedelta(hours=h) for h in (5, 4.75, 4.5, 2, 1.75)]  # 2 h 30 gap after 4.5
    for slot in slots:
        conn.execute(
            f"""INSERT INTO {schema}.station_series_15min
                (source, station_id, slot_start, bikes_available, docks_available, is_empty,
                 is_full, is_operational, n_readings, n_changes, is_complete)
                VALUES ('s', '1', %s, 3, 7, false, false, true, 15, 0, true)""",
            (slot,),
        )
    conn.commit()
    _, body = get(server, "/api/status")
    assert len(body["gaps"]) == 1 and body["gaps"][0]["seconds"] == pytest.approx(9000)


def test_status_still_answers_when_the_database_is_down(monkeypatch, pg, schema):
    monkeypatch.setattr(web_app, "_settings", web_settings(pg, schema, port=1))
    srv = start_server()
    try:
        status_code, body = get(srv, "/api/status")
        assert status_code == 200
        assert body["freshness"] == "unknown" and body["cause"] == "database_unavailable"
        assert get(srv, "/api/overview")[0] == 503  # the other endpoints keep failing loudly
    finally:
        stop_server(srv)


def read_events(srv, count, timeout=10):
    """Read ``count`` SSE events (name, data) from /api/stream, then close."""
    connection = http.client.HTTPConnection("127.0.0.1", srv.port, timeout=timeout)
    connection.request("GET", "/api/stream", headers={"Accept": "text/event-stream"})
    response = connection.getresponse()
    assert response.status == 200
    assert response.getheader("content-type").startswith("text/event-stream")
    events, name, data = [], None, []
    deadline = time.time() + timeout
    while len(events) < count and time.time() < deadline:
        line = response.fp.readline().decode().rstrip("\n")
        if line.startswith("event:"):
            name = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            data.append(line.split(":", 1)[1].strip())
        elif line == "" and data:
            events.append((name, json.loads("\n".join(data))))
            name, data = None, []
    connection.close()
    return events


def test_the_stream_starts_with_a_full_snapshot(conn, schema, server):
    seed(conn, schema, [("1", 3), ("2", 0)], observed_ago=10, heartbeat_ago=2)
    ((name, data),) = read_events(server, 1)
    assert name == "snapshot"
    assert {s["station_id"] for s in data["stations"]} == {"1", "2"}
    assert data["kpis"]["freshness"] == "live" and data["kpis"]["stations"] == 2


def test_a_change_in_the_database_is_pushed_as_a_small_update(conn, schema, server):
    seed(conn, schema, [("1", 3), ("2", 5)], observed_ago=10, heartbeat_ago=2)
    collected = []

    def listen():
        collected.extend(read_events(server, 2, timeout=10))

    listener = threading.Thread(target=listen)
    listener.start()
    time.sleep(1.0)  # the snapshot has been delivered
    seed(conn, schema, [("1", 1)], observed_ago=1, heartbeat_ago=1)  # station 1 loses bikes
    listener.join(12)

    assert [name for name, _ in collected] == ["snapshot", "update"]
    update = collected[1][1]
    assert [s["station_id"] for s in update["changed"]] == ["1"]
    assert update["changed"][0]["num_bikes_available"] == 1
    assert update["kpis"]["bikes"] == 6


def test_the_stream_survives_a_client_leaving(conn, schema, server):
    seed(conn, schema, [("1", 3)], observed_ago=10, heartbeat_ago=2)
    read_events(server, 1)
    time.sleep(0.5)
    assert get(server, "/api/status")[0] == 200
    assert web_app.app.state.broadcaster.subscribers == 0
