"""Rebalancing job against a real PostgreSQL (throw-away schema per test).

Skipped when no database is reachable.
"""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from common.config import ConfigError, PostgresSettings  # noqa: E402
from helpers import skip_or_fail  # noqa: E402
from loader.gold_loader import SPECS, load_table  # noqa: E402
from loader.migrations import apply_migrations  # noqa: E402
from prescriptive.job import StaleDataError, run  # noqa: E402
from prescriptive.scoring import Params  # noqa: E402

MIGRATIONS = Path(__file__).resolve().parents[2] / "sql" / "migrations"
SPEC = {s.name: s for s in SPECS}
T0 = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
NOW = T0 + timedelta(minutes=5)
LAT, LON = 47.20, -1.55


@pytest.fixture(scope="module")
def settings():
    try:
        return PostgresSettings.from_env()
    except ConfigError as exc:
        skip_or_fail(str(exc))


@pytest.fixture
def conn(settings):
    try:
        connection = psycopg.connect(
            host=settings.host, port=settings.port, dbname=settings.database,
            user=settings.user, password=settings.password, connect_timeout=5,
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


def current_row(station_id, bikes, capacity=20, *, dlat=0.0, operational=True, observed=T0):
    values = {
        "source": "test", "station_id": station_id, "station_name": f"S{station_id}",
        "latitude": LAT + dlat, "longitude": LON, "capacity": capacity,
        "num_bikes_available": bikes, "num_docks_available": capacity - bikes,
        "num_bikes_disabled": 0, "num_docks_disabled": 0, "fill_rate": bikes / capacity,
        "is_empty": bikes == 0, "is_full": bikes == capacity, "is_operational": operational,
        "observed_at": observed, "last_reported_at": None,
        "first_observed_at": T0 - timedelta(hours=2),
        "last_change_at": T0 - timedelta(hours=1), "minutes_since_observed": 0.0,
        "hours_in_current_state": 1.0, "state_age_is_lower_bound": False, "as_of": T0,
    }  # fmt: skip
    return tuple(values[c] for c in SPEC["station_current"].columns)


def series_row(station_id, slot, bikes, capacity=20):
    return ("test", station_id, slot, bikes, capacity - bikes, capacity, bikes / capacity,
            bikes == 0, bikes == capacity, True, bikes, bikes, float(bikes), 15, 0,
            True)  # fmt: skip


def load(conn, schema, name, rows):
    with conn.transaction():
        load_table(conn, schema, SPEC[name], rows)


def stations():
    return [
        current_row("R", 0),  # empty: needs 10 bikes
        current_row("L", 2, dlat=0.004),  # low: needs 8
        current_row("D", 20, dlat=0.006),  # full: 10 spare bikes
        current_row("O", 14, dlat=0.010),  # ok donor: 4 spare bikes
        current_row("X", 20, dlat=0.002, operational=False),  # out of service: never used
    ]


def count(conn, schema, table):
    return conn.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]


def test_job_writes_priorities_and_transfers_that_respect_every_constraint(conn, schema):
    load(conn, schema, "station_current", stations())
    summary = run(conn, schema, Params(), now=NOW)

    assert summary["stations"] == 5 and summary["receivers"] == 2 and summary["needs_docks"] == 1
    assert count(conn, schema, "rebalancing_priority") == 5
    transfers = conn.execute(
        f"SELECT from_station_id, to_station_id, bikes FROM {schema}.rebalancing_transfers "
        "ORDER BY transfer_rank"
    ).fetchall()
    assert transfers and all(t[0] != "X" and t[1] != "X" for t in transfers)

    # No donor gives more than its surplus, no receiver receives more than its gap.
    surplus = {"D": 10, "O": 4}
    gap = {"R": 10, "L": 8}
    given: dict[str, int] = {}
    received: dict[str, int] = {}
    for src, dst, bikes in transfers:
        given[src] = given.get(src, 0) + bikes
        received[dst] = received.get(dst, 0) + bikes
    assert all(given[k] <= surplus[k] for k in given)
    assert all(received[k] <= gap[k] for k in received)
    assert sum(given.values()) == sum(received.values()) == summary["bikes_moved"]

    row = conn.execute(
        f"SELECT state, need_type, bikes_gap, planned_in, unplanned_gap, risk_source "
        f"FROM {schema}.rebalancing_priority WHERE station_id = 'R'"
    ).fetchone()
    assert row[:3] == ("empty", "needs_bikes", 10)
    assert row[3] + row[4] == 10  # planned + unplanned = gap
    assert row[5] == "heuristic_trend"


def test_stored_distances_match_an_independent_haversine_in_sql(conn, schema):
    load(conn, schema, "station_current", stations())
    run(conn, schema, Params(), now=NOW)
    rows = conn.execute(
        f"""
        SELECT distance_km,
               2 * 6371.0088 * asin(sqrt(
                   power(sin(radians(to_latitude - from_latitude) / 2), 2)
                   + cos(radians(from_latitude)) * cos(radians(to_latitude))
                     * power(sin(radians(to_longitude - from_longitude) / 2), 2)))
        FROM {schema}.rebalancing_transfers
        """
    ).fetchall()
    assert rows
    for stored, independent in rows:
        assert stored == pytest.approx(independent, abs=1e-6)
        assert 0 < stored <= Params().max_distance_km


def test_stale_data_is_refused_unless_forced(conn, schema):
    load(conn, schema, "station_current", stations())
    with pytest.raises(StaleDataError, match="min old"):
        run(conn, schema, Params(), now=T0 + timedelta(hours=2))
    assert count(conn, schema, "rebalancing_priority") == 0  # nothing written
    summary = run(conn, schema, Params(), now=T0 + timedelta(hours=2), force=True)
    assert summary["stations"] == 5


def test_empty_station_current_is_refused(conn, schema):
    with pytest.raises(StaleDataError, match="empty"):
        run(conn, schema, Params(), now=NOW)


def test_second_run_is_idempotent_and_the_mirror_drops_vanished_stations(conn, schema):
    load(conn, schema, "station_current", stations())
    first = run(conn, schema, Params(), now=NOW)
    again = run(conn, schema, Params(), now=NOW)
    for table, result in again["loaded"].items():
        assert (result["inserted"], result["updated"], result["deleted"]) == (0, 0, 0), table
    assert again["transfers"] == first["transfers"]

    # A station disappears from the source table: its rows (and its transfers) go away.
    load(conn, schema, "station_current", [s for s in stations() if s[1] != "D"])
    conn.execute(f"DELETE FROM {schema}.station_current WHERE station_id = 'D'")
    conn.commit()
    after = run(conn, schema, Params(), now=NOW)
    assert after["loaded"]["rebalancing_priority"]["deleted"] == 1
    gone = conn.execute(
        f"SELECT count(*) FROM {schema}.rebalancing_transfers "
        "WHERE from_station_id = 'D' OR to_station_id = 'D'"
    ).fetchone()[0]
    assert gone == 0


def test_trend_comes_from_the_last_two_complete_slots_and_goes_stale(conn, schema):
    slot = datetime(2026, 10, 6, 11, 45, tzinfo=UTC)  # last complete slot, 15 min before as_of
    load(conn, schema, "station_current", [current_row("A", 4)])
    load(
        conn,
        schema,
        "station_series_15min",
        [series_row("A", slot - timedelta(minutes=30), 10), series_row("A", slot, 4)],
    )
    summary = run(conn, schema, Params(), now=NOW)
    assert summary["trend_available"] == 1
    trend, projected = conn.execute(
        f"SELECT trend_30min, projected_fill FROM {schema}.rebalancing_priority"
    ).fetchone()
    assert trend == pytest.approx(0.2 - 0.5)  # fill 0.20 now vs 0.50 thirty minutes before
    assert projected == 0.0  # 0.20 - 0.30 clamped to 0

    # The same series, but the pipeline stopped long ago: no trend is used.
    old_as_of = datetime(2026, 10, 6, 14, 0, tzinfo=UTC)
    conn.execute(f"UPDATE {schema}.station_current SET as_of = %s", (old_as_of,))
    conn.commit()
    stale = run(conn, schema, Params(), now=old_as_of + timedelta(minutes=5), force=True)
    assert stale["trend_available"] == 0
    assert (
        conn.execute(f"SELECT trend_30min FROM {schema}.rebalancing_priority").fetchone()[0] is None
    )


def test_check_constraints_protect_the_tables(conn, schema):
    load(conn, schema, "station_current", stations())
    run(conn, schema, Params(), now=NOW)
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(f"UPDATE {schema}.rebalancing_priority SET priority_score = 150")
    conn.rollback()
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(f"UPDATE {schema}.rebalancing_transfers SET bikes = 0")
