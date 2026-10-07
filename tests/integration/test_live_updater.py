"""Live updater against a real PostgreSQL (throw-away schema) and temporary Silver files.

Skipped when no database is reachable (or pyarrow is missing, i.e. on the host).
"""

import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from common.config import ConfigError, PostgresSettings  # noqa: E402
from helpers import skip_or_fail  # noqa: E402
from live.cycle import LiveState, run_cycle  # noqa: E402
from loader.migrations import apply_migrations  # noqa: E402

MIGRATIONS = Path(__file__).resolve().parents[2] / "sql" / "migrations"
T0 = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


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


def silver_row(station="1", minute=0, bikes=3, **overrides):
    row = {
        "source": "s", "station_id": station, "station_name": f"S{station}",
        "latitude": 47.2, "longitude": -1.55, "capacity": 10,
        "num_bikes_available": bikes, "num_docks_available": 10 - bikes,
        "num_bikes_disabled": 0, "num_docks_disabled": 0,
        "is_installed": True, "is_renting": True, "is_returning": True,
        "event_timestamp": T0 - timedelta(hours=5),
        "observed_at": T0 + timedelta(minutes=minute), "kafka_offset": 100 + minute,
    }  # fmt: skip
    row.update(overrides)
    return row


def write_silver(root, name, rows, *, written_at):
    path = root / f"ingestion_date={written_at.astimezone(UTC).date()}" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    stamp = written_at.timestamp()
    os.utime(path, (stamp, stamp))
    return path


def cycle(conn, schema, root, state, now, **kwargs):
    return run_cycle(conn, schema, root, state, now=now, settle_seconds=2, **kwargs)


def current(conn, schema):
    rows = conn.execute(f"SELECT * FROM {schema}.station_current ORDER BY station_id").fetchall()
    names = [d.name for d in conn.execute(f"SELECT * FROM {schema}.station_current").description]
    return {r[1]: dict(zip(names, r, strict=True)) for r in rows}


def test_new_silver_files_create_the_current_state(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=3)
    write_silver(
        tmp_path, "part-a.parquet",
        [silver_row("1", 0), silver_row("2", 0, bikes=0), silver_row("1", 1, bikes=2)],
        written_at=now - timedelta(seconds=30),
    )  # fmt: skip

    result = cycle(conn, schema, tmp_path, LiveState(), now)

    rows = current(conn, schema)
    assert (result.files, result.updated) == (1, 2)
    one, two = rows["1"], rows["2"]
    assert one["num_bikes_available"] == 2 and one["num_docks_available"] == 8
    assert one["observed_at"] == T0 + timedelta(minutes=1)
    assert one["first_observed_at"] == T0
    assert one["last_change_at"] == T0 + timedelta(minutes=1)
    assert one["state_age_is_lower_bound"] is False
    assert one["fill_rate"] == pytest.approx(0.2)
    assert two["is_empty"] is True and two["last_change_at"] is None
    assert two["state_age_is_lower_bound"] is True
    assert two["as_of"] == now
    assert two["minutes_since_observed"] == pytest.approx(3.0)
    assert two["hours_in_current_state"] == pytest.approx(3 / 60)


def test_every_cycle_beats_even_without_new_files(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=3)
    cycle(conn, schema, tmp_path, LiveState(), now)
    beat_at, detail = conn.execute(
        f"SELECT beat_at, detail FROM {schema}.pipeline_status WHERE component = 'live-updater'"
    ).fetchone()
    assert beat_at == now
    assert detail["files"] == 0 and detail["updated"] == 0


def test_replaying_the_same_files_changes_nothing(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=3)
    write_silver(
        tmp_path, "part-a.parquet", [silver_row("1", 0)], written_at=now - timedelta(seconds=30)
    )
    state = LiveState()
    cycle(conn, schema, tmp_path, state, now)
    before = current(conn, schema)

    again = cycle(conn, schema, tmp_path, state, now + timedelta(seconds=10))

    assert again.updated == 0
    assert current(conn, schema)["1"]["updated_at"] == before["1"]["updated_at"]


def test_a_restart_resumes_from_the_database(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=3)
    write_silver(
        tmp_path, "part-a.parquet", [silver_row("1", 0), silver_row("1", 1, bikes=2)],
        written_at=now - timedelta(seconds=30),
    )  # fmt: skip
    cycle(conn, schema, tmp_path, LiveState(), now)

    restarted = LiveState.load(conn, schema)
    write_silver(
        tmp_path, "part-b.parquet", [silver_row("1", 2, bikes=2)],
        written_at=now + timedelta(seconds=30),
    )  # fmt: skip
    cycle(conn, schema, tmp_path, restarted, now + timedelta(minutes=1))

    row = current(conn, schema)["1"]
    assert row["observed_at"] == T0 + timedelta(minutes=2)
    assert row["first_observed_at"] == T0  # kept across the restart
    assert row["last_change_at"] == T0 + timedelta(minutes=1)  # unchanged counters: no new change


def test_an_older_reading_never_overwrites_a_newer_row(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=10)
    write_silver(
        tmp_path,
        "part-new.parquet",
        [silver_row("1", 9, bikes=1)],
        written_at=now - timedelta(seconds=30),
    )
    cycle(conn, schema, tmp_path, LiveState(), now)

    late = write_silver(
        tmp_path,
        "part-late.parquet",
        [silver_row("1", 2, bikes=7)],
        written_at=now + timedelta(seconds=20),
    )
    assert late.exists()
    cycle(conn, schema, tmp_path, LiveState.load(conn, schema), now + timedelta(seconds=60))

    assert current(conn, schema)["1"]["num_bikes_available"] == 1


def test_the_time_columns_follow_the_clock_without_new_data(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=3)
    write_silver(
        tmp_path, "part-a.parquet", [silver_row("1", 0)], written_at=now - timedelta(seconds=30)
    )
    state = LiveState()
    cycle(conn, schema, tmp_path, state, now)

    later = now + timedelta(minutes=30)
    cycle(conn, schema, tmp_path, state, later)

    row = current(conn, schema)["1"]
    assert row["as_of"] == later
    assert row["minutes_since_observed"] == pytest.approx(33.0)
    assert row["hours_in_current_state"] == pytest.approx(0.55)


def test_stations_gone_from_the_feed_are_pruned(conn, schema, tmp_path):
    now = T0 + timedelta(hours=30)
    write_silver(
        tmp_path, "part-a.parquet",
        [silver_row("old", 0), silver_row("live", 29 * 60)],
        written_at=now - timedelta(seconds=30),
    )  # fmt: skip
    result = cycle(conn, schema, tmp_path, LiveState(), now, prune_after=timedelta(hours=24))
    assert set(current(conn, schema)) == {"live"} and result.pruned == 1


def test_a_corrupt_file_does_not_stop_the_cycle(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=3)
    bad = tmp_path / "ingestion_date=2026-10-07" / "part-bad.parquet"
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"garbage")
    os.utime(bad, ((now - timedelta(seconds=30)).timestamp(),) * 2)
    write_silver(
        tmp_path, "part-ok.parquet", [silver_row("1", 0)], written_at=now - timedelta(seconds=20)
    )

    result = cycle(conn, schema, tmp_path, LiveState(), now)

    assert result.updated == 1 and set(current(conn, schema)) == {"1"}


# --- Updater step: cycle + rebalancing + Gold series/hourly -------------------------------

from live.settings import LiveSettings  # noqa: E402
from live.updater import Updater  # noqa: E402


def settings_for(tmp_path):
    return LiveSettings.from_env({"DATA_DIR": str(tmp_path)})


def silver_root(tmp_path):
    return tmp_path / "silver" / "station_status"


def count(conn, schema, table):
    return conn.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]


def test_a_step_with_new_readings_recomputes_the_priorities(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=3)
    write_silver(
        silver_root(tmp_path), "part-a.parquet",
        [silver_row("low", 2, bikes=0, capacity=20, num_docks_available=20),
         silver_row("high", 2, bikes=20, capacity=20, num_docks_available=0, latitude=47.202)],
        written_at=now - timedelta(seconds=30),
    )  # fmt: skip
    updater = Updater(conn, schema, settings_for(tmp_path))

    step = updater.step(now)

    assert step.cycle.updated == 2 and step.rebalanced is True
    assert count(conn, schema, "rebalancing_priority") == 2
    assert count(conn, schema, "rebalancing_transfers") >= 1


def test_a_step_without_new_readings_does_not_recompute(conn, schema, tmp_path):
    now = T0 + timedelta(minutes=3)
    write_silver(
        silver_root(tmp_path), "part-a.parquet", [silver_row("1", 2)],
        written_at=now - timedelta(seconds=30),
    )  # fmt: skip
    updater = Updater(conn, schema, settings_for(tmp_path))
    updater.step(now)

    again = updater.step(now + timedelta(seconds=10))

    assert again.cycle.updated == 0 and again.rebalanced is False


def test_old_data_is_never_turned_into_recommendations(conn, schema, tmp_path):
    now = T0 + timedelta(hours=3)
    write_silver(
        silver_root(tmp_path), "part-a.parquet", [silver_row("1", 0)],
        written_at=now - timedelta(seconds=30),
    )  # fmt: skip
    step = Updater(conn, schema, settings_for(tmp_path)).step(now)

    assert step.cycle.updated == 1  # shown, flagged stale by the API
    assert step.rebalanced is False and "stale" in step.rebalance_error.lower()
    assert count(conn, schema, "rebalancing_priority") == 0


def test_a_missing_gold_directory_is_not_an_error(conn, schema, tmp_path):
    step = Updater(conn, schema, settings_for(tmp_path)).step(T0)
    assert step.gold_loaded is False


def test_new_gold_files_load_series_and_hourly_but_not_station_current(conn, schema, tmp_path):
    from test_postgres_loader import current_row, hourly_row, series_row, write_gold

    gold = tmp_path / "gold"
    write_gold(
        gold,
        current=[current_row("9")],
        series=[series_row("1", datetime(2026, 10, 6, 10, 0))],
        hourly=[hourly_row()],
    )
    updater = Updater(conn, schema, settings_for(tmp_path))

    first = updater.step(T0)
    second = updater.step(T0 + timedelta(seconds=10))

    assert first.gold_loaded is True and second.gold_loaded is False  # same files: nothing new
    assert count(conn, schema, "station_series_15min") == 1
    assert count(conn, schema, "station_hourly_rates") == 1
    assert count(conn, schema, "station_current") == 0  # owned by the live path
