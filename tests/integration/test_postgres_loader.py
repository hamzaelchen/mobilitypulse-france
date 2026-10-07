"""Loader against a real PostgreSQL (run via ``docker compose run --rm loader pytest ...``).

Each test works in its own throw-away schema. Skipped when no database is reachable.
"""

import shutil
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from common.config import ConfigError, PostgresSettings  # noqa: E402
from helpers import skip_or_fail  # noqa: E402
from loader.gold_loader import SPECS, load_gold, load_table, read_gold_rows  # noqa: E402
from loader.main import ensure_reader_role  # noqa: E402
from loader.migrations import MigrationError, apply_migrations  # noqa: E402

MIGRATIONS = Path(__file__).resolve().parents[2] / "sql" / "migrations"
ALL_VERSIONS = [
    "001_gold_tables",
    "002_station_state_view",
    "003_gold_from_observations",
    "004_rebalancing",
    "005_data_quality",
    "006_pipeline_status",
]
SPEC = {s.name: s for s in SPECS}
T0 = datetime(2026, 10, 6, 10, 0)  # naive on purpose: Spark/pyarrow often return naive UTC


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
    yield name
    conn.rollback()
    conn.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    conn.commit()


def current_row(station_id="1", bikes=3, operational=True, **overrides):
    values = {
        "source": "test", "station_id": station_id, "station_name": f"S{station_id}",
        "latitude": 47.2, "longitude": -1.55, "capacity": 10, "num_bikes_available": bikes,
        "num_docks_available": 10 - bikes, "num_bikes_disabled": 0, "num_docks_disabled": 0,
        "fill_rate": bikes / 10, "is_empty": bikes == 0, "is_full": bikes == 10,
        "is_operational": operational, "observed_at": T0, "last_reported_at": None,
        "first_observed_at": T0 - timedelta(hours=1), "last_change_at": T0,
        "minutes_since_observed": 1.5, "hours_in_current_state": 1.0,
        "state_age_is_lower_bound": False, "as_of": T0,
    }  # fmt: skip
    values.update(overrides)
    return tuple(values[c] for c in SPEC["station_current"].columns)


def write_gold(root: Path, current, series, hourly):
    """Parquet in the Gold layout (hive partitions), types like the Spark output."""
    for name, rows, part in [
        ("station_current", current, None),
        ("station_series_15min", series, "slot_date"),
        ("station_hourly_rates", hourly, "hour_date"),
    ]:
        columns = SPEC[name].columns
        table = pa.table({c: [r[i] for r in rows] for i, c in enumerate(columns)})
        target = root / name
        if part:
            target = target / f"{part}=2026-10-06"
        target.mkdir(parents=True)
        pq.write_table(table, target / "part-0.parquet")


def series_row(station_id="1", slot=T0, bikes=3):
    return ("test", station_id, slot, bikes, 10 - bikes, 10, bikes / 10, bikes == 0, False,
            True, bikes, bikes, float(bikes), 15, 1, True)  # fmt: skip


def hourly_row(hour=T0, coverage=1.0):
    return ("test", hour, 5, coverage, 20, 19, 0.1, 0.05, 0.05, True)


def count(conn, schema, table):
    return conn.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]


def test_migrations_apply_once_and_are_checksum_protected(conn, schema, tmp_path):
    assert apply_migrations(conn, MIGRATIONS, schema) == ALL_VERSIONS
    assert apply_migrations(conn, MIGRATIONS, schema) == []  # idempotent

    local = tmp_path / "migrations"
    shutil.copytree(MIGRATIONS, local)
    (local / "002_station_state_view.sql").write_text("-- edited\nSELECT 1;\n")
    with pytest.raises(MigrationError, match="modified after being applied"):
        apply_migrations(conn, local, schema)
    conn.rollback()

    (local / "002_station_state_view.sql").unlink()
    with pytest.raises(MigrationError, match="missing from disk"):
        apply_migrations(conn, local, schema)


def test_migration_003_discards_the_old_gold_content(conn, schema, tmp_path):
    """Upgrade path: hourly rows computed with the old semantics must not survive."""
    old = tmp_path / "old"
    old.mkdir()
    for name in ("001_gold_tables.sql", "002_station_state_view.sql"):
        shutil.copy(MIGRATIONS / name, old / name)
    apply_migrations(conn, old, schema)
    conn.execute(
        f"INSERT INTO {schema}.station_hourly_rates (source, hour_start, stations_observed, "
        "station_coverage, slot_observations, operational_observations, non_operational_rate, "
        "is_complete) VALUES ('test', now(), 1, 1, 1, 1, 0, true)"
    )
    conn.commit()
    assert count(conn, schema, "station_hourly_rates") == 1

    assert apply_migrations(conn, MIGRATIONS, schema) == ALL_VERSIONS[2:]  # 003 and later
    assert count(conn, schema, "station_hourly_rates") == 0
    columns = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'station_current'",
            (schema,),
        )
    }
    assert {"observed_at", "hours_in_current_state"} <= columns
    assert "event_timestamp" not in columns


def test_upsert_is_idempotent_and_counts_changes(conn, schema):
    apply_migrations(conn, MIGRATIONS, schema)
    spec = SPEC["station_current"]
    rows = [current_row("1"), current_row("2", bikes=0)]

    with conn.transaction():
        first = load_table(conn, schema, spec, rows)
    assert (first.rows_read, first.inserted, first.updated, first.unchanged) == (2, 2, 0, 0)

    with conn.transaction():
        again = load_table(conn, schema, spec, rows)
    assert (again.inserted, again.updated, again.unchanged, again.deleted) == (0, 0, 2, 0)

    changed = [current_row("1", bikes=7), current_row("2", bikes=0), current_row("3")]
    with conn.transaction():
        third = load_table(conn, schema, spec, changed)
    assert (third.inserted, third.updated, third.unchanged) == (1, 1, 1)
    assert count(conn, schema, "station_current") == 3
    bikes = conn.execute(
        f"SELECT num_bikes_available FROM {schema}.station_current WHERE station_id = '1'"
    ).fetchone()[0]
    assert bikes == 7


def test_unchanged_rows_keep_their_updated_at(conn, schema):
    apply_migrations(conn, MIGRATIONS, schema)
    spec = SPEC["station_current"]
    with conn.transaction():
        load_table(conn, schema, spec, [current_row("1")])
    before = conn.execute(f"SELECT updated_at FROM {schema}.station_current").fetchone()[0]
    with conn.transaction():
        load_table(conn, schema, spec, [current_row("1")])
    after = conn.execute(f"SELECT updated_at FROM {schema}.station_current").fetchone()[0]
    assert before == after


def test_rows_missing_from_gold_are_deleted(conn, schema):
    apply_migrations(conn, MIGRATIONS, schema)
    spec = SPEC["station_current"]
    with conn.transaction():
        load_table(conn, schema, spec, [current_row("1"), current_row("2"), current_row("3")])
    with conn.transaction():
        result = load_table(conn, schema, spec, [current_row("1"), current_row("3")])
    assert (result.inserted, result.updated, result.deleted) == (0, 0, 1)
    ids = {r[0] for r in conn.execute(f"SELECT station_id FROM {schema}.station_current")}
    assert ids == {"1", "3"}


def test_deletion_is_limited_to_the_loaded_time_range(conn, schema):
    apply_migrations(conn, MIGRATIONS, schema)
    spec = SPEC["station_series_15min"]
    old, recent = datetime(2026, 10, 1, 10), datetime(2026, 10, 6, 10)
    with conn.transaction():
        load_table(
            conn, schema, spec,
            [series_row("1", old), series_row("1", recent), series_row("2", recent)],
        )  # fmt: skip
    # A partial reload (only the recent days) that no longer contains station 2.
    with conn.transaction():
        result = load_table(conn, schema, spec, [series_row("1", recent)], since=date(2026, 10, 5))
    assert result.deleted == 1  # station 2's recent row; the old row is out of the range
    slots = {
        (r[0], r[1].date())
        for r in conn.execute(f"SELECT station_id, slot_start FROM {schema}.station_series_15min")
    }
    assert slots == {("1", date(2026, 10, 1)), ("1", date(2026, 10, 6))}


def test_an_empty_gold_dataset_deletes_nothing(conn, schema):
    apply_migrations(conn, MIGRATIONS, schema)
    spec = SPEC["station_current"]
    with conn.transaction():
        load_table(conn, schema, spec, [current_row("1")])
    with conn.transaction():
        result = load_table(conn, schema, spec, [])
    assert (result.rows_read, result.deleted) == (0, 0)
    assert count(conn, schema, "station_current") == 1


def test_duplicate_keys_in_the_input_are_rejected(conn, schema):
    apply_migrations(conn, MIGRATIONS, schema)
    with pytest.raises(ValueError, match="duplicated primary keys"), conn.transaction():
        load_table(conn, schema, SPEC["station_current"], [current_row("1"), current_row("1")])


def test_check_constraints_reject_impossible_values(conn, schema):
    apply_migrations(conn, MIGRATIONS, schema)
    bad = current_row("1", num_bikes_available=-1)
    with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
        load_table(conn, schema, SPEC["station_current"], [bad])
    assert count(conn, schema, "station_current") == 0


def test_load_gold_from_parquet_end_to_end(conn, schema, tmp_path):
    apply_migrations(conn, MIGRATIONS, schema)
    write_gold(
        tmp_path,
        current=[current_row("1"), current_row("2", bikes=0), current_row("3", bikes=10),
                 current_row("4", bikes=0, operational=False)],
        series=[series_row("1", T0), series_row("1", datetime(2026, 10, 6, 10, 15), bikes=4)],
        hourly=[hourly_row()],
    )  # fmt: skip
    results = load_gold(conn, schema, tmp_path)
    assert {r.table: r.inserted for r in results} == {
        "station_current": 4, "station_series_15min": 2, "station_hourly_rates": 1,
    }  # fmt: skip

    states = dict(
        conn.execute(f"SELECT station_id, critical_type FROM {schema}.v_station_state").fetchall()
    )
    assert states == {"1": None, "2": "empty", "3": "full", "4": "out_of_service"}
    stamp = conn.execute(f"SELECT as_of FROM {schema}.station_current LIMIT 1").fetchone()[0]
    assert stamp == T0.replace(tzinfo=UTC)  # naive Parquet timestamps are read as UTC
    assert count(conn, schema, "load_log") == 3

    second = load_gold(conn, schema, tmp_path)
    assert [(r.inserted, r.updated, r.deleted) for r in second] == [(0, 0, 0)] * 3  # idempotent
    assert count(conn, schema, "load_log") == 6


def test_load_gold_is_atomic(conn, schema, tmp_path):
    apply_migrations(conn, MIGRATIONS, schema)
    write_gold(tmp_path, [current_row("1")], [series_row()], [hourly_row()])
    shutil.rmtree(tmp_path / "station_hourly_rates")  # last dataset missing
    with pytest.raises(FileNotFoundError):
        load_gold(conn, schema, tmp_path)
    assert count(conn, schema, "station_current") == 0
    assert count(conn, schema, "load_log") == 0


def test_since_filters_old_partitions(tmp_path):
    columns = SPEC["station_series_15min"].columns
    rows = [series_row("1", T0), series_row("2", datetime(2026, 10, 1, 10, 0))]
    for day, row in [("2026-10-06", rows[0]), ("2026-10-01", rows[1])]:
        target = tmp_path / "station_series_15min" / f"slot_date={day}"
        target.mkdir(parents=True)
        pq.write_table(pa.table({c: [row[i]] for i, c in enumerate(columns)}), target / "p.parquet")
    spec = SPEC["station_series_15min"]
    assert len(read_gold_rows(tmp_path, spec)) == 2
    assert [r[1] for r in read_gold_rows(tmp_path, spec, since=T0.date())] == ["1"]


def test_missing_parquet_column_is_reported(tmp_path):
    target = tmp_path / "station_hourly_rates" / "hour_date=2026-10-06"
    target.mkdir(parents=True)
    pq.write_table(pa.table({"source": ["x"], "hour_start": [T0]}), target / "p.parquet")
    with pytest.raises(ValueError, match="columns missing"):
        read_gold_rows(tmp_path, SPEC["station_hourly_rates"])


def test_reader_role_can_select_but_not_write(conn, settings, schema):
    apply_migrations(conn, MIGRATIONS, schema)
    with conn.transaction():
        load_table(conn, schema, SPEC["station_current"], [current_row("1")])
    role = f"ro_{uuid.uuid4().hex[:8]}"
    reader_settings = PostgresSettings(
        host=settings.host, port=settings.port, database=settings.database,
        user=settings.user, password=settings.password, schema=schema,
        reader_user=role, reader_password="pw-" + uuid.uuid4().hex,
    )  # fmt: skip
    try:
        assert ensure_reader_role(conn, reader_settings) is True
        conn.commit()
        with psycopg.connect(
            host=settings.host, port=settings.port, dbname=settings.database,
            user=role, password=reader_settings.reader_password,
        ) as ro:  # fmt: skip
            assert ro.execute(f"SELECT count(*) FROM {schema}.v_station_state").fetchone()[0] == 1
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                ro.execute(f"DELETE FROM {schema}.station_current")
    finally:
        conn.rollback()
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"DROP OWNED BY {role}")
        conn.execute(f"DROP ROLE IF EXISTS {role}")
        conn.commit()
