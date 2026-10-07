"""Serving-layer quality checks against a real PostgreSQL (throw-away schema per test)."""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from common.config import ConfigError, PostgresSettings  # noqa: E402
from helpers import skip_or_fail  # noqa: E402
from loader.gold_loader import SPECS, LoadResult, load_table, record_load  # noqa: E402
from loader.migrations import apply_migrations  # noqa: E402
from prescriptive.job import run as run_rebalancing  # noqa: E402
from prescriptive.scoring import Params  # noqa: E402
from quality.core import FAIL, PASS, WARN, CheckResult, to_json  # noqa: E402
from quality.run import run as run_quality  # noqa: E402
from quality.serving_checks import run_serving_checks  # noqa: E402

MIGRATIONS = Path(__file__).resolve().parents[2] / "sql" / "migrations"
SPEC = {s.name: s for s in SPECS}
NOW = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
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


def current_row(station_id, bikes, *, capacity=20, observed=None, operational=True, docks=None):
    observed = observed or NOW - timedelta(minutes=1)
    docks = capacity - bikes if docks is None else docks
    as_of = NOW - timedelta(seconds=90)
    values = {
        "source": "t", "station_id": station_id, "station_name": f"S{station_id}",
        "latitude": LAT + int(station_id) * 0.002, "longitude": LON, "capacity": capacity,
        "num_bikes_available": bikes, "num_docks_available": docks,
        "num_bikes_disabled": 0, "num_docks_disabled": 0, "fill_rate": bikes / capacity,
        "is_empty": bikes == 0, "is_full": docks == 0, "is_operational": operational,
        "observed_at": observed, "last_reported_at": None,
        "first_observed_at": NOW - timedelta(hours=3),
        "last_change_at": None, "minutes_since_observed": (as_of - observed).total_seconds() / 60,
        "hours_in_current_state": 3.0, "state_age_is_lower_bound": True, "as_of": as_of,
    }  # fmt: skip
    return tuple(values[c] for c in SPEC["station_current"].columns)


def series_row(station_id, slot, bikes=5, *, n_readings=15, capacity=20):
    return ("t", station_id, slot, bikes, capacity - bikes, capacity, bikes / capacity,
            bikes == 0, bikes == capacity, True, bikes, bikes, float(bikes), n_readings, 0,
            True)  # fmt: skip


def hourly_row(hour, observations):
    return ("t", hour, 4, 1.0, observations, observations, 0.0, 0.0, 0.0, True)


def load(conn, schema, name, rows):
    with conn.transaction():
        load_table(conn, schema, SPEC[name], rows)


def build_healthy(conn, schema, *, slots=None):
    stations = [
        current_row("1", 0),
        current_row("2", 2),
        current_row("3", 10),
        current_row("4", 14),
    ]
    load(conn, schema, "station_current", stations)
    slots = slots or [NOW - timedelta(minutes=15 * k) for k in (4, 3, 2, 1)]  # 11:00 .. 11:45
    load(
        conn,
        schema,
        "station_series_15min",
        [series_row(s[1], slot) for s in stations for slot in slots],
    )
    load(
        conn, schema, "station_hourly_rates", [hourly_row(NOW - timedelta(hours=1), 4 * len(slots))]
    )
    with conn.transaction():
        record_load(conn, schema, LoadResult("station_current", 4, 4, 0))
        conn.execute(f"UPDATE {schema}.load_log SET loaded_at = %s", (NOW - timedelta(minutes=5),))
    run_rebalancing(conn, schema, Params(), now=NOW, force=True)


def by_name(results):
    return {r.name: r for r in results}


def test_healthy_data_passes_every_check(conn, schema):
    build_healthy(conn, schema)
    results = by_name(run_serving_checks(conn, schema, NOW))
    assert {n: r.status for n, r in results.items() if r.status != PASS} == {}, results
    assert results["serving_freshness_minutes"].value == pytest.approx(1.0)
    assert results["readings_per_slot_ratio_24h"].value == pytest.approx(1.0)
    assert results["stations_vs_history_ratio"].value == 1.0


def test_stale_data_fails_the_freshness_check(conn, schema):
    build_healthy(conn, schema)
    conn.execute(
        f"UPDATE {schema}.station_current SET observed_at = %s", (NOW - timedelta(hours=2),)
    )
    conn.commit()
    results = by_name(run_serving_checks(conn, schema, NOW))
    assert results["serving_freshness_minutes"].status == FAIL
    assert results["serving_freshness_minutes"].value == pytest.approx(120.0)


def test_a_collection_hole_is_a_warning_with_its_size(conn, schema):
    slots = [NOW - timedelta(minutes=m) for m in (225, 210, 60, 45)]  # 90-minute hole
    build_healthy(conn, schema, slots=slots)
    result = by_name(run_serving_checks(conn, schema, NOW))["slot_gap_max_minutes_24h"]
    assert result.status == WARN and result.value == pytest.approx(150.0)
    assert "1 gap(s) > 30 min" in result.detail


def test_incomplete_slots_lower_the_readings_ratio(conn, schema):
    slots = [NOW - timedelta(minutes=15 * k) for k in (4, 3, 2, 1)]
    build_healthy(conn, schema, slots=slots)
    conn.execute(f"UPDATE {schema}.station_series_15min SET n_readings = 6")  # 6 of 15
    conn.commit()
    result = by_name(run_serving_checks(conn, schema, NOW))["readings_per_slot_ratio_24h"]
    assert result.status == FAIL and result.value == pytest.approx(6 / 15)


def test_physically_impossible_counters_are_flagged(conn, schema):
    build_healthy(conn, schema)
    conn.execute(
        f"UPDATE {schema}.station_current SET num_docks_available = 30 WHERE station_id = '1'"
    )
    conn.commit()
    results = by_name(run_serving_checks(conn, schema, NOW))
    assert results["capacity_overflow_share"].value == pytest.approx(0.25)  # 1 of 4
    assert results["capacity_overflow_share"].status == FAIL  # beyond the 5 % threshold


def test_future_slots_fail(conn, schema):
    build_healthy(conn, schema)
    load(conn, schema, "station_series_15min", [series_row("1", NOW + timedelta(hours=1))])
    assert by_name(run_serving_checks(conn, schema, NOW))["future_slots"].status == FAIL


def test_stale_recommendations_and_broken_totals_are_detected(conn, schema):
    build_healthy(conn, schema)
    conn.execute(
        f"UPDATE {schema}.rebalancing_priority SET computed_at = computed_at - interval '3 hours'"
    )
    conn.execute(
        f"UPDATE {schema}.rebalancing_priority SET planned_in = planned_in + 1 "
        "WHERE station_id = '1'"
    )
    conn.commit()
    results = by_name(run_serving_checks(conn, schema, NOW))
    assert results["rebalancing_alignment_minutes"].status == FAIL
    assert results["rebalancing_integrity_difference"].status == FAIL
    assert results["rebalancing_integrity_difference"].value >= 1


def test_missing_rebalancing_output_is_a_warning_not_a_pass(conn, schema):
    load(conn, schema, "station_current", [current_row("1", 5)])
    results = by_name(run_serving_checks(conn, schema, NOW))
    assert results["rebalancing_alignment_minutes"].status == WARN
    assert "not computable" in results["rebalancing_alignment_minutes"].detail
    assert results["rebalancing_integrity_difference"].status == WARN


def test_a_check_that_cannot_run_does_not_stop_the_others(conn, schema):
    build_healthy(conn, schema)
    conn.execute(f"DROP VIEW IF EXISTS {schema}.v_data_quality_latest")
    conn.execute(f"DROP TABLE {schema}.rebalancing_transfers")  # breaks two checks only
    conn.commit()
    results = by_name(run_serving_checks(conn, schema, NOW))
    errors = {n: r for n, r in results.items() if "check error" in r.detail}
    # The failing check keeps its own stable name (so that the history stays aligned).
    assert set(errors) == {"rebalancing_integrity_difference"}
    assert errors["rebalancing_integrity_difference"].status == WARN
    assert errors["rebalancing_integrity_difference"].threshold == "fail > 0"
    assert results["serving_freshness_minutes"].status == PASS  # the others still ran
    assert len(results) == 14


def test_run_stores_results_and_the_latest_view_shows_only_the_last_run(conn, schema, tmp_path):
    build_healthy(conn, schema)
    run_quality(conn, schema, now=NOW, lake_report=None)
    later = NOW + timedelta(minutes=10)
    run_id, results = run_quality(conn, schema, now=later, lake_report=None)
    total = conn.execute(f"SELECT count(*) FROM {schema}.data_quality_results").fetchone()[0]
    latest = conn.execute(f"SELECT DISTINCT run_id FROM {schema}.v_data_quality_latest").fetchall()
    assert total == 2 * len(results) and latest == [(run_id,)]


def test_old_results_are_purged(conn, schema):
    build_healthy(conn, schema)
    run_quality(conn, schema, now=NOW - timedelta(days=40), lake_report=None, keep_days=30)
    run_id, results = run_quality(conn, schema, now=NOW, lake_report=None, keep_days=30)
    rows = conn.execute(f"SELECT DISTINCT run_id FROM {schema}.data_quality_results").fetchall()
    assert rows == [(run_id,)]


def test_lake_report_is_merged_and_its_age_is_checked(conn, schema, tmp_path):
    build_healthy(conn, schema)
    report = tmp_path / "lake_latest.json"

    _, missing = run_quality(conn, schema, now=NOW, lake_report=report)
    assert by_name(missing)["lake_report_age_minutes"].status == WARN

    lake = [CheckResult("lake", "silver_rows_not_in_bronze", PASS, 0.0, "fail > 0")]
    report.write_text(to_json(lake, checked_at=(NOW - timedelta(minutes=10)).isoformat()))
    _, fresh = run_quality(conn, schema, now=NOW, lake_report=report)
    merged = by_name(fresh)
    assert merged["lake_report_age_minutes"].status == PASS
    assert merged["silver_rows_not_in_bronze"].layer == "lake"

    report.write_text(to_json(lake, checked_at=(NOW - timedelta(hours=3)).isoformat()))
    _, stale = run_quality(conn, schema, now=NOW, lake_report=report)
    assert by_name(stale)["lake_report_age_minutes"].status == WARN
