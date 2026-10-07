"""Gold aggregates on hand-built Silver readings. Need Java: run inside the Spark image."""

from datetime import UTC, datetime

import pytest

pytest.importorskip("pyspark")

from streaming.gold import (  # noqa: E402
    current_state,
    floor_datetime,
    hourly_rates,
    station_series_15min,
    with_change_flag,
)
from streaming.schemas import (  # noqa: E402
    GOLD_HOURLY_RATES_SCHEMA,
    GOLD_SERIES_15MIN_SCHEMA,
    GOLD_STATION_CURRENT_SCHEMA,
    SILVER_SCHEMA,
)

AS_OF = datetime(2026, 10, 6, 11, 20, tzinfo=UTC)


def utc(value):
    """Spark returns naive datetimes (in the session timezone, forced to UTC in conftest)."""
    return value.replace(tzinfo=UTC)


def at(hour, minute=0, day=6):
    return datetime(2026, 10, day, hour, minute, tzinfo=UTC)


def reading(station, observed, bikes, *, capacity=10, operational=True, offset=0, **overrides):
    docks = 0 if capacity is None else capacity - bikes
    values = {
        "source": "test",
        "station_id": station,
        "station_name": f"S{station}",
        "latitude": 47.2,
        "longitude": -1.55,
        "capacity": capacity,
        "num_bikes_available": bikes,
        "num_docks_available": docks,
        "num_bikes_disabled": 0,
        "num_docks_disabled": 0,
        "is_installed": True,
        "is_renting": operational,
        "is_returning": True,
        # The feed's last_reported: stale on purpose, it must never drive the time axis.
        "event_timestamp": datetime(2026, 10, 6, 1, 0, tzinfo=UTC),
        "ingestion_timestamp": observed,
        "kafka_key": station,
        "kafka_topic": "t",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp": observed,
        "bronze_ingested_at": observed,
        "fill_rate": bikes / capacity if capacity else None,
        "is_empty": bikes == 0,
        "is_full": docks == 0,
        "is_operational": operational,
        "observed_at": observed,
        "silver_processed_at": observed,
        "ingestion_date": observed.date(),
    }
    values.update(overrides)
    return tuple(values[f.name] for f in SILVER_SCHEMA.fields)


def silver(spark, rows):
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def schema_of(df):
    return [(f.name, f.dataType) for f in df.schema.fields]


def test_floor_datetime():
    assert floor_datetime(at(10, 44), 900) == at(10, 30)
    assert floor_datetime(at(10, 44), 3600) == at(10)


def test_change_flag_is_null_safe_and_ignores_the_first_reading(spark):
    df = silver(
        spark,
        [
            reading("1", at(10, 0), 3, offset=1),
            reading("1", at(10, 1), 3, offset=2),  # same counters: no change
            reading("1", at(10, 2), 4, offset=3),  # bikes changed (same last_reported)
            reading("1", at(10, 3), 4, capacity=None, offset=4),  # capacity 10 -> null: a change
            reading("1", at(10, 4), 4, capacity=None, offset=5),  # null -> null: no change
            reading("2", at(10, 0), 7, offset=6),  # first reading of another station
        ],
    )
    flags = {
        (r.station_id, r.observed_at.minute): r.changed for r in with_change_flag(df).collect()
    }
    assert flags == {
        ("1", 0): False, ("1", 1): False, ("1", 2): True, ("1", 3): True, ("1", 4): False,
        ("2", 0): False,
    }  # fmt: skip


def test_current_state_uses_the_latest_observation_not_last_reported(spark):
    df = silver(
        spark,
        [
            reading("1", at(10, 0), 3, offset=1),
            reading("1", at(10, 1), 3, offset=2),
            reading("1", at(10, 2), 0, offset=3),  # emptied; last_reported never moved
            reading("1", at(11, 30), 9, offset=4),  # after as_of: ignored
            reading("2", at(10, 0), 5, offset=5),
            reading("2", at(11, 19), 5, offset=6),  # never changes
        ],
    )
    result = current_state(df, AS_OF)
    assert schema_of(result) == [(f.name, f.dataType) for f in GOLD_STATION_CURRENT_SCHEMA.fields]
    rows = {r.station_id: r for r in result.collect()}
    assert set(rows) == {"1", "2"}

    one = rows["1"]
    assert one.num_bikes_available == 0 and one.is_empty
    assert utc(one.observed_at) == at(10, 2)
    assert utc(one.last_reported_at) == at(1, 0)  # exposed as received, not used
    assert utc(one.last_change_at) == at(10, 2)
    assert one.state_age_is_lower_bound is False
    assert one.hours_in_current_state == pytest.approx(78 / 60)  # 10:02 -> 11:20
    assert one.minutes_since_observed == pytest.approx(78)

    two = rows["2"]
    assert two.last_change_at is None
    assert two.state_age_is_lower_bound is True  # unchanged since we started watching
    assert utc(two.first_observed_at) == at(10, 0)
    assert two.hours_in_current_state == pytest.approx(80 / 60)
    assert two.minutes_since_observed == pytest.approx(1)
    assert utc(two.as_of) == AS_OF


def test_series_summarizes_the_readings_of_each_slot_without_inventing_slots(spark):
    df = silver(
        spark,
        [
            reading("1", at(10, 0), 5, offset=1),
            reading("1", at(10, 7), 5, offset=2),
            reading("1", at(10, 14), 3, offset=3),  # slot 10:00 ends on 3 bikes
            # no reading at all during slot 10:15: no row (pipeline gap), nothing carried over
            reading("1", at(10, 31), 0, offset=4),
            reading("1", at(11, 16), 2, offset=5),  # slot 11:15 contains as_of: incomplete
        ],
    )
    result = station_series_15min(df, AS_OF, window_days=1)
    assert schema_of(result) == [(f.name, f.dataType) for f in GOLD_SERIES_15MIN_SCHEMA.fields]
    rows = sorted(result.collect(), key=lambda r: r.slot_start)
    assert [utc(r.slot_start) for r in rows] == [at(10), at(10, 30), at(11, 15)]

    first = rows[0]
    assert first.bikes_available == 3 and first.docks_available == 7
    assert (first.min_bikes_available, first.max_bikes_available) == (3, 5)
    assert first.mean_bikes_available == pytest.approx(13 / 3)
    assert (first.n_readings, first.n_changes) == (3, 1)
    assert first.is_complete is True
    assert str(first.slot_date) == "2026-10-06"

    second = rows[1]
    assert second.is_empty and second.bikes_available == 0
    assert (second.n_readings, second.n_changes) == (1, 1)  # 3 -> 0 vs the previous slot's reading
    assert [r.is_complete for r in rows] == [True, True, False]


def test_series_window_excludes_old_readings_but_not_their_effect_on_changes(spark):
    df = silver(
        spark,
        [
            reading("3", at(8, 0, day=3), 7, offset=1),  # 3 days before as_of: out of the window
            reading("3", at(10, 0), 6, offset=2),  # first reading in the window
        ],
    )
    rows = station_series_15min(df, AS_OF, window_days=1).collect()
    assert len(rows) == 1
    assert rows[0].n_changes == 1  # compared with the previous reading, even outside the window


def test_hourly_rates_and_coverage(spark):
    rows = []
    offset = 0
    for minute in (5, 20, 35, 50):  # one reading per slot, whole hour 10
        for station, bikes, operational in [("A", 0, True), ("B", 5, True), ("C", 0, False)]:
            offset += 1
            rows.append(
                reading(station, at(10, minute), bikes, operational=operational, offset=offset)
            )
    rows += [reading("A", at(9, 5), 0, offset=100), reading("B", at(9, 5), 5, offset=101)]
    rows += [reading("D", at(10, 40), 2, offset=102)]  # D starts being observed at 10:40
    # Hour 11 is open: slot 11:00 is over, slot 11:15 holds as_of (11:20).
    rows += [reading("A", at(11, 5), 0, offset=103), reading("A", at(11, 16), 0, offset=104)]
    series = station_series_15min(silver(spark, rows), AS_OF, window_days=1)
    result = hourly_rates(series)
    assert schema_of(result) == [(f.name, f.dataType) for f in GOLD_HOURLY_RATES_SCHEMA.fields]

    hours = {utc(r.hour_start): r for r in result.collect()}
    ten = hours[at(10)]
    assert ten.stations_observed == 4  # A, B, C, D
    assert ten.station_coverage == 1.0
    assert ten.slot_observations == 13  # 12 + D's single slot (10:30)
    assert ten.operational_observations == 9  # C is out of service
    assert ten.empty_rate == pytest.approx(4 / 9)  # A empty in 4 slots; C's empties don't count
    assert ten.full_rate == 0.0
    assert ten.non_operational_rate == pytest.approx(4 / 13)
    assert ten.is_complete is True

    nine = hours[at(9)]
    assert nine.stations_observed == 2
    assert nine.station_coverage == pytest.approx(2 / 4)  # C and D unknown that hour
    assert hours[at(11)].is_complete is False  # the slot holding as_of is still open


def test_hourly_rates_are_null_when_no_station_is_operational(spark):
    df = silver(spark, [reading("C", at(10, 5), 0, operational=False, offset=1)])
    hourly = hourly_rates(station_series_15min(df, AS_OF, window_days=1))
    ten = {utc(r.hour_start): r for r in hourly.collect()}[at(10)]
    assert ten.empty_rate is None and ten.full_rate is None
    assert ten.non_operational_rate == 1.0


def test_no_forward_fill_when_the_pipeline_was_down(spark):
    df = silver(
        spark,
        [reading("1", at(8, 0), 4, offset=1), reading("1", at(11, 5), 4, offset=2)],
    )
    slots = [utc(r.slot_start) for r in station_series_15min(df, AS_OF, 1).collect()]
    assert sorted(slots) == [at(8), at(11)]  # nothing invented for the 3-hour gap
