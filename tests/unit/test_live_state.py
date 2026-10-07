"""Incremental station state (live path): same semantics as Gold, hand-computed expectations."""

from datetime import UTC, datetime, timedelta

import pytest

from live.state import Reading, apply_readings, state_times

T0 = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


def reading(station="1", minute=0, bikes=3, *, capacity=10, offset=0, **overrides):
    values = {
        "source": "s",
        "station_id": station,
        "station_name": f"S{station}",
        "latitude": 47.2,
        "longitude": -1.55,
        "capacity": capacity,
        "bikes": bikes,
        "docks": None if capacity is None else capacity - bikes,
        "bikes_disabled": 0,
        "docks_disabled": 0,
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
        "last_reported_at": T0 - timedelta(hours=5),  # stale on purpose: never the time axis
        "observed_at": T0 + timedelta(minutes=minute),
        "kafka_offset": offset,
    }
    values.update(overrides)
    if values["docks"] is None:
        values["docks"] = 0
    return Reading(**values)


def only(rows, station="1"):
    return rows[("s", station)]


def test_a_new_station_starts_unchanged_and_first_observed_now():
    row = only(apply_readings({}, [reading(minute=0)]))
    assert row.observed_at == T0 and row.first_observed_at == T0
    assert row.last_change_at is None


def test_an_identical_later_reading_only_advances_the_observation_time():
    first = apply_readings({}, [reading(minute=0)])
    second = apply_readings(first, [reading(minute=5)])
    row = only(second)
    assert row.observed_at == T0 + timedelta(minutes=5)
    assert row.first_observed_at == T0 and row.last_change_at is None


def test_a_counter_change_records_the_time_of_that_reading():
    state = apply_readings({}, [reading(minute=0, bikes=3)])
    state = apply_readings(state, [reading(minute=1, bikes=3), reading(minute=2, bikes=2)])
    assert only(state).last_change_at == T0 + timedelta(minutes=2)
    assert only(state).bikes == 2 and only(state).docks == 8


def test_going_back_to_a_previous_value_is_still_a_change():
    state = apply_readings({}, [reading(minute=0, bikes=5), reading(minute=1, bikes=4)])
    state = apply_readings(state, [reading(minute=2, bikes=5)])
    assert only(state).last_change_at == T0 + timedelta(minutes=2)


def test_a_replayed_or_older_reading_changes_nothing():
    state = apply_readings({}, [reading(minute=0, bikes=3), reading(minute=2, bikes=2)])
    updated = apply_readings(state, [reading(minute=2, bikes=2), reading(minute=1, bikes=9)])
    assert updated == {}  # nothing newer than what is stored


def test_readings_are_processed_in_time_order_whatever_the_input_order():
    shuffled = [reading(minute=3, bikes=5), reading(minute=1, bikes=3), reading(minute=2, bikes=3)]
    row = only(apply_readings({}, shuffled))
    assert row.first_observed_at == T0 + timedelta(minutes=1)
    assert row.last_change_at == T0 + timedelta(minutes=3)
    assert row.observed_at == T0 + timedelta(minutes=3) and row.bikes == 5


def test_the_kafka_offset_breaks_ties_between_identical_timestamps():
    same_time = [reading(minute=1, bikes=2, offset=9), reading(minute=1, bikes=4, offset=3)]
    assert only(apply_readings({}, same_time)).bikes == 2  # the higher offset is the latest


def test_comparison_is_null_safe():
    state = apply_readings({}, [reading(minute=0, capacity=10)])
    state = apply_readings(state, [reading(minute=1, capacity=None)])  # known -> unknown: change
    assert only(state).last_change_at == T0 + timedelta(minutes=1)
    state = apply_readings(state, [reading(minute=2, capacity=None)])  # unknown -> unknown
    assert only(state).last_change_at == T0 + timedelta(minutes=1)


def test_every_compared_field_counts_as_a_change():
    base = reading(minute=0)
    for field, value in [
        ("bikes_disabled", 2),
        ("docks_disabled", 1),
        ("is_installed", False),
        ("is_renting", False),
        ("is_returning", False),
    ]:
        state = apply_readings({}, [base])
        state = apply_readings(state, [reading(minute=1, **{field: value})])
        assert only(state).last_change_at is not None, field


def test_the_station_name_and_position_follow_the_latest_reading_but_are_not_a_change():
    state = apply_readings({}, [reading(minute=0)])
    state = apply_readings(state, [reading(minute=1, station_name="Renamed", latitude=47.3)])
    row = only(state)
    assert row.station_name == "Renamed" and row.latitude == 47.3
    assert row.last_change_at is None


def test_only_the_stations_that_received_a_newer_reading_are_returned():
    state = apply_readings({}, [reading("1", 0), reading("2", 0)])
    updated = apply_readings(state, [reading("1", 1)])
    assert set(updated) == {("s", "1")}


def test_derived_columns():
    empty = only(apply_readings({}, [reading(bikes=0)]))
    assert empty.is_empty and not empty.is_full and empty.fill_rate == 0.0
    full = only(apply_readings({}, [reading(bikes=10)]))
    assert full.is_full and full.fill_rate == 1.0
    out = only(apply_readings({}, [reading(is_renting=False)]))
    assert out.is_operational is False
    unknown = only(apply_readings({}, [reading(capacity=None)]))
    assert unknown.fill_rate is None
    zero = only(apply_readings({}, [reading(capacity=0, bikes=0)]))
    assert zero.fill_rate is None


def test_state_times_from_the_last_observed_change():
    state = apply_readings({}, [reading(minute=0, bikes=3), reading(minute=30, bikes=2)])
    as_of = T0 + timedelta(minutes=90)
    minutes_since_observed, hours_in_state, lower_bound = state_times(only(state), as_of)
    assert minutes_since_observed == pytest.approx(60.0)
    assert hours_in_state == pytest.approx(1.0)  # 30 -> 90 min
    assert lower_bound is False


def test_state_times_without_an_observed_change_is_a_lower_bound():
    state = apply_readings({}, [reading(minute=0), reading(minute=10)])
    as_of = T0 + timedelta(minutes=60)
    minutes_since_observed, hours_in_state, lower_bound = state_times(only(state), as_of)
    assert minutes_since_observed == pytest.approx(50.0)
    assert hours_in_state == pytest.approx(1.0)  # counted from the first observation
    assert lower_bound is True
