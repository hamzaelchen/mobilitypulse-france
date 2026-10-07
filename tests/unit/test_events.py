from datetime import UTC, datetime

import pytest

from common.events import build_event, build_events

NOW = datetime(2026, 10, 6, 12, 0, 0, 123000, tzinfo=UTC)
INFO = {"station_id": "7", "name": " GARE ", "lat": 47.21, "lon": -1.54, "capacity": 20}
STATUS = {
    "station_id": "7",
    "num_bikes_available": 4,
    "num_docks_available": 16,
    "num_bikes_disabled": 0,
    "num_docks_disabled": 0,
    "is_installed": True,
    "is_renting": True,
    "is_returning": True,
    "last_reported": 1791300000,
}


def make(info=None, status=None, feed_last_updated=1791299325):
    return build_event(
        {**INFO, **(info or {})},
        {**STATUS, **(status or {})},
        source="test",
        feed_last_updated=feed_last_updated,
        ingestion_time=NOW,
    )


def test_event_has_expected_fields_and_metadata():
    event = make()
    assert event["source"] == "test"
    assert event["station_id"] == "7"
    assert event["station_name"] == "GARE"
    assert event["num_bikes_available"] == 4
    assert event["event_timestamp"] == "2026-10-06T15:20:00+00:00"
    assert event["ingestion_timestamp"] == "2026-10-06T12:00:00.123+00:00"
    assert event["is_renting"] is True


def test_event_timestamp_falls_back_to_feed_last_updated():
    event = make(status={"last_reported": None})
    assert event["event_timestamp"] == "2026-10-06T15:08:45+00:00"


@pytest.mark.parametrize(
    ("info", "status", "reason"),
    [
        ({"station_id": ""}, {}, "invalid_station_id"),
        ({"name": None}, {}, "invalid_station_name"),
        ({"lat": None}, {}, "missing_coordinates"),
        ({"lat": 91}, {}, "coordinates_out_of_range"),
        ({"lon": -181}, {}, "coordinates_out_of_range"),
        ({}, {"num_bikes_available": None}, "missing_availability"),
        ({}, {"num_docks_available": "3"}, "missing_availability"),
        ({}, {"num_bikes_available": True}, "missing_availability"),
        ({}, {"num_bikes_available": -1}, "negative_availability"),
        ({}, {"is_renting": None}, "missing_operational_flags"),
        ({}, {"is_installed": 1}, "missing_operational_flags"),
    ],
)
def test_invalid_records_raise_with_reason(info, status, reason):
    with pytest.raises(ValueError, match=reason):
        make(info, status)


def test_missing_every_timestamp_is_rejected():
    with pytest.raises(ValueError, match="missing_event_timestamp"):
        make(status={"last_reported": None}, feed_last_updated=None)


def test_optional_fields_become_null_when_invalid():
    event = make(info={"capacity": None}, status={"num_bikes_disabled": -2})
    assert event["capacity"] is None
    assert event["num_bikes_disabled"] is None


def test_build_events_on_real_fixture(gbfs_info, gbfs_status):
    events, rejected = build_events(gbfs_info, gbfs_status, source="fx", ingestion_time=NOW)
    assert len(events) == 12
    assert rejected == []
    out_of_service = next(e for e in events if e["station_id"] == "16")
    assert out_of_service["is_renting"] is False
    assert out_of_service["num_bikes_available"] == 0


def test_build_events_skips_bad_and_unknown_stations(gbfs_info, gbfs_status):
    status = gbfs_status["data"]["stations"]
    status[0]["num_bikes_available"] = -5
    status.append({**status[1], "station_id": "not-in-information"})
    events, rejected = build_events(gbfs_info, gbfs_status, source="fx", ingestion_time=NOW)
    assert len(events) == 11
    assert {(r.station_id, r.reason) for r in rejected} == {
        ("1", "negative_availability"),
        ("not-in-information", "unknown_station"),
    }
