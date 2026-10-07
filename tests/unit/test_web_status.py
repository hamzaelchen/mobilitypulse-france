"""Freshness classification, gap detection and snapshot diff (pure logic, no I/O)."""

from datetime import UTC, datetime, timedelta

from web.snapshot import diff_stations, kpi_signature
from web.status import classify_freshness, find_gaps, pipeline_status

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


def ago(seconds):
    return NOW - timedelta(seconds=seconds)


def test_freshness_thresholds():
    assert classify_freshness(0) == "live"
    assert classify_freshness(150) == "live"
    assert classify_freshness(150.1) == "delayed"
    assert classify_freshness(600) == "delayed"
    assert classify_freshness(600.1) == "stale"
    assert classify_freshness(None) == "unknown"


def test_a_negative_age_from_clock_skew_is_treated_as_live():
    assert classify_freshness(-3) == "live"


def test_live_data_has_no_cause():
    status = pipeline_status(NOW, observed_at=ago(30), heartbeat_at=ago(5))
    assert status["freshness"] == "live" and status["cause"] is None
    assert status["data_age_seconds"] == 30


def test_old_data_with_a_beating_updater_means_collection_stopped():
    status = pipeline_status(NOW, observed_at=ago(900), heartbeat_at=ago(5))
    assert status["freshness"] == "stale"
    assert status["cause"] == "collection_stopped"


def test_old_data_with_a_silent_updater_means_updater_down():
    status = pipeline_status(NOW, observed_at=ago(900), heartbeat_at=ago(300))
    assert status["cause"] == "updater_down"


def test_no_heartbeat_at_all_means_updater_down():
    assert pipeline_status(NOW, ago(900), None)["cause"] == "updater_down"


def test_delayed_data_also_gets_a_cause():
    status = pipeline_status(NOW, observed_at=ago(300), heartbeat_at=ago(5))
    assert status["freshness"] == "delayed" and status["cause"] == "collection_stopped"


def test_no_data_at_all():
    status = pipeline_status(NOW, observed_at=None, heartbeat_at=ago(5))
    assert status["freshness"] == "unknown" and status["cause"] == "no_data"
    assert status["data_age_seconds"] is None


def test_gaps_are_polls_further_apart_than_the_threshold():
    times = [ago(3600), ago(3540), ago(3480), ago(2400), ago(2340), ago(60)]
    gaps = find_gaps(times, min_gap_seconds=300)
    assert gaps == [
        {"start": ago(3480), "end": ago(2400), "seconds": 1080},
        {"start": ago(2340), "end": ago(60), "seconds": 2280},
    ]


def test_regular_polls_have_no_gap_and_unsorted_input_is_fine():
    assert find_gaps([ago(120), ago(0), ago(60)], min_gap_seconds=300) == []
    assert find_gaps([], 300) == [] and find_gaps([ago(1)], 300) == []


def station(station_id, bikes=3, **extra):
    return {"station_id": station_id, "num_bikes_available": bikes, "state": "ok", **extra}


def test_diff_reports_changed_added_and_removed_stations():
    previous = {"1": station("1", 3), "2": station("2", 5), "3": station("3", 1)}
    current = {"1": station("1", 3), "2": station("2", 4), "4": station("4", 0)}
    diff = diff_stations(previous, current)
    assert diff["changed"] == [station("2", 4), station("4", 0)]
    assert diff["removed"] == ["3"]


def test_identical_snapshots_give_an_empty_diff():
    same = {"1": station("1")}
    assert diff_stations(same, dict(same)) == {"changed": [], "removed": []}


def test_the_first_snapshot_reports_everything_as_changed():
    diff = diff_stations(None, {"1": station("1"), "2": station("2")})
    assert [s["station_id"] for s in diff["changed"]] == ["1", "2"] and diff["removed"] == []


def test_kpi_signature_changes_only_when_a_displayed_value_changes():
    base = {"stations": 105, "bikes": 400, "observed_at": "t1", "freshness": "live"}
    assert kpi_signature(base) == kpi_signature(dict(base))
    assert kpi_signature(base) != kpi_signature({**base, "bikes": 401})
    assert kpi_signature(base) != kpi_signature({**base, "freshness": "stale"})


def test_an_updater_that_beats_but_fails_is_reported_as_such():
    status = pipeline_status(NOW, ago(900), ago(5), updater_error="OperationalError")
    assert status["cause"] == "updater_error" and status["updater_error"] == "OperationalError"


def test_an_updater_error_does_not_hide_a_silent_updater():
    assert pipeline_status(NOW, ago(900), ago(300), "X")["cause"] == "updater_down"


def test_an_updater_error_is_not_a_cause_while_the_data_is_live():
    status = pipeline_status(NOW, ago(30), ago(5), updater_error="X")
    assert status["freshness"] == "live" and status["cause"] is None
