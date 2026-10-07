"""Haversine, priority score and transfer matching: pure Python, hand-computed expectations."""

import random

import pytest

from prescriptive.geo import haversine_km
from prescriptive.matching import recommend_transfers
from prescriptive.outputs import PRIORITY_SPEC, TRANSFER_SPEC, build_outputs
from prescriptive.scoring import (
    RISK_SOURCE,
    Params,
    StationState,
    classify,
    criticality,
    score_station,
    urgency,
)

P = Params()
LAT, LON = 47.20, -1.55
STEP_KM = 111.1949 / 100  # 0.01 degree of latitude


def st(
    station_id,
    bikes,
    capacity=20,
    *,
    dlat=0.0,
    operational=True,
    hours=1.0,
    trend=None,
    source="s",
):
    docks = None if capacity is None else capacity - bikes
    return StationState(
        source=source,
        station_id=station_id,
        station_name=f"S{station_id}",
        latitude=LAT + dlat,
        longitude=LON,
        capacity=capacity,
        bikes=bikes,
        docks=docks if docks is not None else 0,
        fill_rate=None if not capacity else bikes / capacity,
        is_operational=operational,
        hours_in_state=hours,
        state_age_is_lower_bound=False,
        trend_30min=trend,
    )


# --- haversine -------------------------------------------------------------------------------
def test_haversine_known_distances():
    assert haversine_km(0, 0, 0, 0) == 0.0
    assert haversine_km(0, 0, 1, 0) == pytest.approx(111.195, abs=0.01)  # 1 degree of latitude
    assert haversine_km(48.8566, 2.3522, 51.5074, -0.1278) == pytest.approx(343.5, abs=1.0)
    a, b = (47.2184, -1.5536), (47.2300, -1.5000)
    assert haversine_km(*a, *b) == pytest.approx(haversine_km(*b, *a))  # symmetric


# --- classification and score ----------------------------------------------------------------
def test_classification():
    assert classify(st("1", 0), P) == "empty"
    assert classify(st("1", 2), P) == "low"  # 0.10 < 0.15
    assert classify(st("1", 10), P) == "ok"
    assert classify(st("1", 18), P) == "high"  # 0.90 > 0.85
    assert classify(st("1", 20), P) == "full"
    assert classify(st("1", 5, operational=False), P) == "out_of_service"
    assert classify(st("1", 5, capacity=None), P) == "unknown_capacity"


def test_empty_station_scores_by_hand():
    s = score_station(st("1", 0, hours=1.0), P)
    assert s.urgency == pytest.approx(1.0)
    assert s.criticality == pytest.approx(0.6 + 0.4 * 0.5)
    assert s.priority_score == pytest.approx(100 * (0.5 * 1.0 + 0.5 * 0.8))  # 90
    assert (s.need_type, s.bikes_gap, s.donor_surplus) == ("needs_bikes", 10, 0)


def test_low_station_has_urgency_but_no_criticality():
    s = score_station(st("1", 2), P)  # fill 0.10
    assert s.urgency == pytest.approx((0.15 - 0.10) / 0.15)
    assert s.criticality == 0.0
    assert s.priority_score == pytest.approx(100 * 0.5 * (0.05 / 0.15))
    assert (s.need_type, s.bikes_gap) == ("needs_bikes", 8)


def test_ok_station_is_a_possible_donor_but_has_no_priority():
    ok = score_station(st("1", 10), P)
    assert (ok.priority_score, ok.need_type, ok.donor_surplus) == (0.0, None, 0)
    donor = score_station(st("2", 14), P)  # fill 0.70 >= 0.60
    assert (donor.priority_score, donor.need_type, donor.donor_surplus) == (0.0, None, 4)


def test_full_and_high_stations_need_docks_and_can_donate():
    full = score_station(st("1", 20, hours=3.0), P)
    assert full.criticality == pytest.approx(1.0)  # past the 2 h ramp
    assert full.priority_score == pytest.approx(100.0)
    assert (full.need_type, full.bikes_gap, full.donor_surplus) == ("needs_docks", 10, 10)
    high = score_station(st("2", 18), P)
    assert high.urgency == pytest.approx((0.90 - 0.85) / 0.15)
    assert (high.need_type, high.bikes_gap, high.donor_surplus) == ("needs_docks", 8, 8)


def test_out_of_service_and_unknown_capacity_are_never_rebalanced():
    for station in (st("1", 0, operational=False), st("2", 0, capacity=None)):
        s = score_station(station, P)
        assert (s.priority_score, s.need_type, s.bikes_gap, s.donor_surplus) == (0.0, None, 0, 0)


def test_trend_raises_the_urgency_of_a_station_that_is_still_ok():
    falling = score_station(st("1", 4, trend=-0.15), P)  # fill 0.20 -> projected 0.05
    assert falling.state == "ok"
    assert falling.projected_fill == pytest.approx(0.05)
    assert falling.urgency == pytest.approx((0.15 - 0.05) / 0.15)
    assert falling.priority_score == pytest.approx(100 * 0.5 * (0.10 / 0.15))
    assert falling.need_type is None  # needs are decided on the CURRENT state only
    no_trend = score_station(st("1", 4, trend=-0.15), Params(use_trend=False))
    assert no_trend.projected_fill == pytest.approx(0.20) and no_trend.urgency == 0.0
    assert score_station(st("2", 4, trend=-5.0), P).projected_fill == 0.0  # clamped


def test_target_rounds_halves_up_not_to_even():
    from prescriptive.scoring import target_bikes

    # Python's round() would give 10, 12 and 6 here; the rule is half up, like PostgreSQL.
    assert [target_bikes(c, P) for c in (21, 25, 13, 20, 15, 1)] == [11, 13, 7, 10, 8, 1]
    donor = score_station(st("d", 24, capacity=25), P)  # fill 0.96, target 13
    assert donor.donor_surplus == 11 and donor.bikes_gap == 11


def test_criticality_ramp():
    assert criticality("empty", 0.0, P) == pytest.approx(0.6)
    assert criticality("full", 1.0, P) == pytest.approx(0.8)
    assert criticality("empty", 10.0, P) == pytest.approx(1.0)
    assert criticality("low", 10.0, P) == 0.0
    assert urgency(None, P) == 0.0


def test_invalid_parameters_are_rejected():
    for kwargs in [
        {"low_band": 0.6},
        {"high_band": 0.4},
        {"donor_min_fill": 0.5},
        {"min_transfer": 0},
        {"min_transfer": 5, "max_transfer": 3},
        {"weight_urgency": 0.7, "weight_criticality": 0.7},
    ]:
        with pytest.raises(ValueError):
            Params(**kwargs)


# --- matching ---------------------------------------------------------------------------------
def transfers_of(states, params=P):
    return recommend_transfers([score_station(s, params) for s in states], params)


def test_donor_lacking_docks_is_preferred_even_if_farther():
    states = [
        st("R", 0),  # empty, needs 10 bikes
        st("near", 14, dlat=0.005),  # ok donor, surplus 4, 0.56 km
        st("far", 20, dlat=0.02),  # full: needs docks, surplus 10, 2.22 km
    ]
    [t] = transfers_of(states)
    assert (t.from_station_id, t.to_station_id, t.bikes) == ("far", "R", 10)
    assert t.distance_km == pytest.approx(2 * STEP_KM, rel=1e-3)
    assert (t.from_state, t.to_state, t.rank) == ("full", "empty", 1)


def test_several_donors_fill_a_gap_nearest_first_among_equals():
    states = [
        st("R", 0),
        st("A", 14, dlat=0.005),  # surplus 4, nearer
        st("B", 14, dlat=0.010),  # surplus 4, farther
        st("C", 14, dlat=0.015),  # surplus 4, farthest
    ]
    result = [(t.from_station_id, t.bikes) for t in transfers_of(states)]
    assert result == [("A", 4), ("B", 4), ("C", 2)]  # gap 10 = 4 + 4 + 2


def test_limits_distance_max_transfer_and_min_transfer():
    # Out of range: the only donor is 5.6 km away.
    assert transfers_of([st("R", 0), st("D", 20, dlat=0.05)]) == []
    # max_transfer caps a pair (and a donor is used once per receiver).
    small = Params(max_transfer=4)
    result = [
        (t.from_station_id, t.bikes)
        for t in transfers_of([st("R", 0), st("A", 14, dlat=0.005), st("B", 20, dlat=0.01)], small)
    ]
    # Both donors give 4 (the pair is capped); gap 10 leaves 2 unplanned. The list is numbered
    # by receiver priority then distance: the nearer donor A comes first, though B was chosen first.
    assert sorted(result) == [("A", 4), ("B", 4)]
    # min_transfer: a remainder of 1 bike is not worth a trip.
    states = [st("R", 0, capacity=6), st("D", 12, dlat=0.005)]  # gap 3, donor surplus 2
    [t] = transfers_of(states)
    assert t.bikes == 2


def test_a_donor_never_gives_more_than_its_surplus_across_receivers():
    states = [
        st("R1", 0, hours=3.0),  # priority 100: served first
        st("R2", 0, hours=0.0),  # priority 80
        st("D", 16, dlat=0.005),  # surplus 6
    ]
    result = [(t.to_station_id, t.from_station_id, t.bikes) for t in transfers_of(states)]
    assert result == [("R1", "D", 6)]  # nothing left for R2
    assert sum(t.bikes for t in transfers_of(states)) <= 6


def test_out_of_service_stations_do_not_take_part_and_sources_do_not_mix():
    states = [
        st("R", 0),
        st("oos", 20, dlat=0.005, operational=False),
        st("other", 20, dlat=0.005, source="elsewhere"),
    ]
    assert transfers_of(states) == []
    [t] = transfers_of([*states, st("D", 20, dlat=0.01)])
    assert t.from_station_id == "D"


def test_receivers_by_priority_and_deterministic_whatever_the_input_order():
    states = [st("R1", 0, hours=3.0), st("R2", 0, hours=0.0, dlat=0.004)]
    states += [st("A", 20, dlat=0.006), st("B", 20, dlat=0.008), st("C", 16, dlat=0.002)]
    reference = transfers_of(states)
    assert [t.rank for t in reference] == list(range(1, len(reference) + 1))
    assert reference[0].to_station_id == "R1"
    for seed in range(5):
        shuffled = states[:]
        random.Random(seed).shuffle(shuffled)
        assert transfers_of(shuffled) == reference


# --- output rows ------------------------------------------------------------------------------
def test_outputs_are_consistent_with_the_tables():
    from datetime import UTC, datetime

    states = [
        st("R", 0),  # empty
        st("L", 2, dlat=0.004),  # low
        st("D", 20, dlat=0.006),  # full, surplus 10
        st("ok", 10, dlat=0.02),
        st("oos", 0, operational=False, dlat=0.03),
    ]
    now = datetime(2026, 10, 6, 12, tzinfo=UTC)
    priority_rows, transfer_rows, scores, transfers = build_outputs(states, P, now)

    assert all(len(r) == len(PRIORITY_SPEC.columns) for r in priority_rows)
    assert all(len(r) == len(TRANSFER_SPEC.columns) for r in transfer_rows)
    col = {name: i for i, name in enumerate(PRIORITY_SPEC.columns)}
    by_id = {r[col["station_id"]]: r for r in priority_rows}
    assert sorted(r[col["priority_rank"]] for r in priority_rows) == [1, 2, 3, 4, 5]
    # D (full) and R (empty) both score 90: the tie is broken by station id; then L (low), ok, oos.
    assert [by_id[i][col["priority_rank"]] for i in ("D", "R", "L", "ok", "oos")] == [1, 2, 3, 4, 5]
    assert {r[col["risk_source"]] for r in priority_rows} == {RISK_SOURCE}

    # D (full, 10 spare bikes) feeds the receivers by priority: R needs 10, L needs 8.
    assert [(t.to_station_id, t.bikes) for t in transfers] == [("R", 10)]
    assert by_id["R"][col["planned_in"]] == 10 and by_id["R"][col["unplanned_gap"]] == 0
    assert by_id["D"][col["planned_out"]] == 10 and by_id["D"][col["unplanned_gap"]] == 0
    assert by_id["L"][col["planned_in"]] == 0 and by_id["L"][col["unplanned_gap"]] == 8
    assert by_id["oos"][col["priority_score"]] == 0.0 and by_id["oos"][col["need_type"]] is None
    assert transfer_rows[0][TRANSFER_SPEC.columns.index("distance_km")] > 0
    assert len(scores) == len(priority_rows) == 5
