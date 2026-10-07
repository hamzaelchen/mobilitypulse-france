"""Station priority score (pure Python, no I/O).

IMPORTANT: the "risk" used here is a transparent HEURISTIC (current fill rate shifted by the
trend of the last 30 minutes), not the output of a trained model: phase 5 has not enough history to
produce a reliable one. Every threshold and weight below is a CONVENTION, documented and
configurable, not a measured quantity. ``risk_source`` records this in the output tables, so that a
model probability can replace it later without ambiguity.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import floor

RISK_SOURCE = "heuristic_trend"


@dataclass(frozen=True)
class Params:
    low_band: float = 0.15  # fill rate below: the station is "low" and needs bikes
    high_band: float = 0.85  # fill rate above: the station is "high" and needs free docks
    target_fill: float = 0.5  # fill rate a rebalancing aims at
    donor_min_fill: float = 0.6  # a station above this may give bikes away
    use_trend: bool = True
    weight_urgency: float = 0.5
    weight_criticality: float = 0.5
    critical_ramp_hours: float = 2.0  # time in a critical state after which criticality is maximal
    max_distance_km: float = 3.0
    max_transfer: int = 20  # bikes per transfer (vehicle capacity convention)
    min_transfer: int = 2  # moving fewer bikes is not worth a trip

    def __post_init__(self) -> None:
        if not 0 < self.low_band < self.target_fill < self.high_band < 1:
            raise ValueError("bands must satisfy 0 < low < target < high < 1")
        if self.donor_min_fill <= self.target_fill:
            raise ValueError("donor_min_fill must be above target_fill")
        if self.min_transfer < 1 or self.max_transfer < self.min_transfer:
            raise ValueError("need 1 <= min_transfer <= max_transfer")
        if abs(self.weight_urgency + self.weight_criticality - 1.0) > 1e-9:
            raise ValueError("weights must sum to 1")


@dataclass(frozen=True)
class StationState:
    source: str
    station_id: str
    station_name: str
    latitude: float
    longitude: float
    capacity: int | None
    bikes: int
    docks: int
    fill_rate: float | None
    is_operational: bool
    hours_in_state: float
    state_age_is_lower_bound: bool
    trend_30min: float | None  # change of fill rate over the last 30 min; None if unknown


@dataclass(frozen=True)
class StationScore:
    station: StationState
    state: str  # empty | low | ok | high | full | out_of_service | unknown_capacity
    projected_fill: float | None
    urgency: float
    criticality: float
    priority_score: float  # 0..100
    need_type: str | None  # needs_bikes | needs_docks | None
    bikes_gap: int  # bikes to receive (needs_bikes) or to remove (needs_docks); 0 otherwise
    donor_surplus: int  # bikes this station could give away (donor_min_fill and above)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def classify(s: StationState, p: Params) -> str:
    if not s.is_operational:
        return "out_of_service"
    if s.capacity is None or s.capacity <= 0 or s.fill_rate is None:
        return "unknown_capacity"
    if s.bikes == 0:
        return "empty"
    if s.docks == 0:
        return "full"
    if s.fill_rate < p.low_band:
        return "low"
    if s.fill_rate > p.high_band:
        return "high"
    return "ok"


def target_bikes(capacity: int, p: Params) -> int:
    """Bikes at the target fill rate, rounded HALF UP (10.5 -> 11).

    Python's ``round`` rounds halves to the even integer (10.5 -> 10), which is neither what
    PostgreSQL does nor what a reader expects; this was caught by an independent SQL check.
    """
    return floor(p.target_fill * capacity + 0.5)


def projected_fill(s: StationState, p: Params) -> float | None:
    """Fill rate in 30 min if the last 30-minute trend continued (clamped to [0, 1])."""
    if s.fill_rate is None:
        return None
    if not p.use_trend or s.trend_30min is None:
        return s.fill_rate
    return _clamp(s.fill_rate + s.trend_30min)


def urgency(projected: float | None, p: Params) -> float:
    """0 inside the healthy band, rising to 1 at an empty / full projected station."""
    if projected is None:
        return 0.0
    if projected < p.low_band:
        return _clamp((p.low_band - projected) / p.low_band)
    if projected > p.high_band:
        return _clamp((projected - p.high_band) / (1 - p.high_band))
    return 0.0


def criticality(state: str, hours_in_state: float, p: Params) -> float:
    """0 unless the station is empty or full now; then 0.6 rising to 1.0 with the time spent so."""
    if state not in ("empty", "full"):
        return 0.0
    return 0.6 + 0.4 * _clamp(hours_in_state / p.critical_ramp_hours)


def score_station(s: StationState, p: Params) -> StationScore:
    state = classify(s, p)
    if state in ("out_of_service", "unknown_capacity"):
        # Not rebalanced and never a receiver or a donor: its counters say nothing about service.
        return StationScore(s, state, None, 0.0, 0.0, 0.0, None, 0, 0)

    assert s.capacity is not None  # guaranteed by classify()
    projected = projected_fill(s, p)
    u = urgency(projected, p)
    c = criticality(state, s.hours_in_state, p)
    priority = 100.0 * (p.weight_urgency * u + p.weight_criticality * c)

    target = target_bikes(s.capacity, p)
    need_type: str | None = None
    gap = 0
    if state in ("empty", "low"):
        need_type, gap = "needs_bikes", max(0, target - s.bikes)
    elif state in ("full", "high"):
        need_type, gap = "needs_docks", max(0, s.bikes - target)
    surplus = 0
    if s.fill_rate is not None and s.fill_rate >= p.donor_min_fill:
        surplus = max(0, s.bikes - target)
    return StationScore(s, state, projected, u, c, round(priority, 6), need_type, gap, surplus)
