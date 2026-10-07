"""Data-quality checks on the serving layer (PostgreSQL). One number per check, explicit thresholds.

Each check function returns ``(value, detail)``; the registry ``SPECS`` gives it a stable name and
its thresholds, so that a check that fails to run is reported under the SAME name (and Grafana
history stays aligned). All thresholds are conventions, written next to each check. ``now`` is a
parameter so that checks are reproducible and never depend on the database clock.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from loader.migrations import validate_identifier
from quality.core import CheckResult, describe_thresholds, measure

logger = logging.getLogger("quality")
LAYER = "serving"
READINGS_PER_SLOT = 15  # polls every ~60 s in a 15-minute slot

Measurement = tuple[float | None, str]


def _scalar(conn, sql: str, params: tuple[Any, ...] = ()) -> float | None:
    row = conn.execute(sql, params).fetchone()
    value = row[0] if row else None
    if value is None:
        return None
    return float(value) if isinstance(value, Decimal) else float(value)


# --- checks: each returns (value, detail) ----------------------------------------------------
def serving_freshness(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        "SELECT extract(epoch FROM (%s::timestamptz - max(observed_at))) / 60 "
        f"FROM {s}.station_current",
        (now,),
    )
    return value, "newest reading in station_current (depends on manual Gold + load runs)"


def stations_not_reporting(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn, f"SELECT avg((minutes_since_observed > 10)::int) FROM {s}.station_current"
    )
    return value, "stations whose last reading was > 10 min before the Gold run"


def stations_vs_history(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        f"SELECT (SELECT count(*) FROM {s}.station_current)::float8 / "
        f"NULLIF((SELECT count(DISTINCT station_id) FROM {s}.station_series_15min), 0)",
    )
    return value, "stations in station_current / stations ever seen in the series"


def slot_gaps(conn, s: str, now: datetime) -> Measurement:
    """Largest hole between consecutive complete 15-minute slots over the last 24 h."""
    row = conn.execute(
        f"""
        WITH slots AS (
            SELECT DISTINCT slot_start FROM {s}.station_series_15min
            WHERE is_complete AND slot_start >= %s::timestamptz - interval '24 hours'
        ), gaps AS (
            SELECT extract(epoch FROM (slot_start - lag(slot_start) OVER (ORDER BY slot_start)))
                   / 60 AS minutes
            FROM slots
        )
        SELECT max(minutes), count(*) FILTER (WHERE minutes > 30) FROM gaps
        """,
        (now,),
    ).fetchone()
    value = None if row[0] is None else float(row[0])
    return value, f"{row[1]} gap(s) > 30 min in the last 24 h"


def readings_per_slot(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        f"SELECT avg(n_readings) / {READINGS_PER_SLOT}.0 FROM {s}.station_series_15min "
        "WHERE is_complete AND slot_start >= %s::timestamptz - interval '24 hours'",
        (now,),
    )
    return value, f"average readings per complete slot / {READINGS_PER_SLOT} expected"


def capacity_overflow(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        f"SELECT avg((num_bikes_available + num_docks_available > capacity)::int) "
        f"FROM {s}.station_current WHERE capacity > 0",
    )
    return value, "stations with bikes + free docks > capacity (physically impossible)"


def fill_rate_above_one(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        f"SELECT avg((fill_rate > 1)::int) FROM {s}.station_current WHERE fill_rate IS NOT NULL",
    )
    return value, "stations with more bikes than capacity"


def hourly_matches_series(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        f"""
        SELECT abs(
            coalesce((SELECT sum(slot_observations) FROM {s}.station_hourly_rates
                      WHERE is_complete), 0)
            - (SELECT count(*) FROM {s}.station_series_15min
               WHERE date_trunc('hour', slot_start) IN
                     (SELECT hour_start FROM {s}.station_hourly_rates WHERE is_complete)))
        """,
    )
    return value, "|sum of slot_observations - rows of the series| over complete hours"


def last_load_age(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        "SELECT extract(epoch FROM (%s::timestamptz - max(loaded_at))) / 60 "
        f"FROM {s}.load_log WHERE dataset = 'station_current'",
        (now,),
    )
    return value, "age of the last Gold load into PostgreSQL"


def rebalancing_alignment(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        f"SELECT abs(extract(epoch FROM ((SELECT max(computed_at) FROM {s}.rebalancing_priority)"
        f" - (SELECT max(as_of) FROM {s}.station_current)))) / 60",
    )
    return value, "recommendations computed from the same Gold run as station_current?"


def rebalancing_integrity(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        f"""
        SELECT CASE WHEN (SELECT count(*) FROM {s}.rebalancing_priority) = 0 THEN NULL ELSE
            abs((SELECT sum(planned_in) FROM {s}.rebalancing_priority)
                - (SELECT sum(planned_out) FROM {s}.rebalancing_priority))
          + abs((SELECT sum(planned_out) FROM {s}.rebalancing_priority)
                - coalesce((SELECT sum(bikes) FROM {s}.rebalancing_transfers), 0)) END
        """,
    )
    return value, "|bikes in - bikes out| + |bikes out - bikes in the transfers|"


def operational_empty_share(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn, f"SELECT avg(is_empty::int) FROM {s}.station_current WHERE is_operational"
    )
    return value, "operational stations that are empty (a feed fault looks like this)"


def out_of_service_share(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(conn, f"SELECT avg((NOT is_operational)::int) FROM {s}.station_current")
    return value, "stations out of service"


def future_slots(conn, s: str, now: datetime) -> Measurement:
    value = _scalar(
        conn,
        f"SELECT count(*) FROM {s}.station_series_15min WHERE slot_start > %s::timestamptz",
        (now,),
    )
    return value, "slots dated after now (clock or logic fault)"


# --- registry --------------------------------------------------------------------------------
@dataclass(frozen=True)
class Spec:
    name: str
    fn: Callable[[Any, str, datetime], Measurement]
    thresholds: dict[str, float] = field(default_factory=dict)


SPECS: tuple[Spec, ...] = (
    Spec("serving_freshness_minutes", serving_freshness, {"warn_above": 10, "fail_above": 60}),
    Spec(
        "stations_not_reporting_share", stations_not_reporting, {"warn_above": 0, "fail_above": 0.1}
    ),
    Spec("stations_vs_history_ratio", stations_vs_history, {"warn_below": 0.95, "fail_below": 0.8}),
    # A historical hole is a WARN, never a FAIL: the pipeline may be healthy again.
    Spec("slot_gap_max_minutes_24h", slot_gaps, {"warn_above": 30}),
    Spec("readings_per_slot_ratio_24h", readings_per_slot, {"warn_below": 0.9, "fail_below": 0.5}),
    Spec("capacity_overflow_share", capacity_overflow, {"warn_above": 0, "fail_above": 0.05}),
    Spec("fill_rate_above_one_share", fill_rate_above_one, {"warn_above": 0, "fail_above": 0.05}),
    Spec("hourly_vs_series_difference", hourly_matches_series, {"fail_above": 0}),
    Spec("last_gold_load_age_minutes", last_load_age, {"warn_above": 60, "fail_above": 1440}),
    Spec(
        "rebalancing_alignment_minutes", rebalancing_alignment, {"warn_above": 1, "fail_above": 120}
    ),
    Spec("rebalancing_integrity_difference", rebalancing_integrity, {"fail_above": 0}),
    Spec("operational_empty_share", operational_empty_share, {"warn_above": 0.5}),
    Spec("out_of_service_share", out_of_service_share, {"warn_above": 0.2}),
    Spec("future_slots", future_slots, {"fail_above": 0}),
)


def run_serving_checks(conn, schema: str, now: datetime) -> list[CheckResult]:
    """Run every check; one that cannot run is a WARN carrying the error, never a silent PASS."""
    schema = validate_identifier(schema)
    results: list[CheckResult] = []
    for spec in SPECS:
        try:
            with conn.transaction():  # a savepoint: a failing query does not poison the others
                value, detail = spec.fn(conn, schema, now)
            results.append(measure(LAYER, spec.name, value, detail, **spec.thresholds))
        except Exception as exc:
            logger.warning("Check %s could not run: %s", spec.name, exc)
            results.append(
                CheckResult(
                    LAYER,
                    spec.name,
                    "WARN",
                    None,
                    describe_thresholds(**spec.thresholds),
                    f"check error: {exc}"[:300],
                )
            )
    return results
