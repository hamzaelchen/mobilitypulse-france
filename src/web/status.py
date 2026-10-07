"""Honest freshness of the displayed data (pure logic, no I/O).

The UI must never present stale data as fresh. The age is measured on ``observed_at`` (poll
time) and, when the data is not live, a cause is derived from the live-updater heartbeat:
an updater that keeps beating while nothing new arrives means the collection (GBFS feed, producer,
Kafka or Spark) stopped; a silent updater means the updater itself is down.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

LIVE_MAX_SECONDS = 150
DELAYED_MAX_SECONDS = 600
HEARTBEAT_MAX_SECONDS = 60


def classify_freshness(age_seconds: float | None) -> str:
    if age_seconds is None:
        return "unknown"
    if age_seconds <= LIVE_MAX_SECONDS:
        return "live"
    if age_seconds <= DELAYED_MAX_SECONDS:
        return "delayed"
    return "stale"


def pipeline_status(
    now: datetime,
    observed_at: datetime | None,
    heartbeat_at: datetime | None,
    updater_error: str | None = None,
) -> dict[str, Any]:
    data_age = None if observed_at is None else (now - observed_at).total_seconds()
    heartbeat_age = None if heartbeat_at is None else (now - heartbeat_at).total_seconds()
    freshness = classify_freshness(data_age)
    cause = None
    if observed_at is None:
        cause = "no_data"
    elif freshness != "live":
        updater_alive = heartbeat_age is not None and heartbeat_age <= HEARTBEAT_MAX_SECONDS
        if not updater_alive:
            cause = "updater_down"
        elif updater_error:
            cause = "updater_error"
        else:
            cause = "collection_stopped"
    return {
        "freshness": freshness,
        "cause": cause,
        "data_age_seconds": data_age,
        "heartbeat_age_seconds": heartbeat_age,
        "observed_at": observed_at,
        "updater_error": updater_error,
    }


def find_gaps(poll_times: list[datetime], min_gap_seconds: float) -> list[dict[str, Any]]:
    """Consecutive polls further apart than ``min_gap_seconds`` (a collection gap)."""
    ordered = sorted(poll_times)
    gaps = []
    for start, end in zip(ordered, ordered[1:], strict=False):
        seconds = (end - start).total_seconds()
        if seconds > min_gap_seconds:
            gaps.append({"start": start, "end": end, "seconds": seconds})
    return gaps
