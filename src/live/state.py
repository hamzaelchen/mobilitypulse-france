"""Incremental current-station state, with the same semantics as the Gold ``current_state``.

Pure logic (no I/O). A reading is the Silver row of one station at one poll. The state keeps,
per station, the newest reading plus ``first_observed_at`` and ``last_change_at``:

* a reading is a *change* when one of ``CHANGE_FIELDS`` differs from the previous reading
  (null-safe; the very first reading of a station is not a change);
* the time axis is ``observed_at`` (poll time), never the GBFS ``last_reported``;
* readings are applied in ``(observed_at, kafka_offset)`` order and anything not newer than the
  stored row is ignored, so replaying the same files is harmless (idempotent, newest wins).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

CHANGE_FIELDS = (
    "capacity",
    "bikes",
    "docks",
    "bikes_disabled",
    "docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
)

Key = tuple[str, str]  # (source, station_id)


@dataclass(frozen=True)
class Reading:
    source: str
    station_id: str
    station_name: str | None
    latitude: float | None
    longitude: float | None
    capacity: int | None
    bikes: int | None
    docks: int | None
    bikes_disabled: int | None
    docks_disabled: int | None
    is_installed: bool | None
    is_renting: bool | None
    is_returning: bool | None
    last_reported_at: datetime | None
    observed_at: datetime
    kafka_offset: int = 0

    @property
    def key(self) -> Key:
        return (self.source, self.station_id)

    @property
    def order(self) -> tuple[datetime, int]:
        return (self.observed_at, self.kafka_offset)


@dataclass(frozen=True)
class StationRow(Reading):
    first_observed_at: datetime | None = None
    last_change_at: datetime | None = None

    @property
    def is_empty(self) -> bool | None:
        return None if self.bikes is None else self.bikes == 0

    @property
    def is_full(self) -> bool | None:
        return None if self.docks is None else self.docks == 0

    @property
    def is_operational(self) -> bool | None:
        flags = (self.is_installed, self.is_renting, self.is_returning)
        return None if None in flags else all(flags)

    @property
    def fill_rate(self) -> float | None:
        if self.capacity is None or self.capacity <= 0 or self.bikes is None:
            return None
        return self.bikes / self.capacity


def _changed(previous: Reading, current: Reading) -> bool:
    return any(getattr(previous, f) != getattr(current, f) for f in CHANGE_FIELDS)


def _first(reading: Reading) -> StationRow:
    return StationRow(**vars(reading), first_observed_at=reading.observed_at, last_change_at=None)


def _next(row: StationRow, reading: Reading) -> StationRow:
    last_change_at = reading.observed_at if _changed(row, reading) else row.last_change_at
    return StationRow(
        **vars(reading), first_observed_at=row.first_observed_at, last_change_at=last_change_at
    )


def apply_readings(state: dict[Key, StationRow], readings: list[Reading]) -> dict[Key, StationRow]:
    """Return the rows that changed: stations that received a reading newer than the stored one.

    ``state`` is not modified; the caller merges the returned rows into it.
    """
    updated: dict[Key, StationRow] = {}
    for reading in sorted(readings, key=lambda r: r.order):
        current = updated.get(reading.key) or state.get(reading.key)
        if current is None:
            updated[reading.key] = _first(reading)
        elif reading.order > current.order:
            updated[reading.key] = _next(current, reading)
    return updated


def state_times(row: StationRow, as_of: datetime) -> tuple[float, float, bool]:
    """``(minutes_since_observed, hours_in_current_state, state_age_is_lower_bound)``.

    Without an observed change the time in state is counted from the first observation, so it
    is only a lower bound.
    """
    since = row.last_change_at or row.first_observed_at or row.observed_at
    minutes_since_observed = (as_of - row.observed_at).total_seconds() / 60
    hours_in_state = (as_of - since).total_seconds() / 3600
    return minutes_since_observed, hours_in_state, row.last_change_at is None


__all__ = ["CHANGE_FIELDS", "Reading", "StationRow", "apply_readings", "state_times"]
