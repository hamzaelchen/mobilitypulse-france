"""Business logic: turn GBFS station payloads into validated station-status events.

Pure functions only (no I/O) so they are trivially unit-testable.
The event contract (field names/types) is mirrored by the Spark schema in
``streaming/schemas.py``; keep both in sync.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class RejectedStation:
    station_id: str | None
    reason: str


def to_iso(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, UTC).isoformat(timespec="seconds")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _optional_int(value: Any) -> int | None:
    return value if _is_int(value) and value >= 0 else None


def build_event(
    info: dict[str, Any],
    status: dict[str, Any],
    *,
    source: str,
    feed_last_updated: int | None,
    ingestion_time: datetime,
) -> dict[str, Any]:
    """Return an event dict, or raise ``ValueError`` with a short machine-readable reason."""
    station_id = info.get("station_id")
    if not isinstance(station_id, str) or not station_id.strip():
        raise ValueError("invalid_station_id")
    name = info.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("invalid_station_name")

    lat, lon = info.get("lat"), info.get("lon")
    if not (_is_number(lat) and _is_number(lon)):
        raise ValueError("missing_coordinates")
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError("coordinates_out_of_range")

    bikes, docks = status.get("num_bikes_available"), status.get("num_docks_available")
    if not (_is_int(bikes) and _is_int(docks)):
        raise ValueError("missing_availability")
    if bikes < 0 or docks < 0:
        raise ValueError("negative_availability")

    flags = {key: status.get(key) for key in ("is_installed", "is_renting", "is_returning")}
    if not all(isinstance(v, bool) for v in flags.values()):
        raise ValueError("missing_operational_flags")

    # Measure time: the station's own last_reported, else the feed publication time.
    measured_at = status.get("last_reported")
    if not (_is_number(measured_at) and measured_at > 0):
        measured_at = feed_last_updated
    if not (_is_number(measured_at) and measured_at > 0):
        raise ValueError("missing_event_timestamp")

    return {
        "source": source,
        "station_id": station_id.strip(),
        "station_name": name.strip(),
        "latitude": float(lat),
        "longitude": float(lon),
        "capacity": _optional_int(info.get("capacity")),
        "num_bikes_available": bikes,
        "num_docks_available": docks,
        "num_bikes_disabled": _optional_int(status.get("num_bikes_disabled")),
        "num_docks_disabled": _optional_int(status.get("num_docks_disabled")),
        **flags,
        "event_timestamp": to_iso(measured_at),
        "ingestion_timestamp": ingestion_time.astimezone(UTC).isoformat(timespec="milliseconds"),
    }


def build_events(
    information_payload: dict[str, Any],
    status_payload: dict[str, Any],
    *,
    source: str,
    ingestion_time: datetime,
) -> tuple[list[dict[str, Any]], list[RejectedStation]]:
    """Join station_information and station_status on ``station_id`` and validate each row."""
    infos = {
        s["station_id"]: s
        for s in information_payload.get("data", {}).get("stations", [])
        if isinstance(s, dict) and isinstance(s.get("station_id"), str)
    }
    feed_last_updated = status_payload.get("last_updated")
    events: list[dict[str, Any]] = []
    rejected: list[RejectedStation] = []

    for status in status_payload.get("data", {}).get("stations", []):
        station_id = status.get("station_id") if isinstance(status, dict) else None
        info = infos.get(station_id)
        if info is None:
            rejected.append(RejectedStation(station_id, "unknown_station"))
            continue
        try:
            events.append(
                build_event(
                    info,
                    status,
                    source=source,
                    feed_last_updated=feed_last_updated,
                    ingestion_time=ingestion_time,
                )
            )
        except ValueError as exc:
            rejected.append(RejectedStation(station_id, str(exc)))
    return events, rejected
