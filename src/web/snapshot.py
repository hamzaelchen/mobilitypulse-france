"""Light snapshot diff for the SSE stream (pure logic, no I/O)."""

from __future__ import annotations

from typing import Any

Station = dict[str, Any]


def diff_stations(previous: dict[str, Station] | None, current: dict[str, Station]) -> dict:
    """Stations to send to the client: new or modified ones, and the ids that disappeared."""
    previous = previous or {}
    changed = [row for key, row in current.items() if previous.get(key) != row]
    removed = [key for key in previous if key not in current]
    return {"changed": changed, "removed": removed}


def kpi_signature(kpis: dict[str, Any]) -> tuple:
    """Hashable fingerprint of the displayed KPIs, to push only when something changed."""
    return tuple(sorted((key, repr(value)) for key, value in kpis.items()))
