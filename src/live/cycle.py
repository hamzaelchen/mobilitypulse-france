"""One live-updater cycle: tail new Silver files, apply them, refresh times, beat."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from live import store
from live.silver_tail import list_new_files, read_files
from live.state import Key, StationRow, apply_readings

logger = logging.getLogger("live.cycle")
COMPONENT = "live-updater"
MAX_SEEN = 5000


@dataclass
class LiveState:
    rows: dict[Key, StationRow] = field(default_factory=dict)
    seen: set[Path] = field(default_factory=set)

    @classmethod
    def load(cls, conn, schema: str) -> LiveState:
        return cls(rows=store.load_rows(conn, schema))

    def newest(self) -> datetime | None:
        return max((row.observed_at for row in self.rows.values()), default=None)


@dataclass(frozen=True)
class CycleResult:
    files: int
    readings: int
    updated: int
    pruned: int


def run_cycle(
    conn,
    schema: str,
    silver_root: Path,
    state: LiveState,
    *,
    now: datetime,
    settle_seconds: float = 2.0,
    overlap: timedelta = timedelta(minutes=5),
    bootstrap: timedelta = timedelta(hours=6),
    prune_after: timedelta = timedelta(hours=24),
) -> CycleResult:
    """Apply what Silver wrote since the last cycle. Safe to repeat: everything is idempotent.

    The listing window starts ``overlap`` before the newest stored reading (files are written a
    few seconds to a minute after their polls); files already applied are skipped from memory.
    """
    newest = state.newest()
    since = newest - overlap if newest else now - bootstrap
    files = list_new_files(
        silver_root, since=since, now=now, settle_seconds=settle_seconds, skip=state.seen
    )
    readings, failed = read_files(files)
    updated = apply_readings(state.rows, readings)

    with conn.transaction():
        store.upsert_rows(conn, schema, list(updated.values()), now)
        pruned = store.prune_stale(conn, schema, now, prune_after)
        store.refresh_derived(conn, schema, now)
        store.write_heartbeat(
            conn,
            schema,
            COMPONENT,
            now,
            {"files": len(files), "readings": len(readings), "updated": len(updated)},
        )

    state.rows.update(updated)
    for key in pruned:
        state.rows.pop(key, None)
    state.seen.update(set(files) - set(failed))
    if len(state.seen) > MAX_SEEN:
        state.seen = set(sorted(state.seen)[-MAX_SEEN // 2 :])
    return CycleResult(len(files), len(readings), len(updated), len(pruned))
