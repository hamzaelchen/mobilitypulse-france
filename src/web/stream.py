"""Server-Sent Events fan-out: one shared poller, many clients (no I/O of its own).

A single task reads the state through ``read_state`` (blocking, run in a thread) every
``interval`` seconds, only while somebody is connected, and pushes to each client:

* ``snapshot``: the full state (first message, after a database outage, or for a client that
  fell behind);
* ``update``: KPIs plus only the stations that changed, and the priority/transfer lists only
  when they changed;
* ``status``: sent once when the database becomes unavailable.

Nothing is sent while nothing changes. Per-client queues are bounded: a slow client is
resynchronised with a fresh snapshot instead of slowing the others down.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from web.snapshot import diff_stations, kpi_signature

logger = logging.getLogger("web.stream")

# Change every poll without meaning anything: the client derives the age from observed_at.
VOLATILE_KPIS = frozenset(
    {"server_time", "data_age_seconds", "data_age_minutes", "heartbeat_age_seconds"}
)
DB_DOWN = {"freshness": "unknown", "cause": "database_unavailable"}

Message = tuple[str, dict[str, Any]]


@dataclass(eq=False)
class Subscription:
    queue: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)


def _stable(kpis: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in kpis.items() if key not in VOLATILE_KPIS}


class Broadcaster:
    def __init__(
        self,
        read_state: Callable[[], dict[str, Any]],
        *,
        interval: float = 2.0,
        queue_size: int = 8,
    ) -> None:
        self._read_state = read_state
        self._interval = interval
        self._queue_size = queue_size
        self._subs: set[Subscription] = set()
        self._previous: dict[str, Any] | None = None
        self._db_down = False
        self._wakeup = asyncio.Event()

    @property
    def subscribers(self) -> int:
        return len(self._subs)

    def subscribe(self) -> Subscription:
        sub = Subscription(asyncio.Queue(self._queue_size))
        if self._previous is not None:
            sub.queue.put_nowait(self._snapshot(self._previous))
        self._subs.add(sub)
        self._wakeup.set()
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        self._subs.discard(sub)
        if not self._subs:
            self._previous = None  # never greet the next client with an old snapshot

    @staticmethod
    def _snapshot(state: dict[str, Any]) -> Message:
        return "snapshot", {
            "kpis": state["kpis"],
            "stations": list(state["stations"].values()),
            "priorities": state["priorities"],
            "transfers": state["transfers"],
        }

    def _publish(self, message: Message) -> None:
        for sub in self._subs:
            try:
                sub.queue.put_nowait(message)
            except asyncio.QueueFull:
                while not sub.queue.empty():
                    sub.queue.get_nowait()
                resync = self._snapshot(self._previous) if self._previous else message
                sub.queue.put_nowait(resync)

    def _diff(self, new: dict[str, Any]) -> Message | None:
        old = self._previous
        if old is None:
            return self._snapshot(new)
        diff = diff_stations(old["stations"], new["stations"])
        data: dict[str, Any] = {"kpis": new["kpis"], **diff}
        changed = bool(diff["changed"] or diff["removed"])
        if kpi_signature(_stable(new["kpis"])) != kpi_signature(_stable(old["kpis"])):
            changed = True
        for key in ("priorities", "transfers"):
            if new[key] != old[key]:
                data[key] = new[key]
                changed = True
        return ("update", data) if changed else None

    async def tick(self) -> None:
        try:
            new = await asyncio.to_thread(self._read_state)
        except Exception as exc:  # noqa: BLE001 - any read failure means "database unavailable"
            logger.warning("State read failed: %s", type(exc).__name__)
            if not self._db_down:
                self._db_down = True
                self._previous = None
                self._publish(("status", dict(DB_DOWN)))
            return
        self._db_down = False
        message = self._diff(new)
        self._previous = new
        if message is not None:
            self._publish(message)

    async def run(self) -> None:
        while True:
            if not self._subs:
                self._wakeup.clear()
                await self._wakeup.wait()
                continue
            await self.tick()
            await asyncio.sleep(self._interval)
