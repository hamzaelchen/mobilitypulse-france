"""Live updater: keeps ``station_current`` and the rebalancing priorities fresh between Gold runs.

Each step tails the new Silver files (see ``live.cycle``), recomputes the rebalancing when a new
reading arrived, and loads the Gold series/hourly tables when the Gold job wrote new files. It
never loads ``station_current`` from Gold: that table belongs to the live path.

Run: ``python -m live.updater`` (loop, stops cleanly on SIGTERM/SIGINT).
"""

from __future__ import annotations

import contextlib
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from common.config import ConfigError, PostgresSettings
from common.logging_utils import configure_logging, log_fields
from live import store
from live.cycle import COMPONENT, CycleResult, LiveState, run_cycle
from live.settings import LiveSettings
from loader.gold_loader import load_gold, specs_without_current
from loader.main import connect
from prescriptive.job import StaleDataError
from prescriptive.job import run as run_rebalancing
from prescriptive.scoring import Params

logger = logging.getLogger("live.updater")
GOLD_DATASETS = ("station_series_15min", "station_hourly_rates")
MAX_BACKOFF_SECONDS = 30


@dataclass(frozen=True)
class StepResult:
    cycle: CycleResult
    rebalanced: bool = False
    rebalance_error: str | None = None
    gold_loaded: bool = False


def gold_marker(gold_dir: Path) -> float | None:
    """Newest modification time among the Gold files this updater loads (None: no files)."""
    newest = None
    for name in GOLD_DATASETS:
        for path in (gold_dir / name).rglob("*.parquet"):
            if any(part.startswith(("_", ".")) for part in path.relative_to(gold_dir).parts):
                continue
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            newest = mtime if newest is None else max(newest, mtime)
    return newest


class Updater:
    def __init__(self, conn, schema: str, settings: LiveSettings) -> None:
        self.conn = conn
        self.schema = schema
        self.settings = settings
        self.state = LiveState.load(conn, schema)
        self._gold_marker: float | None = None

    def step(self, now: datetime) -> StepResult:
        cycle = run_cycle(
            self.conn,
            self.schema,
            self.settings.silver_root,
            self.state,
            now=now,
            settle_seconds=self.settings.settle_seconds,
            overlap=self.settings.overlap,
            bootstrap=self.settings.bootstrap,
            prune_after=self.settings.prune_after,
        )
        gold_loaded = self._load_gold_if_new()
        rebalanced, error = False, None
        if cycle.updated:
            rebalanced, error = self._rebalance(now)
        return StepResult(cycle, rebalanced, error, gold_loaded)

    def _rebalance(self, now: datetime) -> tuple[bool, str | None]:
        try:
            run_rebalancing(self.conn, self.schema, Params(), now=now)
        except StaleDataError as exc:  # the guard refuses to recommend from old data
            logger.warning("Rebalancing skipped: %s", exc)
            return False, f"stale data: {exc}"
        return True, None

    def _load_gold_if_new(self) -> bool:
        marker = gold_marker(self.settings.gold_dir)
        if marker is None or marker == self._gold_marker:
            return False
        results = load_gold(
            self.conn, self.schema, self.settings.gold_dir, specs=specs_without_current()
        )
        self._gold_marker = marker
        logger.info(
            "Gold loaded",
            extra=log_fields(
                tables=[r.table for r in results], rows=sum(r.rows_read for r in results)
            ),
        )
        return True


def _beat_error(conn, schema: str, error: Exception) -> None:
    """Best effort: tell the UI the updater is alive but failing (never raise)."""
    with contextlib.suppress(psycopg.Error):
        store.write_heartbeat(
            conn, schema, COMPONENT, datetime.now(UTC), {"error": type(error).__name__}
        )


def run_forever(
    pg: PostgresSettings, settings: LiveSettings, stop: threading.Event
) -> None:  # pragma: no cover - exercised by the running service
    attempt = 0
    while not stop.is_set():
        try:
            with connect(pg) as conn:
                conn.autocommit = True
                updater = Updater(conn, pg.schema, settings)
                attempt = 0
                logger.info("Live updater started", extra=log_fields(rows=len(updater.state.rows)))
                while not stop.is_set():
                    try:
                        started = time.perf_counter()
                        step = updater.step(datetime.now(UTC))
                        elapsed_ms = round((time.perf_counter() - started) * 1000)
                        if step.cycle.updated or step.gold_loaded or step.rebalance_error:
                            logger.info(
                                "Step",
                                extra=log_fields(
                                    files=step.cycle.files,
                                    updated=step.cycle.updated,
                                    pruned=step.cycle.pruned,
                                    rebalanced=step.rebalanced,
                                    gold_loaded=step.gold_loaded,
                                    elapsed_ms=elapsed_ms,
                                ),
                            )
                    except psycopg.OperationalError:
                        raise  # lost connection: reconnect in the outer loop
                    except Exception as exc:  # noqa: BLE001 - keep serving, show it in the heartbeat
                        logger.exception("Cycle failed")
                        _beat_error(conn, pg.schema, exc)
                    stop.wait(settings.interval_seconds)
        except psycopg.Error as exc:
            attempt += 1
            delay = min(2**attempt, MAX_BACKOFF_SECONDS)
            logger.error("Database unavailable (%s), retrying in %ss", type(exc).__name__, delay)
            stop.wait(delay)


def main() -> int:  # pragma: no cover
    configure_logging()
    try:
        pg = PostgresSettings.from_env()
        settings = LiveSettings.from_env(os.environ)
    except ConfigError as exc:
        logger.error("Invalid configuration: %s", exc)
        return 2
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    run_forever(pg, settings, stop)
    logger.info("Live updater stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
