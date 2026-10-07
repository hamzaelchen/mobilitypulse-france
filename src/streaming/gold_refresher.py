"""Re-run the Gold job on a schedule, one short-lived process per cycle.

No JVM stays resident between two runs (the Spark session lives only inside ``gold_job``), so the
idle cost of this service is that of a sleeping Python process. On SIGTERM the running Gold job is
asked to stop and no new cycle starts, so ``docker compose stop`` leaves no half-written run.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from typing import Protocol

logger = logging.getLogger("gold_refresher")
CHILD_STOP_TIMEOUT_SECONDS = 25


class Stop(Protocol):
    def is_set(self) -> bool: ...
    def wait(self, timeout: float) -> bool: ...


def run_loop(run_once: Callable[[], int], interval: float, stop: Stop) -> None:
    """Run, wait ``interval`` seconds (counted after the run), repeat until ``stop``."""
    while not stop.is_set():
        try:
            code = run_once()
            if code != 0:
                logger.error("Gold run failed with exit code %s", code)
        except Exception:  # noqa: BLE001 - a failed cycle must not kill the service
            logger.exception("Gold run crashed")
        if stop.wait(interval):
            return


def main() -> int:  # pragma: no cover - exercised by the running service
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    interval = float(os.environ.get("GOLD_REFRESH_SECONDS", "900"))
    stop = threading.Event()
    child: subprocess.Popen | None = None

    def on_signal(*_: object) -> None:
        stop.set()
        if child is not None and child.poll() is None:
            child.terminate()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)

    def run_once() -> int:
        nonlocal child
        child = subprocess.Popen([sys.executable, "-m", "streaming.gold_job"])
        try:
            return child.wait()
        finally:
            child = None

    logger.info("Gold refresher started, every %ss", interval)
    run_loop(run_once, interval, stop)
    logger.info("Gold refresher stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
