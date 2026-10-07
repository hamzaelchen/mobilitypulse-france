"""GBFS -> Kafka producer.

Run: ``python -m producer.main`` (``--once`` for a single poll).
"""

from __future__ import annotations

import argparse
import logging
import signal
import threading
import time
from datetime import UTC, datetime
from typing import Any

from common.config import ConfigError, ProducerSettings
from common.events import build_events
from common.logging_utils import configure_logging, log_fields
from producer.gbfs import FixtureGbfsSource, GbfsFetchError, GbfsSource, HttpGbfsSource
from producer.publisher import KafkaPublisher, Publisher, encode_event

logger = logging.getLogger(__name__)


class StationStatusProducer:
    def __init__(
        self,
        settings: ProducerSettings,
        source: GbfsSource,
        publisher: Publisher,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._settings = settings
        self._source = source
        self._publisher = publisher
        self._stop = stop_event or threading.Event()
        self._info_payload: dict[str, Any] | None = None
        self._info_fetched_at = 0.0

    def _station_information(self) -> dict[str, Any]:
        """Static data (names, coordinates) is cached; refreshed periodically."""
        age = time.monotonic() - self._info_fetched_at
        if self._info_payload is None or age >= self._settings.station_info_refresh_seconds:
            try:
                self._info_payload = self._source.fetch_station_information()
                self._info_fetched_at = time.monotonic()
            except GbfsFetchError:
                if self._info_payload is None:
                    raise
                logger.warning("station_information refresh failed, using cached copy")
        return self._info_payload

    def poll_once(self) -> int:
        """One cycle: fetch, validate, publish. Returns the number of events published."""
        info = self._station_information()
        status = self._source.fetch_station_status()
        events, rejected = build_events(
            info, status, source=self._settings.source_name, ingestion_time=datetime.now(UTC)
        )
        for item in rejected:
            logger.warning(
                "Station skipped: invalid record",
                extra=log_fields(station_id=item.station_id, reason=item.reason),
            )
        for event in events:
            self._publisher.publish(event["station_id"], encode_event(event))
        logger.info(
            "Poll completed",
            extra=log_fields(published=len(events), skipped=len(rejected)),
        )
        return len(events)

    def run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                self.poll_once()
                failures = 0
            except GbfsFetchError as exc:
                failures += 1
                logger.error(
                    "GBFS fetch failed, will retry next cycle",
                    extra=log_fields(error=str(exc), consecutive_failures=failures),
                )
            self._stop.wait(self._settings.poll_interval_seconds)
        remaining = self._publisher.flush(10)
        logger.info("Producer stopped", extra=log_fields(undelivered_messages=remaining))

    def stop(self) -> None:
        self._stop.set()


def build_source(settings: ProducerSettings) -> GbfsSource:
    if settings.fixture_dir is not None:
        logger.warning(
            "FIXTURE MODE: reading local files, not the live GBFS feed",
            extra=log_fields(fixture_dir=str(settings.fixture_dir)),
        )
        return FixtureGbfsSource(settings.fixture_dir)
    return HttpGbfsSource(settings.discovery_url, settings.language, settings.http_timeout_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="poll a single time and exit")
    args = parser.parse_args()

    configure_logging()
    try:
        settings = ProducerSettings.from_env()
    except ConfigError as exc:
        logger.error("Invalid configuration: %s", exc)
        return 2

    publisher = KafkaPublisher(settings.kafka_bootstrap_servers, settings.kafka_topic)
    producer = StationStatusProducer(settings, build_source(settings), publisher)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: producer.stop())

    logger.info(
        "Producer starting",
        extra=log_fields(
            topic=settings.kafka_topic,
            bootstrap=settings.kafka_bootstrap_servers,
            interval_seconds=settings.poll_interval_seconds,
            source=settings.source_name,
        ),
    )
    if args.once:
        try:
            producer.poll_once()
        except GbfsFetchError as exc:
            logger.error("GBFS fetch failed: %s", exc)
            return 1
        return 1 if publisher.flush(10) else 0
    producer.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
