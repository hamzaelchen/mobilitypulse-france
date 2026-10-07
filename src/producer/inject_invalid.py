"""Publish deliberately broken messages to prove the pipeline tolerates them.

Run: ``python -m producer.inject_invalid`` (needs KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC).
"""

from __future__ import annotations

import json
import logging

from common.config import ConfigError, ProducerSettings
from common.logging_utils import configure_logging, log_fields
from producer.publisher import KafkaPublisher, encode_event

logger = logging.getLogger(__name__)

_VALID_BASE = {
    "source": "test-injection",
    "station_id": "999",
    "station_name": "INJECTED",
    "latitude": 47.2,
    "longitude": -1.55,
    "capacity": 10,
    "num_bikes_available": 3,
    "num_docks_available": 7,
    "num_bikes_disabled": 0,
    "num_docks_disabled": 0,
    "is_installed": True,
    "is_renting": True,
    "is_returning": True,
    "event_timestamp": "2026-01-01T00:00:00+00:00",
    "ingestion_timestamp": "2026-01-01T00:00:01.000+00:00",
}


def broken_messages() -> list[tuple[str, bytes]]:
    """(key, payload) pairs, each broken in a different way."""
    return [
        ("bad-json", b"{this is not json"),
        ("not-an-object", b'["a", "list"]'),
        ("negative-bikes", encode_event({**_VALID_BASE, "num_bikes_available": -4})),
        ("missing-station", json.dumps({**_VALID_BASE, "station_id": None}).encode()),
        ("bad-latitude", encode_event({**_VALID_BASE, "latitude": 123.4})),
        ("bad-timestamp", encode_event({**_VALID_BASE, "event_timestamp": "yesterday"})),
        ("wrong-type", encode_event({**_VALID_BASE, "num_docks_available": "many"})),
    ]


def main() -> int:
    configure_logging()
    try:
        settings = ProducerSettings.from_env()
    except ConfigError as exc:
        logger.error("Invalid configuration: %s", exc)
        return 2
    publisher = KafkaPublisher(settings.kafka_bootstrap_servers, settings.kafka_topic)
    messages = broken_messages()
    for key, payload in messages:
        publisher.publish(key, payload)
    remaining = publisher.flush(15)
    logger.info(
        "Invalid messages sent", extra=log_fields(count=len(messages), undelivered=remaining)
    )
    return 1 if remaining else 0


if __name__ == "__main__":
    raise SystemExit(main())
