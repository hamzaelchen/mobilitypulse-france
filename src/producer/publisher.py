"""Kafka publishing."""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

from confluent_kafka import KafkaError, Message, Producer

from common.logging_utils import log_fields

logger = logging.getLogger(__name__)


class Publisher(Protocol):
    def publish(self, key: str, value: bytes) -> None: ...

    def flush(self, timeout_seconds: float) -> int: ...


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self.delivery_errors = 0
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "mobilitypulse-producer",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 50,
                "message.timeout.ms": 30000,
                "error_cb": self._on_error,
            }
        )

    @staticmethod
    def _on_error(err: KafkaError) -> None:
        logger.warning("Kafka client error: %s", err, extra=log_fields(kafka_error=err.name()))

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.delivery_errors += 1
            logger.error(
                "Kafka delivery failed",
                extra=log_fields(error=str(err), topic=msg.topic(), key=msg.key()),
            )

    def publish(self, key: str, value: bytes) -> None:
        try:
            self._producer.produce(self._topic, key=key, value=value, on_delivery=self._on_delivery)
        except BufferError:
            self._producer.poll(1.0)  # local queue full: serve callbacks, then retry once
            self._producer.produce(self._topic, key=key, value=value, on_delivery=self._on_delivery)
        self._producer.poll(0)

    def flush(self, timeout_seconds: float) -> int:
        """Return the number of messages still undelivered after the timeout."""
        return self._producer.flush(timeout_seconds)


def encode_event(event: dict[str, Any]) -> bytes:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
