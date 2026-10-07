import json
import threading
from pathlib import Path

import pytest

from common.config import ConfigError, ProducerSettings
from producer.gbfs import FixtureGbfsSource, GbfsFetchError, resolve_feed_urls
from producer.inject_invalid import broken_messages
from producer.main import StationStatusProducer

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "gbfs"


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []
        self.flushed = False

    def publish(self, key: str, value: bytes) -> None:
        self.messages.append((key, value))

    def flush(self, timeout_seconds: float) -> int:
        self.flushed = True
        return 0


class FlakySource:
    """Fails on the first status call, then behaves like the fixture."""

    def __init__(self, inner: FixtureGbfsSource) -> None:
        self._inner = inner
        self.status_calls = 0

    def fetch_station_information(self):
        return self._inner.fetch_station_information()

    def fetch_station_status(self):
        self.status_calls += 1
        if self.status_calls == 1:
            raise GbfsFetchError("HTTP 503 on test")
        return self._inner.fetch_station_status()


def settings(**overrides) -> ProducerSettings:
    base = {
        "discovery_url": "",
        "source_name": "test-source",
        "language": "fr",
        "fixture_dir": FIXTURE_DIR,
        "poll_interval_seconds": 0.01,
        "http_timeout_seconds": 1,
        "station_info_refresh_seconds": 3600,
        "kafka_bootstrap_servers": "unused:9092",
        "kafka_topic": "t",
    }
    return ProducerSettings(**{**base, **overrides})


def test_poll_once_publishes_one_json_event_per_station_keyed_by_id():
    publisher = FakePublisher()
    producer = StationStatusProducer(settings(), FixtureGbfsSource(FIXTURE_DIR), publisher)
    assert producer.poll_once() == 12
    key, value = publisher.messages[0]
    event = json.loads(value)
    assert key == event["station_id"]
    assert event["source"] == "test-source"
    assert {"event_timestamp", "ingestion_timestamp"} <= event.keys()


def test_run_survives_a_failing_cycle_and_flushes_on_stop():
    publisher = FakePublisher()
    stop = threading.Event()
    source = FlakySource(FixtureGbfsSource(FIXTURE_DIR))
    producer = StationStatusProducer(settings(), source, publisher, stop)

    def stop_after_first_success():
        while not publisher.messages:
            stop.wait(0.01)
        stop.set()

    watcher = threading.Thread(target=stop_after_first_success)
    watcher.start()
    producer.run()
    watcher.join(5)
    assert source.status_calls >= 2  # first cycle failed, loop kept going
    assert len(publisher.messages) >= 12
    assert publisher.flushed


def test_fixture_source_missing_file_raises(tmp_path):
    with pytest.raises(GbfsFetchError):
        FixtureGbfsSource(tmp_path).fetch_station_status()


def test_resolve_feed_urls_prefers_requested_language():
    doc = {
        "version": "2.3",
        "data": {
            "en": {"feeds": [{"name": "station_status", "url": "http://x/en"}]},
            "fr": {"feeds": [{"name": "station_status", "url": "http://x/fr"}]},
        },
    }
    assert resolve_feed_urls(doc, "fr") == {"station_status": "http://x/fr"}
    assert resolve_feed_urls(doc, "de") == {"station_status": "http://x/en"}


def test_resolve_feed_urls_rejects_non_gbfs2_documents():
    with pytest.raises(GbfsFetchError):
        resolve_feed_urls({"version": "3.0", "data": {"feeds": []}}, "fr")


def test_settings_require_kafka_and_url(monkeypatch):
    for name in ("GBFS_DISCOVERY_URL", "GBFS_FIXTURE_DIR", "KAFKA_BOOTSTRAP_SERVERS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GBFS_SOURCE_NAME", "s")
    monkeypatch.setenv("KAFKA_TOPIC", "t")
    with pytest.raises(ConfigError, match="GBFS_DISCOVERY_URL"):
        ProducerSettings.from_env()
    monkeypatch.setenv("GBFS_FIXTURE_DIR", "tests/fixtures/gbfs")
    with pytest.raises(ConfigError, match="KAFKA_BOOTSTRAP_SERVERS"):
        ProducerSettings.from_env()
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
    assert ProducerSettings.from_env().fixture_dir == Path("tests/fixtures/gbfs")


def test_settings_reject_non_positive_interval(monkeypatch):
    monkeypatch.setenv("GBFS_SOURCE_NAME", "s")
    monkeypatch.setenv("GBFS_FIXTURE_DIR", "x")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "k")
    monkeypatch.setenv("KAFKA_TOPIC", "t")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0")
    with pytest.raises(ConfigError, match="POLL_INTERVAL_SECONDS"):
        ProducerSettings.from_env()


def test_injection_helper_produces_seven_distinct_broken_messages():
    messages = broken_messages()
    assert len(messages) == 7
    assert len({key for key, _ in messages}) == 7
