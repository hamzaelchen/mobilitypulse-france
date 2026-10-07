"""Runtime configuration, read from environment variables only."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class ConfigError(ValueError):
    """Raised when a required setting is missing or malformed."""


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    try:
        value = float(raw) if raw else default
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be > 0, got {value}")
    return value


@dataclass(frozen=True)
class ProducerSettings:
    discovery_url: str
    source_name: str
    language: str
    fixture_dir: Path | None
    poll_interval_seconds: float
    http_timeout_seconds: float
    station_info_refresh_seconds: float
    kafka_bootstrap_servers: str
    kafka_topic: str

    @classmethod
    def from_env(cls) -> ProducerSettings:
        fixture_dir = os.environ.get("GBFS_FIXTURE_DIR", "").strip()
        # The discovery URL is only mandatory when we really call the API.
        discovery_url = (
            os.environ.get("GBFS_DISCOVERY_URL", "").strip()
            if fixture_dir
            else _required("GBFS_DISCOVERY_URL")
        )
        return cls(
            discovery_url=discovery_url,
            source_name=_required("GBFS_SOURCE_NAME"),
            language=os.environ.get("GBFS_LANGUAGE", "fr").strip() or "fr",
            fixture_dir=Path(fixture_dir) if fixture_dir else None,
            poll_interval_seconds=_positive_float("POLL_INTERVAL_SECONDS", 60),
            http_timeout_seconds=_positive_float("HTTP_TIMEOUT_SECONDS", 10),
            station_info_refresh_seconds=_positive_float("STATION_INFO_REFRESH_SECONDS", 3600),
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_topic=_required("KAFKA_TOPIC"),
        )


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "mobility"
    reader_user: str = "grafana_ro"
    reader_password: str = ""

    @classmethod
    def from_env(cls) -> PostgresSettings:
        raw_port = os.environ.get("POSTGRES_PORT", "5432").strip()
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise ConfigError(f"POSTGRES_PORT must be an integer, got {raw_port!r}") from exc
        return cls(
            host=os.environ.get("POSTGRES_HOST", "localhost").strip(),
            port=port,
            database=_required("POSTGRES_DB"),
            user=_required("POSTGRES_USER"),
            password=_required("POSTGRES_PASSWORD"),
            schema=os.environ.get("POSTGRES_SCHEMA", "mobility").strip(),
            reader_user=os.environ.get("GRAFANA_DB_USER", "grafana_ro").strip(),
            reader_password=os.environ.get("GRAFANA_DB_PASSWORD", "").strip(),
        )

    def __repr__(self) -> str:  # never print secrets, even by accident in a log or traceback
        return (
            f"PostgresSettings(host={self.host!r}, port={self.port}, database={self.database!r}, "
            f"user={self.user!r}, schema={self.schema!r})"
        )


@dataclass(frozen=True)
class StreamingSettings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    data_dir: str
    trigger_interval: str
    max_offsets_per_trigger: int
    starting_offsets: str
    silver_watermark_delay: str = "24 hours"
    silver_max_files_per_trigger: int = 50
    gold_series_days: int = 7

    @classmethod
    def from_env(cls) -> StreamingSettings:
        return cls(
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_topic=_required("KAFKA_TOPIC"),
            data_dir=os.environ.get("DATA_DIR", "/data").rstrip("/"),
            trigger_interval=os.environ.get("SPARK_TRIGGER_INTERVAL", "30 seconds"),
            max_offsets_per_trigger=int(os.environ.get("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000")),
            starting_offsets=os.environ.get("SPARK_STARTING_OFFSETS", "earliest"),
            silver_watermark_delay=os.environ.get("SILVER_WATERMARK_DELAY", "24 hours"),
            silver_max_files_per_trigger=int(os.environ.get("SILVER_MAX_FILES_PER_TRIGGER", "50")),
            gold_series_days=int(os.environ.get("GOLD_SERIES_DAYS", "7")),
        )

    @property
    def bronze_path(self) -> str:
        return f"{self.data_dir}/bronze/station_status"

    @property
    def rejected_path(self) -> str:
        return f"{self.data_dir}/rejected/station_status"

    @property
    def checkpoint_path(self) -> str:
        return f"{self.data_dir}/checkpoints/bronze_station_status"

    @property
    def silver_path(self) -> str:
        return f"{self.data_dir}/silver/station_status"

    @property
    def silver_checkpoint_path(self) -> str:
        return f"{self.data_dir}/checkpoints/silver_station_status"

    @property
    def gold_current_path(self) -> str:
        return f"{self.data_dir}/gold/station_current"

    @property
    def gold_series_path(self) -> str:
        return f"{self.data_dir}/gold/station_series_15min"

    @property
    def gold_hourly_path(self) -> str:
        return f"{self.data_dir}/gold/station_hourly_rates"

    @property
    def ml_training_path(self) -> str:
        return f"{self.data_dir}/ml/training_set"

    @property
    def ml_reports_path(self) -> str:
        return f"{self.data_dir}/ml/reports"

    @property
    def ml_models_path(self) -> str:
        return f"{self.data_dir}/ml/models"
