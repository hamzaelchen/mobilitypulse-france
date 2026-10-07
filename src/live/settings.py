"""Live-updater settings, read from environment variables."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from common.config import ConfigError


def _positive(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, "").strip()
    try:
        value = float(raw) if raw else default
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be > 0, got {value}")
    return value


@dataclass(frozen=True)
class LiveSettings:
    interval_seconds: float
    settle_seconds: float
    overlap: timedelta
    bootstrap: timedelta
    prune_after: timedelta
    silver_root: Path
    gold_dir: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> LiveSettings:
        data_dir = Path(env.get("DATA_DIR", "").strip() or "/data")
        return cls(
            interval_seconds=_positive(env, "LIVE_INTERVAL_SECONDS", 10),
            settle_seconds=_positive(env, "LIVE_SETTLE_SECONDS", 2),
            overlap=timedelta(seconds=_positive(env, "LIVE_OVERLAP_SECONDS", 300)),
            bootstrap=timedelta(hours=_positive(env, "LIVE_BOOTSTRAP_HOURS", 6)),
            prune_after=timedelta(hours=_positive(env, "LIVE_PRUNE_HOURS", 24)),
            silver_root=data_dir / "silver" / "station_status",
            gold_dir=data_dir / "gold",
        )
