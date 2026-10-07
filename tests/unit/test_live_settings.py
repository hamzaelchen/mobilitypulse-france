"""Live-updater settings and the loader's station_current opt-out."""

from datetime import timedelta
from pathlib import Path

import pytest

from common.config import ConfigError
from live.settings import LiveSettings
from loader.gold_loader import SPECS, specs_without_current


def test_defaults():
    settings = LiveSettings.from_env({})
    assert settings.interval_seconds == 10
    assert settings.settle_seconds == 2
    assert settings.overlap == timedelta(minutes=5)
    assert settings.bootstrap == timedelta(hours=6)
    assert settings.prune_after == timedelta(hours=24)
    assert settings.silver_root == Path("/data/silver/station_status")
    assert settings.gold_dir == Path("/data/gold")


def test_values_come_from_the_environment():
    settings = LiveSettings.from_env(
        {"LIVE_INTERVAL_SECONDS": "5", "LIVE_OVERLAP_SECONDS": "120", "DATA_DIR": "/x"}
    )
    assert settings.interval_seconds == 5
    assert settings.overlap == timedelta(seconds=120)
    assert settings.silver_root == Path("/x/silver/station_status")


@pytest.mark.parametrize("name", ["LIVE_INTERVAL_SECONDS", "LIVE_OVERLAP_SECONDS"])
@pytest.mark.parametrize("value", ["0", "-3", "abc"])
def test_invalid_values_are_rejected(name, value):
    with pytest.raises(ConfigError, match=name):
        LiveSettings.from_env({name: value})


def test_the_loader_can_leave_station_current_to_the_live_updater():
    names = [spec.name for spec in specs_without_current()]
    assert "station_current" not in names
    assert names == [spec.name for spec in SPECS if spec.name != "station_current"]
    assert len(names) == len(SPECS) - 1
