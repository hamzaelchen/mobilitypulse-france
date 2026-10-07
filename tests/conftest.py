import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from helpers import skip_or_fail

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gbfs"


def java_ok() -> bool:
    """Spark 3.5 needs Java 11 or 17; the Spark tests only run where it is installed."""
    if not shutil.which("java"):
        return False
    out = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr
    return any(f'"{v}.' in out for v in ("11", "17"))


@pytest.fixture
def gbfs_info() -> dict:
    return json.loads((FIXTURE_DIR / "station_information.json").read_text(encoding="utf-8"))


@pytest.fixture
def gbfs_status() -> dict:
    return json.loads((FIXTURE_DIR / "station_status.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    if not java_ok():
        skip_or_fail("Java 11/17 required for Spark 3.5")
    from pyspark.sql import SparkSession

    # Python <-> Spark datetime conversions use the process timezone: pin it to UTC.
    os.environ["TZ"] = "UTC"
    time.tzset()
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
