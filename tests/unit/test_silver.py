"""Silver stream and Bronze compaction. Need Java: run inside the Spark image."""

import os
import time
from datetime import date, datetime

import pytest

pytest.importorskip("pyspark")

from common.config import StreamingSettings  # noqa: E402
from streaming.compact_bronze import compact_partition  # noqa: E402
from streaming.compaction_plan import COMPACTED_PREFIX, plan_compaction  # noqa: E402
from streaming.schemas import BRONZE_SCHEMA  # noqa: E402
from streaming.silver import SILVER_COLUMNS, add_derived_columns  # noqa: E402
from streaming.silver_job import start_silver_query  # noqa: E402

BRONZE_FILE_COLUMNS = [f.name for f in BRONZE_SCHEMA.fields if f.name != "ingestion_date"]


def bronze_row(station_id="1", offset=0, event="10:00:00", ingested="12:00:00", **overrides):
    day = "2026-10-05"
    row = {
        "source": "test",
        "station_id": station_id,
        "station_name": f"S{station_id}",
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
        "event_timestamp": datetime.fromisoformat(f"{day}T{event}"),
        "ingestion_timestamp": datetime.fromisoformat(f"{day}T{ingested}"),
        "kafka_key": station_id,
        "kafka_topic": "t",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp": datetime.fromisoformat(f"{day}T{ingested}"),
        "bronze_ingested_at": datetime.fromisoformat(f"{day}T{ingested}"),
    }
    row.update(overrides)
    return tuple(row[name] for name in BRONZE_FILE_COLUMNS)


def append_bronze_file(spark, bronze_dir, rows, day="2026-10-05"):
    schema = [f for f in BRONZE_SCHEMA.fields if f.name != "ingestion_date"]
    from pyspark.sql.types import StructType

    df = spark.createDataFrame(rows, StructType(schema))
    df.coalesce(1).write.mode("append").parquet(str(bronze_dir / f"ingestion_date={day}"))


def settings_for(tmp_path):
    return StreamingSettings(
        kafka_bootstrap_servers="unused",
        kafka_topic="t",
        data_dir=str(tmp_path),
        trigger_interval="1 second",
        max_offsets_per_trigger=1,
        starting_offsets="earliest",
        silver_max_files_per_trigger=1,
    )


def run_silver_once(spark, settings):
    query = start_silver_query(spark, settings, once=True)
    assert query.awaitTermination(120)
    return spark.read.parquet(settings.silver_path)


def test_derived_columns(spark):
    schema = "capacity int, num_bikes_available int, num_docks_available int, "
    schema += (
        "is_installed boolean, is_renting boolean, is_returning boolean, "
        "ingestion_timestamp timestamp"
    )
    ts = datetime(2026, 10, 5, 10)
    rows = [
        (10, 0, 10, True, True, True, ts),  # empty
        (10, 10, 0, True, True, True, ts),  # full
        (10, 5, 5, True, False, True, ts),  # not renting
        (0, 0, 0, True, True, True, ts),  # unknown capacity
        (None, 2, 3, True, True, True, ts),
    ]
    result = {
        (r.capacity, r.num_bikes_available): r
        for r in add_derived_columns(spark.createDataFrame(rows, schema)).collect()
    }
    assert result[(10, 0)].is_empty and not result[(10, 0)].is_full
    assert result[(10, 0)].fill_rate == 0.0
    assert result[(10, 10)].is_full and not result[(10, 10)].is_empty
    assert result[(10, 10)].fill_rate == 1.0
    assert result[(10, 5)].is_operational is False
    assert result[(10, 5)].fill_rate == 0.5
    assert result[(0, 0)].fill_rate is None
    assert result[(None, 2)].fill_rate is None
    assert result[(10, 0)].is_operational is True
    assert result[(10, 0)].observed_at == ts


def test_silver_deduplicates_replays_but_keeps_changes_under_the_same_last_reported(
    spark, tmp_path
):
    """Regression: the real feed changes counters without refreshing ``last_reported``."""
    bronze = tmp_path / "bronze" / "station_status"
    append_bronze_file(
        spark,
        bronze,
        [bronze_row("1", offset=0), bronze_row("2", offset=1)],
    )
    time.sleep(1.1)  # distinct modification times: files are consumed oldest first
    append_bronze_file(
        spark,
        bronze,
        [
            bronze_row("1", offset=0),  # replayed Kafka message: dropped
            # Counters changed, same event_timestamp (last_reported): a REAL change, kept.
            bronze_row("1", offset=5, ingested="12:01:00", num_bikes_available=4),
            # Same station delivered twice in the same poll under another offset: dropped.
            bronze_row("1", offset=7, ingested="12:01:00", num_bikes_available=4),
            # Counters back to a previous value: still a distinct reading (a new poll), kept.
            bronze_row("1", offset=8, ingested="12:02:00"),
        ],
    )
    settings = settings_for(tmp_path)
    silver = run_silver_once(spark, settings)

    assert silver.count() == 4
    readings = sorted(
        (r.station_id, r.observed_at.minute, r.num_bikes_available) for r in silver.collect()
    )
    assert readings == [("1", 0, 3), ("1", 1, 4), ("1", 2, 3), ("2", 0, 3)]
    assert {r.event_timestamp.hour for r in silver.collect()} == {10}  # last_reported unchanged
    assert set(SILVER_COLUMNS) == set(silver.columns)
    assert str(silver.first().ingestion_date) == "2026-10-05"
    assert silver.filter("is_operational and fill_rate is not null").count() == 4


def test_silver_resumes_from_its_checkpoint_without_reprocessing(spark, tmp_path):
    bronze = tmp_path / "bronze" / "station_status"
    settings = settings_for(tmp_path)
    append_bronze_file(spark, bronze, [bronze_row("1", offset=0)])
    assert run_silver_once(spark, settings).count() == 1
    time.sleep(1.1)
    append_bronze_file(spark, bronze, [bronze_row("2", offset=1)])
    assert run_silver_once(spark, settings).count() == 2
    assert run_silver_once(spark, settings).count() == 2  # nothing new: unchanged


def test_compaction_merges_small_files_and_silver_ignores_the_result(spark, tmp_path):
    bronze = tmp_path / "bronze" / "station_status"
    for i in range(3):
        append_bronze_file(spark, bronze, [bronze_row(str(i), offset=i)])
    partition = bronze / "ingestion_date=2026-10-05"
    old = time.time() - 7200
    for f in partition.glob("part-*.parquet"):
        os.utime(f, (old, old))

    settings = settings_for(tmp_path)
    assert run_silver_once(spark, settings).count() == 3

    [plan] = plan_compaction(bronze, today=date(2026, 10, 6), now=time.time())
    assert len(plan.small_files) == 3
    assert compact_partition(spark, bronze, plan) == 3

    assert list(partition.glob("part-*.parquet")) == []
    [compacted] = partition.glob(f"{COMPACTED_PREFIX}*.parquet")
    assert not list(partition.glob("_compaction_tmp"))
    assert not list(partition.glob(".part-*.crc"))
    whole = spark.read.parquet(str(bronze))  # batch readers see the compacted file
    assert whole.count() == 3
    assert "ingestion_date" in whole.columns
    assert plan_compaction(bronze, today=date(2026, 10, 6), now=time.time()) == []

    # The compacted rows must not be read again by Silver: still 3 rows, no duplicates.
    assert run_silver_once(spark, settings).count() == 3
    assert compacted.exists()


def test_compaction_rerun_after_a_crash_does_not_duplicate_rows(spark, tmp_path, monkeypatch):
    bronze = tmp_path / "bronze" / "station_status"
    for i in range(2):
        append_bronze_file(spark, bronze, [bronze_row(str(i), offset=i)])
    partition = bronze / "ingestion_date=2026-10-05"
    old = time.time() - 7200
    for f in partition.glob("part-*.parquet"):
        os.utime(f, (old, old))
    [plan] = plan_compaction(bronze, today=date(2026, 10, 6), now=time.time())

    # Crash after the compacted file is in place but before the sources are deleted.
    def crash(_path):
        raise OSError("simulated crash")

    monkeypatch.setattr("streaming.compact_bronze._delete_with_crc", crash)
    with pytest.raises(OSError):
        compact_partition(spark, bronze, plan)
    monkeypatch.undo()
    assert len(list(partition.glob("part-*.parquet"))) == 2
    assert len(list(partition.glob(f"{COMPACTED_PREFIX}*.parquet"))) == 1

    # Re-run with the same plan: the existing target is reused, then the sources go away.
    assert compact_partition(spark, bronze, plan) == 2
    assert list(partition.glob("part-*.parquet")) == []
    assert len(list(partition.glob(f"{COMPACTED_PREFIX}*.parquet"))) == 1
    assert spark.read.parquet(str(bronze)).count() == 2
