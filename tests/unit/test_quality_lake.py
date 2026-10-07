"""Lake quality checks on a hand-built Bronze / Silver / rejected lake. Needs Java (Spark image)."""

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from common.config import StreamingSettings  # noqa: E402
from quality.core import FAIL, PASS, WARN  # noqa: E402
from quality.lake_checks import run_lake_checks  # noqa: E402
from streaming.schemas import BRONZE_SCHEMA, SILVER_SCHEMA  # noqa: E402

T0 = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
STATIONS = ("1", "2", "3")


def row(station, minute, offset, *, bronze_age_min=0):
    observed = T0 + timedelta(minutes=minute)
    ingested = observed + timedelta(minutes=bronze_age_min)
    values = {
        "source": "t", "station_id": station, "station_name": f"S{station}",
        "latitude": 47.2, "longitude": -1.55, "capacity": 20, "num_bikes_available": 5,
        "num_docks_available": 15, "num_bikes_disabled": 0, "num_docks_disabled": 0,
        "is_installed": True, "is_renting": True, "is_returning": True,
        "event_timestamp": T0, "ingestion_timestamp": observed, "kafka_key": station,
        "kafka_topic": "t", "kafka_partition": 0, "kafka_offset": offset,
        "kafka_timestamp": observed, "bronze_ingested_at": ingested,
        "ingestion_date": observed.date(), "fill_rate": 0.25, "is_empty": False,
        "is_full": False, "is_operational": True, "observed_at": observed,
        "silver_processed_at": ingested,
    }  # fmt: skip
    return values


def polls(minutes, stations=STATIONS, first_offset=0):
    rows, offset = [], first_offset
    for minute in minutes:
        for station in stations:
            rows.append(row(station, minute, offset))
            offset += 1
    return rows


def write(spark, rows, schema, path):
    data = [tuple(r[f.name] for f in schema.fields) for r in rows]
    (
        spark.createDataFrame(data, schema)
        .write.mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(str(path))
    )


def settings_for(tmp_path):
    return StreamingSettings(
        kafka_bootstrap_servers="unused",
        kafka_topic="t",
        data_dir=str(tmp_path),
        trigger_interval="1 second",
        max_offsets_per_trigger=1,
        starting_offsets="earliest",
    )


def build_lake(spark, tmp_path, bronze, silver=None):
    settings = settings_for(tmp_path)
    write(spark, bronze, BRONZE_SCHEMA, settings.bronze_path)
    write(spark, bronze if silver is None else silver, SILVER_SCHEMA, settings.silver_path)
    return settings


def results_of(spark, settings, now):
    return {r.name: r for r in run_lake_checks(spark, settings, now)}


def test_a_healthy_lake_passes_every_check(spark, tmp_path):
    settings = build_lake(spark, tmp_path, polls([0, 1, 2]))
    results = results_of(spark, settings, T0 + timedelta(minutes=3))
    assert {n: r.status for n, r in results.items() if r.status != PASS} == {}
    assert results["bronze_rows"].value == 9 and results["silver_rows"].value == 9
    assert results["stations_per_poll_min_ratio"].value == 1.0
    assert results["poll_gap_max_minutes"].value == pytest.approx(1.0)
    assert results["bronze_freshness_minutes"].value == pytest.approx(1.0)
    assert results["silver_lag_minutes"].value == 0.0
    assert results["rejected_share"].value == 0.0  # no rejected dataset at all


def test_a_silver_row_that_is_not_in_bronze_fails(spark, tmp_path):
    bronze = polls([0, 1])
    silver = bronze + [row("1", 5, 999)]
    results = results_of(
        spark, build_lake(spark, tmp_path, bronze, silver), T0 + timedelta(minutes=2)
    )
    assert results["silver_rows_not_in_bronze"].status == FAIL
    assert results["silver_rows_not_in_bronze"].value == 1


def test_old_bronze_rows_missing_in_silver_fail_but_recent_ones_do_not(spark, tmp_path):
    bronze = polls([0, 1, 2])
    silver = [r for r in bronze if r["station_id"] != "2"]  # Silver never got station 2
    results = results_of(
        spark, build_lake(spark, tmp_path, bronze, silver), T0 + timedelta(minutes=30)
    )
    assert results["bronze_rows_missing_in_silver_over_10min"].status == FAIL
    assert results["bronze_rows_missing_in_silver_over_10min"].value == 3

    # The same gap right after the write is just Silver's normal latency.
    recent = results_of(
        spark, build_lake(spark, tmp_path / "r", bronze, silver), T0 + timedelta(minutes=3)
    )
    assert recent["bronze_rows_missing_in_silver_over_10min"].status == PASS


def test_replayed_bronze_batch_is_a_warning_and_silver_duplicates_fail(spark, tmp_path):
    bronze = polls([0, 1])
    replayed = bronze + bronze[:3]  # the first poll delivered twice (same Kafka coordinates)
    silver = bronze + [dict(bronze[0], kafka_offset=500)]  # same station + poll, other offset
    settings = build_lake(spark, tmp_path, replayed, silver)
    results = results_of(spark, settings, T0 + timedelta(minutes=2))
    assert results["bronze_duplicate_coordinates"].status == WARN
    assert results["bronze_duplicate_coordinates"].value == 3
    assert results["silver_duplicate_business_keys"].status == FAIL
    assert results["silver_duplicate_business_keys"].value == 1


def test_rejected_share_uses_the_rejected_dataset(spark, tmp_path):
    settings = build_lake(spark, tmp_path, polls([0, 1]))  # 6 valid messages
    rejected = spark.createDataFrame(
        [("{bad", "invalid_json", "k", "t", 0, 900 + i, T0, T0, T0.date()) for i in range(2)],
        "raw_value string, reject_reason string, kafka_key string, kafka_topic string, "
        "kafka_partition int, kafka_offset long, kafka_timestamp timestamp, "
        "rejected_at timestamp, rejection_date date",
    )
    rejected.write.mode("overwrite").partitionBy("rejection_date").parquet(settings.rejected_path)
    result = results_of(spark, settings, T0 + timedelta(minutes=2))["rejected_share"]
    assert result.value == pytest.approx(2 / 8) and result.status == FAIL
    assert "2 rejected of 8" in result.detail


def test_a_poll_with_missing_stations_and_a_collection_hole_are_reported(spark, tmp_path):
    bronze = polls([0, 1]) + polls([21], stations=("1",), first_offset=100)  # 20-min hole
    results = results_of(spark, build_lake(spark, tmp_path, bronze), T0 + timedelta(minutes=22))
    assert results["stations_per_poll_min_ratio"].value == pytest.approx(1 / 3)
    assert results["stations_per_poll_min_ratio"].status == FAIL
    gap = results["poll_gap_max_minutes"]
    assert gap.value == pytest.approx(20.0) and gap.status == WARN  # history: warn only
    assert "1 gap(s) > 3 min" in gap.detail


def test_current_staleness_fails_unlike_a_past_hole(spark, tmp_path):
    settings = build_lake(spark, tmp_path, polls([0, 1, 2]))
    results = results_of(spark, settings, T0 + timedelta(minutes=2 + 45))
    assert results["bronze_freshness_minutes"].status == FAIL
    assert results["bronze_freshness_minutes"].value == pytest.approx(45.0)


def test_silver_lag_is_measured_against_bronze(spark, tmp_path):
    bronze = polls([0, 1, 2, 3])
    silver = polls([0, 1])  # Silver stopped 2 polls behind (same offsets as Bronze's first polls)
    results = results_of(
        spark, build_lake(spark, tmp_path, bronze, silver), T0 + timedelta(minutes=4)
    )
    assert results["silver_lag_minutes"].value == pytest.approx(2.0)
    assert results["silver_lag_minutes"].status == PASS  # below the 5-minute warning
