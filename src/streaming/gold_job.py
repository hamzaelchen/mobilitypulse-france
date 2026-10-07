"""Batch job: Silver Parquet -> Gold Parquet (current state, 15-minute series, hourly rates).

Run: ``python -m streaming.gold_job [--as-of ISO_DATETIME] [--every SECONDS]``
(compose service ``gold-job``, profile ``tools``). Each run recomputes the last
``GOLD_SERIES_DAYS`` days; older partitions are left untouched.
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from common.config import StreamingSettings
from common.logging_utils import configure_logging, log_fields
from streaming.gold import current_state, hourly_rates, station_series_15min
from streaming.schemas import SILVER_SCHEMA

logger = logging.getLogger("gold_job")


def run_gold(spark: SparkSession, settings: StreamingSettings, as_of: datetime) -> dict[str, int]:
    silver = spark.read.schema(SILVER_SCHEMA).parquet(settings.silver_path)
    if silver.isEmpty():
        logger.warning("Silver is empty, Gold left untouched")
        return {}

    current = current_state(silver, as_of).cache()
    series = station_series_15min(silver, as_of, settings.gold_series_days).cache()
    hourly = hourly_rates(series)
    try:
        current.coalesce(1).write.mode("overwrite").parquet(settings.gold_current_path)
        # Dynamic overwrite: only the partitions present in the new data are replaced.
        series.repartition("slot_date").write.mode("overwrite").partitionBy("slot_date").parquet(
            settings.gold_series_path
        )
        hourly.repartition("hour_date").write.mode("overwrite").partitionBy("hour_date").parquet(
            settings.gold_hourly_path
        )
        counts = {
            "station_current": current.count(),
            "station_series_15min": series.count(),
            "station_hourly_rates": hourly.count(),
        }
    finally:
        current.unpersist()
        series.unpersist()
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="ISO datetime (UTC if no offset); default: now")
    parser.add_argument("--every", type=float, help="repeat every N seconds instead of once")
    args = parser.parse_args()

    configure_logging()
    settings = StreamingSettings.from_env()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("mobilitypulse-gold")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        while True:
            if args.as_of:
                as_of = datetime.fromisoformat(args.as_of)
                as_of = as_of if as_of.tzinfo else as_of.replace(tzinfo=UTC)
            else:
                as_of = datetime.now(UTC)
            counts = run_gold(spark, settings, as_of)
            logger.info("Gold refreshed", extra=log_fields(as_of=as_of.isoformat(), **counts))
            if not args.every:
                return 0
            time.sleep(args.every)
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
