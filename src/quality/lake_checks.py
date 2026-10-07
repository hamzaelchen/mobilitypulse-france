"""Data-quality checks on the Parquet lake (Bronze, Silver, rejected). Runs in the Spark image.

Run: ``python -m quality.lake_checks`` (compose service ``quality-lake``). Writes
``data/quality/lake_latest.json``, merged later by ``quality.run`` into PostgreSQL / Grafana.
Thresholds are conventions, written next to each check.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common.config import StreamingSettings
from common.logging_utils import configure_logging
from quality.core import FAIL, CheckResult, format_results, measure, overall, summarize, to_json
from streaming.schemas import BRONZE_SCHEMA, SILVER_SCHEMA
from streaming.silver import BUSINESS_KEY, KAFKA_COORDINATES

logger = logging.getLogger("quality_lake")
LAYER = "lake"


def _epoch(column: str) -> F.Column:
    return F.col(column).cast("long")


def run_lake_checks(spark: SparkSession, settings: StreamingSettings, now: datetime):
    now_s = int(now.timestamp())
    bronze = spark.read.schema(BRONZE_SCHEMA).parquet(settings.bronze_path).cache()
    silver = spark.read.schema(SILVER_SCHEMA).parquet(settings.silver_path).cache()
    results: list[CheckResult] = []

    bronze_rows, silver_rows = bronze.count(), silver.count()
    results.append(measure(LAYER, "bronze_rows", float(bronze_rows), "informational"))
    results.append(measure(LAYER, "silver_rows", float(silver_rows), "informational"))

    distinct_bronze = bronze.select(*KAFKA_COORDINATES).distinct().count()
    results.append(
        measure(
            LAYER,
            "bronze_duplicate_coordinates",
            float(bronze_rows - distinct_bronze),
            "Bronze is at-least-once: a replayed batch may duplicate rows",
            warn_above=0,
        )
    )

    not_in_bronze = silver.select(*KAFKA_COORDINATES).join(
        bronze.select(*KAFKA_COORDINATES), KAFKA_COORDINATES, "left_anti"
    )
    results.append(
        measure(
            LAYER,
            "silver_rows_not_in_bronze",
            float(not_in_bronze.count()),
            "every Silver row must come from a Bronze row",
            fail_above=0,
        )
    )

    stuck = (
        bronze.select(*KAFKA_COORDINATES, "bronze_ingested_at")
        .join(silver.select(*KAFKA_COORDINATES), KAFKA_COORDINATES, "left_anti")
        .filter(_epoch("bronze_ingested_at") < now_s - 600)
    )
    results.append(
        measure(
            LAYER,
            "bronze_rows_missing_in_silver_over_10min",
            float(stuck.count()),
            "Bronze rows older than 10 min that Silver never produced",
            fail_above=0,
        )
    )

    distinct_keys = silver.select(*BUSINESS_KEY).distinct().count()
    results.append(
        measure(
            LAYER,
            "silver_duplicate_business_keys",
            float(silver_rows - distinct_keys),
            "same station twice in the same poll",
            fail_above=0,
        )
    )

    results.append(_rejected_share(spark, settings, bronze_rows))

    per_poll = bronze.groupBy("ingestion_timestamp").agg(
        F.countDistinct("station_id").alias("stations")
    )
    stats = per_poll.agg(F.min("stations"), F.max("stations"), F.count("*")).first()
    ratio = (stats[0] / stats[1]) if stats[1] else None
    results.append(
        measure(
            LAYER,
            "stations_per_poll_min_ratio",
            ratio,
            f"fewest / most stations in a poll, over {stats[2]} polls",
            warn_below=0.95,
            fail_below=0.5,
        )
    )

    polls = per_poll.select("ingestion_timestamp")
    gaps = polls.withColumn(
        "minutes",
        (
            _epoch("ingestion_timestamp")
            - F.lag(_epoch("ingestion_timestamp")).over(Window.orderBy("ingestion_timestamp"))
        )
        / 60,
    )
    gap_stats = gaps.agg(
        F.max("minutes"), F.sum((F.col("minutes") > 3).cast("int")).alias("n")
    ).first()
    # History, not the present: a past hole is a WARN. Current staleness is checked below.
    results.append(
        measure(
            LAYER,
            "poll_gap_max_minutes",
            gap_stats[0],
            f"{int(gap_stats[1] or 0)} gap(s) > 3 min in the retained Bronze history",
            warn_above=3,
        )
    )

    last_bronze = bronze.agg(F.max("ingestion_timestamp").cast("long")).first()[0]
    last_silver = silver.agg(F.max("ingestion_timestamp").cast("long")).first()[0]
    results.append(
        measure(
            LAYER,
            "bronze_freshness_minutes",
            None if last_bronze is None else (now_s - last_bronze) / 60,
            "age of the newest Bronze poll",
            warn_above=5,
            fail_above=30,
        )
    )
    lag = None if last_bronze is None or last_silver is None else (last_bronze - last_silver) / 60
    results.append(
        measure(
            LAYER,
            "silver_lag_minutes",
            lag,
            "newest Bronze poll minus newest Silver poll",
            warn_above=5,
            fail_above=30,
        )
    )
    return results


def _rejected_share(
    spark: SparkSession, settings: StreamingSettings, bronze_rows: int
) -> CheckResult:
    try:
        rejected = spark.read.parquet(settings.rejected_path).count()
    except AnalysisException:
        rejected = 0  # nothing was ever rejected
    total = rejected + bronze_rows
    value = rejected / total if total else None
    return measure(
        LAYER,
        "rejected_share",
        value,
        f"{rejected} rejected of {total} messages (includes deliberate test injections)",
        warn_above=0.01,
        fail_above=0.05,
    )


def main() -> int:
    configure_logging()
    settings = StreamingSettings.from_env()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("mobilitypulse-quality-lake")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    now = datetime.now(UTC)
    try:
        results = run_lake_checks(spark, settings, now)
    finally:
        spark.stop()
    out = Path(settings.data_dir) / "quality" / "lake_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(to_json(results, checked_at=now.isoformat()) + "\n", encoding="utf-8")
    print(format_results(results))
    print(f"lake report: overall {overall(results)} - {summarize(results)} -> {out}")
    return 1 if any(r.status == FAIL for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
