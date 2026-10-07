"""Batch job: merge the small Bronze files of closed days into one file per partition.

Run: ``python -m streaming.compact_bronze [--dry-run]`` (compose service ``compact-bronze``).
Safe to re-run. Only partitions before today (UTC) are touched, and only if their files are
older than ``--min-age-seconds``, so that the Silver stream has already consumed them.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from common.config import StreamingSettings
from common.logging_utils import configure_logging, log_fields
from streaming.compaction_plan import (
    TMP_DIR_NAME,
    PartitionPlan,
    plan_compaction,
)
from streaming.schemas import BRONZE_SCHEMA

logger = logging.getLogger("compact_bronze")


def _read_rows(spark: SparkSession, bronze_dir: Path, files: list[Path]):
    # basePath keeps ``ingestion_date`` (partition column) available; files do not store it.
    return (
        spark.read.schema(BRONZE_SCHEMA)
        .option("basePath", str(bronze_dir))
        .parquet(*[str(f) for f in files])
        .drop("ingestion_date")
    )


def _delete_with_crc(path: Path) -> None:
    path.unlink()
    crc = path.with_name(f".{path.name}.crc")
    if crc.exists():
        crc.unlink()


def compact_partition(spark: SparkSession, bronze_dir: Path, plan: PartitionPlan) -> int:
    """Returns the number of rows in the partition. Raises if counts do not match."""
    sources = list(plan.small_files)
    expected = _read_rows(spark, bronze_dir, sources).count()
    target = plan.partition_dir / plan.target_name

    if not target.exists():  # a crashed earlier run may already have produced it
        tmp = plan.partition_dir / TMP_DIR_NAME
        shutil.rmtree(tmp, ignore_errors=True)
        _read_rows(spark, bronze_dir, sources).coalesce(1).write.mode("overwrite").parquet(str(tmp))
        written = sorted(tmp.glob("part-*.parquet"))
        if len(written) != 1:
            raise RuntimeError(f"expected 1 compacted file, found {len(written)} in {tmp}")
        written[0].replace(target)
        shutil.rmtree(tmp, ignore_errors=True)

    actual = spark.read.schema(BRONZE_SCHEMA).parquet(str(target)).count()
    if actual != expected:
        raise RuntimeError(
            f"{plan.partition_dir.name}: compacted file has {actual} rows, expected {expected}; "
            "source files kept"
        )
    for source in sources:
        _delete_with_crc(source)
    return expected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="only list what would be compacted")
    parser.add_argument("--min-age-seconds", type=float, default=600)
    args = parser.parse_args()

    configure_logging()
    settings = StreamingSettings.from_env()
    bronze_dir = Path(settings.bronze_path)
    plans = plan_compaction(
        bronze_dir,
        today=datetime.now(UTC).date(),
        now=time.time(),
        min_file_age_seconds=args.min_age_seconds,
    )
    if not plans:
        logger.info("Nothing to compact")
        return 0
    for plan in plans:
        logger.info(
            "Partition to compact",
            extra=log_fields(partition=plan.partition_dir.name, small_files=len(plan.small_files)),
        )
    if args.dry_run:
        return 0

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("mobilitypulse-compact-bronze")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    for plan in plans:
        rows = compact_partition(spark, bronze_dir, plan)
        logger.info(
            "Partition compacted",
            extra=log_fields(
                partition=plan.partition_dir.name, files_before=len(plan.small_files), rows=rows
            ),
        )
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
