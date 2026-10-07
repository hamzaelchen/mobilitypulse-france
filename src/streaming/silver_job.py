"""Spark Structured Streaming: Bronze Parquet -> Silver Parquet.

Submitted by docker compose (``silver-job`` service) with ``spark-submit``.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from common.config import StreamingSettings
from common.logging_utils import configure_logging, log_fields
from streaming.compaction_plan import SMALL_FILE_GLOB
from streaming.schemas import BRONZE_SCHEMA
from streaming.silver import to_silver

logger = logging.getLogger("silver_job")


def read_bronze_stream(spark: SparkSession, settings: StreamingSettings) -> DataFrame:
    # Compacted files (compacted-*) hold rows that were already read as part-* files:
    # the glob keeps them out of the stream.
    return (
        spark.readStream.schema(BRONZE_SCHEMA)
        .option("pathGlobFilter", SMALL_FILE_GLOB)
        .option("maxFilesPerTrigger", settings.silver_max_files_per_trigger)
        .parquet(settings.bronze_path)
    )


def start_silver_query(
    spark: SparkSession, settings: StreamingSettings, *, once: bool = False
) -> StreamingQuery:
    """``once=True`` processes the available backlog then stops (used by tests)."""
    silver = to_silver(read_bronze_stream(spark, settings), settings.silver_watermark_delay)
    writer = (
        silver.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option("path", settings.silver_path)
        .option("checkpointLocation", settings.silver_checkpoint_path)
    )
    writer = (
        writer.trigger(availableNow=True)
        if once
        else writer.trigger(processingTime=settings.trigger_interval)
    )
    return writer.start()


def main() -> None:
    configure_logging()
    settings = StreamingSettings.from_env()
    spark = SparkSession.builder.appName("mobilitypulse-silver-station-status").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    query = start_silver_query(spark, settings)
    logger.info(
        "Silver streaming query started",
        extra=log_fields(
            bronze_path=settings.bronze_path,
            silver_path=settings.silver_path,
            checkpoint=settings.silver_checkpoint_path,
            watermark_delay=settings.silver_watermark_delay,
        ),
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
