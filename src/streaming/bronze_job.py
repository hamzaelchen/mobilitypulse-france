"""Spark Structured Streaming: Kafka -> Bronze (valid) / rejected (invalid) Parquet.

Submitted by docker compose (``bronze-job`` service) with ``spark-submit``.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from common.config import StreamingSettings
from common.logging_utils import configure_logging, log_fields
from streaming.validation import parse_kafka_messages, to_bronze, to_rejected

logger = logging.getLogger("bronze_job")


def make_batch_writer(settings: StreamingSettings):
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Delivery is at-least-once: a batch replayed after a crash may append duplicates
        # (to be removed by deduplication on kafka_topic/partition/offset in Silver).
        batch_df.persist()
        try:
            bronze = to_bronze(batch_df)
            rejected = to_rejected(batch_df)
            valid_count = bronze.count()
            rejected_count = rejected.count()
            if valid_count:
                (
                    bronze.coalesce(1)
                    .write.mode("append")
                    .partitionBy("ingestion_date")
                    .parquet(settings.bronze_path)
                )
            if rejected_count:
                (
                    rejected.coalesce(1)
                    .write.mode("append")
                    .partitionBy("rejection_date")
                    .parquet(settings.rejected_path)
                )
            logger.info(
                "Batch written",
                extra=log_fields(
                    batch_id=batch_id, bronze_rows=valid_count, rejected_rows=rejected_count
                ),
            )
        finally:
            batch_df.unpersist()

    return write_batch


def main() -> None:
    configure_logging()
    settings = StreamingSettings.from_env()
    spark = SparkSession.builder.appName("mobilitypulse-bronze-station-status").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
    )

    query = (
        parse_kafka_messages(kafka_stream)
        .writeStream.foreachBatch(make_batch_writer(settings))
        .option("checkpointLocation", settings.checkpoint_path)
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )
    logger.info(
        "Bronze streaming query started",
        extra=log_fields(
            topic=settings.kafka_topic,
            bronze_path=settings.bronze_path,
            rejected_path=settings.rejected_path,
            checkpoint=settings.checkpoint_path,
        ),
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
