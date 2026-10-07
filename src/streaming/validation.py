"""Parsing, validation and Bronze/rejected shaping of Kafka messages (pure DataFrame logic)."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from streaming.schemas import REQUIRED_FIELDS, STATION_EVENT_SCHEMA

EVENT_COLUMNS = [f.name for f in STATION_EVENT_SCHEMA.fields]


def _reject_reason(raw_value: Column, event: Column) -> Column:
    """First failing rule wins; null means the message is valid."""
    any_required_missing = reduce(
        lambda a, b: a | b, [event[name].isNull() for name in REQUIRED_FIELDS]
    )
    checks = [
        # from_json with a struct schema yields all-null fields (not a null struct) on bad
        # input, so well-formedness is checked separately: parseable as a JSON object?
        ("invalid_json", F.from_json(raw_value, "map<string,string>").isNull() | event.isNull()),
        ("missing_required_field", any_required_missing),
        ("blank_station_id", F.trim(event["station_id"]) == ""),
        (
            "negative_availability",
            (event["num_bikes_available"] < 0) | (event["num_docks_available"] < 0),
        ),
        (
            "invalid_coordinates",
            ~event["latitude"].between(-90, 90) | ~event["longitude"].between(-180, 180),
        ),
        ("invalid_event_timestamp", F.to_timestamp(event["event_timestamp"]).isNull()),
        ("invalid_ingestion_timestamp", F.to_timestamp(event["ingestion_timestamp"]).isNull()),
    ]
    return F.coalesce(*[F.when(cond, F.lit(reason)) for reason, cond in checks])


def parse_kafka_messages(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows -> raw payload, Kafka metadata, parsed ``event`` struct and ``reject_reason``."""
    raw = kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    parsed = raw.withColumn("event", F.from_json(F.col("raw_value"), STATION_EVENT_SCHEMA))
    return parsed.withColumn("reject_reason", _reject_reason(F.col("raw_value"), F.col("event")))


def to_bronze(classified_df: DataFrame) -> DataFrame:
    valid = classified_df.filter(F.col("reject_reason").isNull())
    return valid.select(
        *[F.col("event")[name].alias(name) for name in EVENT_COLUMNS if "timestamp" not in name],
        F.to_timestamp(F.col("event")["event_timestamp"]).alias("event_timestamp"),
        F.to_timestamp(F.col("event")["ingestion_timestamp"]).alias("ingestion_timestamp"),
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("bronze_ingested_at"),
    ).withColumn("ingestion_date", F.to_date("ingestion_timestamp"))


def to_rejected(classified_df: DataFrame) -> DataFrame:
    invalid = classified_df.filter(F.col("reject_reason").isNotNull())
    return invalid.select(
        "raw_value",
        "reject_reason",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("rejected_at"),
    ).withColumn("rejection_date", F.to_date("rejected_at"))
