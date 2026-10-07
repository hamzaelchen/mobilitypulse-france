"""Bronze -> Silver transformations (pure DataFrame logic, no I/O)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.schemas import BRONZE_SCHEMA

KAFKA_COORDINATES = ["kafka_topic", "kafka_partition", "kafka_offset"]
# One reading of one station = one poll. ``ingestion_timestamp`` is the poll time (shared by every
# station of a poll), so this key only matches a reading delivered twice, never a real change.
BUSINESS_KEY = ["source", "station_id", "ingestion_timestamp"]

DERIVED_COLUMNS = ["fill_rate", "is_empty", "is_full", "is_operational", "observed_at"]
# ``ingestion_date`` (partition column) is the date of ``observed_at``.
SILVER_COLUMNS = [
    *[f.name for f in BRONZE_SCHEMA.fields if f.name != "ingestion_date"],
    *DERIVED_COLUMNS,
    "silver_processed_at",
    "ingestion_date",
]


def deduplicate(df: DataFrame, watermark_delay: str) -> DataFrame:
    """Streaming deduplication, bounded by a watermark on ``ingestion_timestamp``.

    1. Same Kafka message seen twice (replayed Bronze batch, re-read file).
    2. Same station reading seen twice in the same poll (``BUSINESS_KEY``).

    The key deliberately does NOT contain ``event_timestamp``: that field is the feed's
    ``last_reported``, which this feed rarely refreshes. Measured on the real feed (Bronze, 6 Oct
    2026): of 1016 counter changes between consecutive polls, only 14 came with a new
    ``last_reported``. Deduplicating on it dropped ~99 % of the real changes.

    Limit: a duplicate arriving more than ``watermark_delay`` after the first copy is kept.
    """
    return (
        df.withWatermark("ingestion_timestamp", watermark_delay)
        .dropDuplicatesWithinWatermark(KAFKA_COORDINATES)
        .dropDuplicatesWithinWatermark(BUSINESS_KEY)
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Business columns. ``fill_rate`` is null when the capacity is unknown or not positive.

    ``observed_at`` is the time the reading was taken (the poll time). ``event_timestamp`` is kept
    as received (the feed's ``last_reported``) but must not be used as the time of the counters.

    ``is_empty`` / ``is_full`` only describe the counters: they stay true for a station that
    is out of service, so combine them with ``is_operational`` for service-level statistics.
    """
    return (
        df.withColumn(
            "fill_rate",
            F.when(F.col("capacity") > 0, F.col("num_bikes_available") / F.col("capacity")),
        )
        .withColumn("is_empty", F.col("num_bikes_available") == 0)
        .withColumn("is_full", F.col("num_docks_available") == 0)
        .withColumn(
            "is_operational",
            F.col("is_installed") & F.col("is_renting") & F.col("is_returning"),
        )
        .withColumn("observed_at", F.col("ingestion_timestamp"))
    )


def to_silver(df: DataFrame, watermark_delay: str) -> DataFrame:
    deduped = deduplicate(df, watermark_delay)
    return (
        add_derived_columns(deduped)
        .withColumn("silver_processed_at", F.current_timestamp())
        .select(*SILVER_COLUMNS)
    )
