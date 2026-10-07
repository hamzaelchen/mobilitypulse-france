"""Explicit Spark schema of the Kafka station-status event (never inferred).

Timestamps stay strings here and are cast during validation, so a malformed timestamp
produces a rejection instead of silently becoming null in Bronze.
Keep in sync with ``common/events.py``.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STATION_EVENT_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("station_id", StringType()),
        StructField("station_name", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("capacity", IntegerType()),
        StructField("num_bikes_available", IntegerType()),
        StructField("num_docks_available", IntegerType()),
        StructField("num_bikes_disabled", IntegerType()),
        StructField("num_docks_disabled", IntegerType()),
        StructField("is_installed", BooleanType()),
        StructField("is_renting", BooleanType()),
        StructField("is_returning", BooleanType()),
        StructField("event_timestamp", StringType()),
        StructField("ingestion_timestamp", StringType()),
    ]
)

# Layout of the Bronze Parquet dataset (see ``validation.to_bronze``). ``ingestion_date`` is the
# partition column: it lives in the directory name, not inside the files.
BRONZE_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("station_id", StringType()),
        StructField("station_name", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("capacity", IntegerType()),
        StructField("num_bikes_available", IntegerType()),
        StructField("num_docks_available", IntegerType()),
        StructField("num_bikes_disabled", IntegerType()),
        StructField("num_docks_disabled", IntegerType()),
        StructField("is_installed", BooleanType()),
        StructField("is_renting", BooleanType()),
        StructField("is_returning", BooleanType()),
        StructField("event_timestamp", TimestampType()),
        StructField("ingestion_timestamp", TimestampType()),
        StructField("kafka_key", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("bronze_ingested_at", TimestampType()),
        StructField("ingestion_date", DateType()),
    ]
)

# Layout of the Silver dataset (see ``silver.to_silver``). ``ingestion_date`` (date of
# ``observed_at``) is the partition column. Silver keeps every reading (one per station per poll);
# ``event_timestamp`` is the feed's ``last_reported`` and is NOT the time of the counters.
SILVER_SCHEMA = StructType(
    [
        *[f for f in BRONZE_SCHEMA.fields if f.name != "ingestion_date"],
        StructField("fill_rate", DoubleType()),
        StructField("is_empty", BooleanType()),
        StructField("is_full", BooleanType()),
        StructField("is_operational", BooleanType()),
        StructField("observed_at", TimestampType()),
        StructField("silver_processed_at", TimestampType()),
        StructField("ingestion_date", DateType()),
    ]
)

# --- Gold (descriptive aggregates) -------------------------------------------------------
GOLD_STATION_CURRENT_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("station_id", StringType()),
        StructField("station_name", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("capacity", IntegerType()),
        StructField("num_bikes_available", IntegerType()),
        StructField("num_docks_available", IntegerType()),
        StructField("num_bikes_disabled", IntegerType()),
        StructField("num_docks_disabled", IntegerType()),
        StructField("fill_rate", DoubleType()),
        StructField("is_empty", BooleanType()),
        StructField("is_full", BooleanType()),
        StructField("is_operational", BooleanType()),
        StructField("observed_at", TimestampType()),  # time of the latest reading (poll time)
        StructField("last_reported_at", TimestampType()),  # the feed's last_reported: unreliable
        StructField("first_observed_at", TimestampType()),
        StructField("last_change_at", TimestampType()),  # null: no change observed yet
        StructField("minutes_since_observed", DoubleType()),
        StructField("hours_in_current_state", DoubleType()),
        StructField("state_age_is_lower_bound", BooleanType()),
        StructField("as_of", TimestampType()),
    ]
)

GOLD_SERIES_15MIN_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("station_id", StringType()),
        StructField("slot_start", TimestampType()),
        StructField("bikes_available", IntegerType()),
        StructField("docks_available", IntegerType()),
        StructField("capacity", IntegerType()),
        StructField("fill_rate", DoubleType()),
        StructField("is_empty", BooleanType()),
        StructField("is_full", BooleanType()),
        StructField("is_operational", BooleanType()),
        StructField("min_bikes_available", IntegerType()),
        StructField("max_bikes_available", IntegerType()),
        StructField("mean_bikes_available", DoubleType()),
        StructField("n_readings", IntegerType()),
        StructField("n_changes", IntegerType()),
        StructField("is_complete", BooleanType()),
        StructField("slot_date", DateType()),
    ]
)

GOLD_HOURLY_RATES_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("hour_start", TimestampType()),
        StructField("stations_observed", IntegerType()),
        StructField("station_coverage", DoubleType()),
        StructField("slot_observations", IntegerType()),
        StructField("operational_observations", IntegerType()),
        StructField("empty_rate", DoubleType()),
        StructField("full_rate", DoubleType()),
        StructField("non_operational_rate", DoubleType()),
        StructField("is_complete", BooleanType()),
        StructField("hour_date", DateType()),
    ]
)

REQUIRED_FIELDS = [
    "source",
    "station_id",
    "station_name",
    "latitude",
    "longitude",
    "num_bikes_available",
    "num_docks_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "event_timestamp",
    "ingestion_timestamp",
]
