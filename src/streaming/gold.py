"""Silver -> Gold descriptive aggregates (pure DataFrame logic, no I/O).

Time axis = ``observed_at`` (the poll time), never ``event_timestamp``: that field is the feed's
``last_reported`` and is not refreshed when the counters change. Silver keeps every reading (one
per station per poll, about every 60 s), so a 15-minute slot holds its own readings and nothing is
carried over from an earlier slot: a slot with no reading (pipeline stopped) simply has no row.
All "as of" logic uses an explicit timezone-aware ``as_of``, never the wall clock.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from streaming.schemas import (
    GOLD_HOURLY_RATES_SCHEMA,
    GOLD_SERIES_15MIN_SCHEMA,
    GOLD_STATION_CURRENT_SCHEMA,
)

SLOT_SECONDS = 900
KEY = ["source", "station_id"]
# Columns whose change makes a reading a "change" compared with the previous one of the station.
CHANGE_COLUMNS = [
    "capacity",
    "num_bikes_available",
    "num_docks_available",
    "num_bikes_disabled",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
]
STATE_COLUMNS = [
    "num_bikes_available",
    "num_docks_available",
    "capacity",
    "fill_rate",
    "is_empty",
    "is_full",
    "is_operational",
]
CURRENT_COLUMNS = [
    "station_name",
    "latitude",
    "longitude",
    "num_bikes_disabled",
    "num_docks_disabled",
    "observed_at",
    "event_timestamp",
    *STATE_COLUMNS,
]
# Tie-break among readings of a station: latest observation, then latest Kafka offset.
RECENCY = ["observed_at", "kafka_offset"]


def _ts(value: datetime) -> Column:
    """Exact timestamp literal (no dependence on the JVM/Python local timezone)."""
    return F.lit(int(value.timestamp())).cast("timestamp")


def floor_datetime(value: datetime, seconds: int) -> datetime:
    return datetime.fromtimestamp(int(value.timestamp()) // seconds * seconds, UTC)


def conform(df: DataFrame, schema: StructType) -> DataFrame:
    """Select and cast to the declared output schema (column order and types are explicit)."""
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def with_change_flag(readings: DataFrame) -> DataFrame:
    """Adds ``changed``: the reading differs from the station's previous one (null-safe).

    The first reading of a station is not a change. Must see all the readings of a station up to
    the point of interest, so call it before any lower time bound is applied.
    """
    ordered = Window.partitionBy(*KEY).orderBy(*RECENCY)
    differs = F.lit(False)
    for column in CHANGE_COLUMNS:
        differs = differs | ~F.col(column).eqNullSafe(F.lag(column).over(ordered))
    is_first = F.lag("observed_at").over(ordered).isNull()
    return readings.withColumn("changed", F.when(is_first, F.lit(False)).otherwise(differs))


def current_state(silver: DataFrame, as_of: datetime) -> DataFrame:
    """Latest reading of every station at ``as_of``, plus how long it has been in that state.

    ``hours_in_current_state`` counts from the last observed change. If no change was ever
    observed it counts from the first observation and ``state_age_is_lower_bound`` is true (the
    station may have been in that state for longer, but we were not watching).
    """
    readings = with_change_flag(silver.filter(F.col("observed_at") <= _ts(as_of)))
    per_station = readings.groupBy(*KEY).agg(
        F.max_by(F.struct(*CURRENT_COLUMNS), F.struct(*RECENCY)).alias("last"),
        F.min("observed_at").alias("first_observed_at"),
        F.max(F.when(F.col("changed"), F.col("observed_at"))).alias("last_change_at"),
    )
    latest = per_station.select(
        *KEY,
        *[F.col("last")[c].alias(c) for c in CURRENT_COLUMNS],
        "first_observed_at",
        "last_change_at",
    )
    out = (
        latest.withColumn("last_reported_at", F.col("event_timestamp"))
        .withColumn("as_of", _ts(as_of))
        .withColumn(
            "minutes_since_observed",
            (F.col("as_of").cast("long") - F.col("observed_at").cast("long")) / 60,
        )
        .withColumn(
            "hours_in_current_state",
            (
                F.col("as_of").cast("long")
                - F.coalesce("last_change_at", "first_observed_at").cast("long")
            )
            / 3600,
        )
        .withColumn("state_age_is_lower_bound", F.col("last_change_at").isNull())
    )
    return conform(out, GOLD_STATION_CURRENT_SCHEMA)


def station_series_15min(silver: DataFrame, as_of: datetime, window_days: int) -> DataFrame:
    """One row per station and 15-minute slot that contains at least one reading.

    The window starts at ``as_of`` minus ``window_days`` (rounded down to the hour). The slot
    containing ``as_of`` is flagged ``is_complete = false``. ``bikes_available`` and the other
    state columns are the LAST reading of the slot; ``min/max/mean_bikes_available`` summarize
    all its readings; ``n_changes`` counts the readings that differ from the previous one.
    """
    as_of_slot = floor_datetime(as_of, SLOT_SECONDS)
    window_start = floor_datetime(as_of_slot - timedelta(days=window_days), 3600)

    readings = (
        with_change_flag(silver.filter(F.col("observed_at") <= _ts(as_of)))
        .filter(F.col("observed_at") >= _ts(window_start))
        .withColumn(
            "slot_start",
            (F.floor(F.col("observed_at").cast("long") / SLOT_SECONDS) * SLOT_SECONDS).cast(
                "timestamp"
            ),
        )
    )
    per_slot = readings.groupBy(*KEY, "slot_start").agg(
        F.max_by(F.struct(*STATE_COLUMNS), F.struct(*RECENCY)).alias("last"),
        F.min("num_bikes_available").alias("min_bikes_available"),
        F.max("num_bikes_available").alias("max_bikes_available"),
        F.avg("num_bikes_available").alias("mean_bikes_available"),
        F.count("*").alias("n_readings"),
        F.sum(F.col("changed").cast("int")).alias("n_changes"),
    )
    series = (
        per_slot.withColumn("bikes_available", F.col("last")["num_bikes_available"])
        .withColumn("docks_available", F.col("last")["num_docks_available"])
        .withColumn("capacity", F.col("last")["capacity"])
        .withColumn("fill_rate", F.col("last")["fill_rate"])
        .withColumn("is_empty", F.col("last")["is_empty"])
        .withColumn("is_full", F.col("last")["is_full"])
        .withColumn("is_operational", F.col("last")["is_operational"])
        .withColumn(
            "is_complete", F.col("slot_start") + F.expr("interval 15 minutes") <= _ts(as_of)
        )
        .withColumn("slot_date", F.to_date("slot_start"))
    )
    return conform(series, GOLD_SERIES_15MIN_SCHEMA)


def hourly_rates(series: DataFrame) -> DataFrame:
    """Share of station-slots that are empty / full, per source and hour.

    Unit of observation: one station at the end of one 15-minute slot (at most 4 per hour).
    ``empty_rate`` and ``full_rate`` only count operational stations (denominator:
    ``operational_observations``); ``non_operational_rate`` is the share of station-slots that
    are out of service.

    ``station_coverage`` = stations with at least one slot during the hour / stations present
    anywhere in ``series``. It drops when the pipeline was not running or a station was not
    reported: rates for hours with a low coverage describe only part of the network.
    """
    operational = F.col("is_operational")
    hourly = series.groupBy("source", F.date_trunc("hour", "slot_start").alias("hour_start")).agg(
        F.countDistinct("station_id").cast("int").alias("stations_observed"),
        F.count("*").cast("int").alias("slot_observations"),
        F.sum(operational.cast("int")).cast("int").alias("operational_observations"),
        F.sum((F.col("is_empty") & operational).cast("int")).alias("_empty"),
        F.sum((F.col("is_full") & operational).cast("int")).alias("_full"),
        F.min(F.col("is_complete").cast("int")).alias("_complete"),
    )
    known_stations = series.groupBy("source").agg(
        F.countDistinct("station_id").alias("_known_stations")
    )
    operational_count = F.col("operational_observations")
    rates = (
        hourly.join(known_stations, "source")
        .withColumn("station_coverage", F.col("stations_observed") / F.col("_known_stations"))
        .withColumn(
            "empty_rate", F.when(operational_count > 0, F.col("_empty") / operational_count)
        )
        .withColumn("full_rate", F.when(operational_count > 0, F.col("_full") / operational_count))
        .withColumn("non_operational_rate", 1 - operational_count / F.col("slot_observations"))
        .withColumn("is_complete", F.col("_complete") == 1)
        .withColumn("hour_date", F.to_date("hour_start"))
    )
    return conform(rates, GOLD_HOURLY_RATES_SCHEMA)
