"""Explicit Spark schema of the training set (one row = one station at the end of one slot)."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

HORIZON_MINUTES = 30
LAG_MINUTES = (15, 30, 60)

TRAINING_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("station_id", StringType()),
        StructField("slot_start", TimestampType()),  # t: the sample describes the state at t
        StructField("label_slot_start", TimestampType()),  # t + 30 min
        StructField("bikes_available", IntegerType()),
        StructField("bikes_lag_15", IntegerType()),
        StructField("bikes_lag_30", IntegerType()),
        StructField("bikes_lag_60", IntegerType()),
        StructField("capacity", IntegerType()),
        StructField("fill_rate", DoubleType()),
        StructField("is_operational", BooleanType()),
        StructField("is_critical_now", BooleanType()),
        StructField("hour_local", IntegerType()),  # Europe/Paris, 0-23
        StructField("day_of_week_local", IntegerType()),  # Europe/Paris, 1 = Monday .. 7 = Sunday
        StructField("label_critical_30min", BooleanType()),
        StructField("slot_date", DateType()),
    ]
)
