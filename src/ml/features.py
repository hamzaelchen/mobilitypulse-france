"""Training samples from the Gold 15-minute series, and the temporal split (pure DataFrame logic).

One sample = one station at the end of a complete slot ``t``:
- features use ONLY slots <= t: bikes at t, t-15, t-30, t-60 min, capacity, fill rate, local hour
  and weekday;
- the label is "the station is operational and empty or full at t+30 min" (the state at the end of
  slot ``t + 30 min``).
A sample exists only when all those slots are present: nothing is interpolated or carried over.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ml.schemas import HORIZON_MINUTES, LAG_MINUTES, TRAINING_SCHEMA

KEY = ["source", "station_id"]
SLOT_MINUTES = 15
LOCAL_TIMEZONE = "Europe/Paris"


def _is_critical() -> Column:
    return F.col("is_operational") & (F.col("is_empty") | F.col("is_full"))


def _shifted(base: DataFrame, minutes: int, **columns: Column) -> DataFrame:
    """``base`` with its slots moved by ``minutes`` (so that a join on slot_start aligns them)."""
    return base.select(
        *KEY,
        (F.col("slot_start") + F.expr(f"interval {minutes} minutes")).alias("slot_start"),
        *[column.alias(name) for name, column in columns.items()],
    )


def build_samples(series: DataFrame) -> DataFrame:
    """Samples from ``gold/station_series_15min``. Incomplete slots are never used."""
    base = series.filter(F.col("is_complete"))
    samples = base.select(
        *KEY,
        "slot_start",
        "bikes_available",
        "capacity",
        "fill_rate",
        "is_operational",
        _is_critical().alias("is_critical_now"),
    )
    for minutes in LAG_MINUTES:
        lag = _shifted(base, minutes, **{f"bikes_lag_{minutes}": F.col("bikes_available")})
        samples = samples.join(lag, [*KEY, "slot_start"], "inner")
    # The label of the sample at t is read from the slot at t + horizon (shift backwards).
    future = _shifted(base, -HORIZON_MINUTES, label_critical_30min=_is_critical())
    samples = samples.join(future, [*KEY, "slot_start"], "inner")

    local = F.from_utc_timestamp("slot_start", LOCAL_TIMEZONE)
    samples = (
        samples.filter(F.col("fill_rate").isNotNull())  # capacity unknown: not usable
        .withColumn("label_slot_start", F.col("slot_start") + F.expr("interval 30 minutes"))
        .withColumn("hour_local", F.hour(local))
        # Spark's dayofweek is 1 = Sunday; shift to ISO 1 = Monday .. 7 = Sunday.
        .withColumn("day_of_week_local", (F.dayofweek(local) + 5) % 7 + 1)
        .withColumn("slot_date", F.to_date("slot_start"))
    )
    return samples.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in TRAINING_SCHEMA.fields]
    )


def temporal_split(
    samples: DataFrame, test_fraction: float = 0.3
) -> tuple[DataFrame, DataFrame, datetime | None]:
    """Chronological split, never random.

    The last ``test_fraction`` of the distinct slots form the test set (``slot_start >= cutoff``).
    The train set keeps only samples whose label slot is fully over before the cutoff
    (``label_slot_start + 15 min <= cutoff``): no label of the train set looks into the test period.
    Returns ``(train, test, cutoff)``; ``cutoff`` is None when there are fewer than 2 slots.
    """
    if not 0 < test_fraction < 1:
        raise ValueError("test_fraction must be in (0, 1)")
    slot_epochs = samples.select(F.col("slot_start").cast("long").alias("slot_epoch"))
    slots = [row["slot_epoch"] for row in slot_epochs.distinct().sort("slot_epoch").collect()]
    if len(slots) < 2:
        empty = samples.limit(0)
        return empty, empty, None
    first_test = min(max(1, int(len(slots) * (1 - test_fraction))), len(slots) - 1)
    cutoff_epoch = slots[first_test]
    cutoff = F.lit(cutoff_epoch).cast("timestamp")
    train = samples.filter(
        F.col("label_slot_start") + F.expr(f"interval {SLOT_MINUTES} minutes") <= cutoff
    )
    test = samples.filter(F.col("slot_start") >= cutoff)
    return train, test, datetime.fromtimestamp(cutoff_epoch, UTC)
