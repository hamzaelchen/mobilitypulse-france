"""Read-only check of the Gold datasets against an independent pure-Python recomputation.

Run inside the Spark image: ``python -m streaming.inspect_gold``. Silver is collected to the
driver, which is fine for a development data set but not meant for large volumes.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from common.config import StreamingSettings
from streaming.gold import CHANGE_COLUMNS, SLOT_SECONDS


def _epoch(value: datetime) -> int:
    return int(value.replace(tzinfo=UTC).timestamp())  # Spark returns UTC-naive datetimes here


def _report(label: str, ok: bool, detail: str) -> bool:
    print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")
    return ok


def check_gold(spark: SparkSession, settings: StreamingSettings) -> bool:
    try:
        current = spark.read.parquet(settings.gold_current_path)
        series = spark.read.parquet(settings.gold_series_path)
        hourly = spark.read.parquet(settings.gold_hourly_path)
    except AnalysisException:
        print("== GOLD: no data yet")
        return False

    as_of = _epoch(current.first()["as_of"])
    # Silver lags the producer by up to about a minute, and keeps growing while we check: only
    # the readings Gold could have seen count (its newest observation), never newer ones.
    gold_seen_until = min(as_of, max(_epoch(r.observed_at) for r in current.collect()))
    as_of_slot = as_of // SLOT_SECONDS * SLOT_SECONDS
    window_start = (as_of_slot - settings.gold_series_days * 86400) // 3600 * 3600

    # --- independent recomputation from Silver (plain Python, one pass per station) -------
    columns = ["source", "station_id", "observed_at", "kafka_offset", *CHANGE_COLUMNS]
    by_station: dict[tuple[str, str], list] = {}
    for row in spark.read.parquet(settings.silver_path).select(*columns).collect():
        if _epoch(row.observed_at) <= gold_seen_until:
            by_station.setdefault((row.source, row.station_id), []).append(row)

    expected_current: dict[tuple[str, str], dict] = {}
    slot_readings: dict[tuple[str, str, int], int] = {}
    slot_changes: dict[tuple[str, str, int], int] = {}
    slot_last_bikes: dict[tuple[str, str, int], int] = {}
    for key, rows in by_station.items():
        rows.sort(key=lambda r: (_epoch(r.observed_at), r.kafka_offset))
        previous = None
        last_change = None
        for r in rows:
            changed = previous is not None and any(
                getattr(r, c) != getattr(previous, c) for c in CHANGE_COLUMNS
            )
            observed = _epoch(r.observed_at)
            if changed:
                last_change = observed
            if observed >= window_start:
                slot = (*key, observed // SLOT_SECONDS * SLOT_SECONDS)
                slot_readings[slot] = slot_readings.get(slot, 0) + 1
                slot_changes[slot] = slot_changes.get(slot, 0) + int(changed)
                slot_last_bikes[slot] = r.num_bikes_available
            previous = r
        expected_current[key] = {
            "bikes": rows[-1].num_bikes_available,
            "observed": _epoch(rows[-1].observed_at),
            "last_change": last_change,
            "first": _epoch(rows[0].observed_at),
        }

    ok = True
    cur = {(r.source, r.station_id): r for r in current.collect()}
    ok &= _report(
        "current: one row per station",
        len(cur) == current.count() == len(expected_current),
        f"{len(cur)} rows, {len(expected_current)} stations in Silver",
    )
    wrong = [
        k
        for k, e in expected_current.items()
        if cur[k].num_bikes_available != e["bikes"] or _epoch(cur[k].observed_at) != e["observed"]
    ]
    ok &= _report("current: latest reading equals Silver's", not wrong, f"{len(wrong)} mismatches")
    wrong = [
        k
        for k, e in expected_current.items()
        if (None if cur[k].last_change_at is None else _epoch(cur[k].last_change_at))
        != e["last_change"]
        or cur[k].state_age_is_lower_bound != (e["last_change"] is None)
    ]
    ok &= _report("current: last_change_at recomputed", not wrong, f"{len(wrong)} mismatches")
    wrong = [
        k
        for k, e in expected_current.items()
        if abs(
            cur[k].hours_in_current_state
            - (as_of - (e["last_change"] if e["last_change"] is not None else e["first"])) / 3600
        )
        > 1e-6
    ]
    ok &= _report("current: hours_in_current_state recomputed", not wrong, f"{len(wrong)} wrong")

    series_rows = series.collect()
    ok &= _report(
        "series: one row per (station, slot) holding a reading",
        len(series_rows) == len(slot_readings),
        f"{len(series_rows)} rows, expected {len(slot_readings)}",
    )
    got = {
        (r.source, r.station_id, _epoch(r.slot_start)): (
            r.n_readings,
            r.n_changes,
            r.bikes_available,
        )
        for r in series_rows
    }
    expected = {k: (slot_readings[k], slot_changes[k], slot_last_bikes[k]) for k in slot_readings}
    wrong = [k for k in expected if got.get(k) != expected[k]]
    ok &= _report(
        "series: n_readings / n_changes / last bikes per slot recomputed",
        not wrong and len(got) == len(series_rows),
        f"{len(wrong)} mismatches; {sum(slot_readings.values())} readings, "
        f"{sum(slot_changes.values())} changes in the window",
    )
    bad_bounds = series.filter(
        "min_bikes_available > bikes_available or max_bikes_available < bikes_available "
        "or mean_bikes_available < min_bikes_available - 1e-9 "
        "or mean_bikes_available > max_bikes_available + 1e-9"
    ).count()
    ok &= _report("series: min <= last / mean <= max", bad_bounds == 0, f"{bad_bounds} violations")
    hourly_obs = hourly.agg({"slot_observations": "sum"}).first()[0]
    ok &= _report(
        "hourly: slot observations add up to the series",
        hourly_obs == len(series_rows),
        f"{hourly_obs} vs {len(series_rows)}",
    )
    bad_rates = hourly.filter(
        "empty_rate < 0 or empty_rate > 1 or full_rate < 0 or full_rate > 1 "
        "or non_operational_rate < 0 or non_operational_rate > 1"
    ).count()
    ok &= _report("hourly: rates within [0, 1]", bad_rates == 0, f"{bad_rates} violations")

    start = datetime.fromtimestamp(window_start, UTC)
    print(f"== as_of={datetime.fromtimestamp(as_of, UTC)} window_start={start}")
    print("== GOLD hourly rates (latest 6 hours)")
    hourly.orderBy(hourly.hour_start.desc()).limit(6).orderBy("hour_start").show(truncate=False)
    return bool(ok)


def main() -> int:
    settings = StreamingSettings.from_env()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("inspect-gold")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    ok = check_gold(spark, settings)
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
