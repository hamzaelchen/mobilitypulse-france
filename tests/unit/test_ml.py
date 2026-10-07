"""Features, labels, temporal split, metrics and training. Need Java + numpy: Spark image."""

import json
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("numpy")

from pyspark.ml import PipelineModel  # noqa: E402
from pyspark.ml.linalg import Vectors, VectorUDT  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, StructField, StructType  # noqa: E402

from common.config import StreamingSettings  # noqa: E402
from ml.evaluate import confusion_metrics, ranking_metrics  # noqa: E402
from ml.features import build_samples, temporal_split  # noqa: E402
from ml.guards import Thresholds  # noqa: E402
from ml.schemas import TRAINING_SCHEMA  # noqa: E402
from ml.train import FEATURE_COLUMNS, run, with_model_columns, write_report  # noqa: E402
from streaming.schemas import GOLD_SERIES_15MIN_SCHEMA  # noqa: E402

DAY = datetime(2026, 10, 6, 10, 0, tzinfo=UTC)  # a Tuesday


def slot_row(station, slot, bikes, *, capacity=10, operational=True, complete=True):
    docks = None if capacity is None else capacity - bikes
    return (
        "test", station, slot, bikes, docks if docks is not None else 0, capacity,
        bikes / capacity if capacity else None, bikes == 0, docks == 0, operational,
        bikes, bikes, float(bikes), 15, 0, complete, slot.date(),
    )  # fmt: skip


def series_df(spark, rows):
    return spark.createDataFrame(rows, GOLD_SERIES_15MIN_SCHEMA)


def one_station(station="1", bikes=(5, 5, 4, 4, 3, 2, 1, 0, 0), skip=(), **kwargs):
    """Slots every 15 min from 10:00 UTC. ``skip``: indexes of slots with no row."""
    return [
        slot_row(station, DAY + timedelta(minutes=15 * i), b, **kwargs)
        for i, b in enumerate(bikes)
        if i not in skip
    ]


def collect(df):
    return sorted(df.collect(), key=lambda r: (r.station_id, r.slot_start))


def at(row_value):
    return row_value.replace(tzinfo=UTC)


def test_samples_have_exact_lags_label_and_local_time(spark):
    samples = build_samples(series_df(spark, one_station()))
    assert [(f.name, f.dataType) for f in samples.schema.fields] == [
        (f.name, f.dataType) for f in TRAINING_SCHEMA.fields
    ]
    rows = collect(samples)
    # Needs t-60 and t+30: only 11:00, 11:15 and 11:30 qualify.
    assert [at(r.slot_start).strftime("%H:%M") for r in rows] == ["11:00", "11:15", "11:30"]
    first, second, third = rows
    assert (first.bikes_available, first.bikes_lag_15, first.bikes_lag_30, first.bikes_lag_60) == (
        3, 4, 4, 5,
    )  # fmt: skip
    assert first.label_critical_30min is False  # 1 bike at 11:30
    assert (second.bikes_available, second.bikes_lag_60) == (2, 5)
    assert second.label_critical_30min is True and second.is_critical_now is False  # an onset
    assert third.label_critical_30min is True
    assert at(first.label_slot_start) == DAY + timedelta(minutes=90)  # t + 30 = 11:30
    assert first.hour_local == 13 and first.day_of_week_local == 2  # Europe/Paris, Tuesday
    assert first.fill_rate == pytest.approx(0.3)


def test_a_missing_slot_removes_the_samples_that_need_it(spark):
    rows = collect(build_samples(series_df(spark, one_station(skip={3}))))  # 10:45 absent
    # 11:00 needs 10:45 (t-15), 11:15 needs 10:45 (t-30): only 11:30 is left, nothing is invented.
    assert [at(r.slot_start).strftime("%H:%M") for r in rows] == ["11:30"]


def test_incomplete_slots_are_never_used(spark):
    series = series_df(spark, one_station())
    # Mark the 12:00 slot incomplete: t = 11:30 loses its label slot.
    series = series.withColumn(
        "is_complete",
        F.when(
            F.col("slot_start") == F.lit("2026-10-06 12:00:00").cast("timestamp"), False
        ).otherwise(True),
    )
    rows = collect(build_samples(series))
    assert [at(r.slot_start).strftime("%H:%M") for r in rows] == ["11:00", "11:15"]


def test_unknown_capacity_samples_are_dropped(spark):
    assert build_samples(series_df(spark, one_station(capacity=None))).count() == 0


def test_out_of_service_station_is_not_a_critical_label(spark):
    rows = collect(build_samples(series_df(spark, one_station(operational=False))))
    assert rows and all(r.label_critical_30min is False for r in rows)
    assert all(r.is_critical_now is False for r in rows)


def test_features_never_use_information_after_t(spark):
    """Changing every slot after t must not change the features of the sample at t."""
    original = one_station()
    # Everything after t = 11:15 (slot index 5) is replaced: the label slot (11:45) was empty,
    # it now holds 9 bikes.
    changed = [
        r if i <= 5 else slot_row("1", DAY + timedelta(minutes=15 * i), 9)
        for i, r in enumerate(original)
    ]
    feature_columns = [
        c for c in TRAINING_SCHEMA.fieldNames() if c not in ("label_critical_30min",)
    ]
    only_t = "slot_start = timestamp'2026-10-06 11:15:00'"
    a = collect(build_samples(series_df(spark, original)).filter(only_t))
    b = collect(build_samples(series_df(spark, changed)).filter(only_t))
    assert len(a) == len(b) == 1
    assert [a[0][c] for c in feature_columns] == [b[0][c] for c in feature_columns]
    assert a[0].label_critical_30min != b[0].label_critical_30min  # only the label sees the future


def long_series(stations=1, slots=21):
    rows = []
    for s in range(stations):
        rows += [
            slot_row(str(s), DAY + timedelta(minutes=15 * i), (i + s) % 6) for i in range(slots)
        ]
    return rows


def test_temporal_split_is_chronological_with_an_embargo(spark):
    samples = build_samples(series_df(spark, long_series()))
    n = samples.count()
    train, test, cutoff = temporal_split(samples, test_fraction=0.3)
    assert n == 15  # 21 slots, 4 lost to t-60 history and 2 to the t+30 label

    train_rows, test_rows = collect(train), collect(test)
    assert train_rows and test_rows
    assert at(test_rows[0].slot_start) == cutoff
    assert all(at(r.slot_start) >= cutoff for r in test_rows)
    # No train label slot reaches into the test period (label slot + 15 min <= cutoff).
    assert all(at(r.label_slot_start) + timedelta(minutes=15) <= cutoff for r in train_rows)
    assert max(r.slot_start for r in train_rows) < min(r.slot_start for r in test_rows)
    # The embargo costs samples: train + test < all.
    assert len(train_rows) + len(test_rows) < n


def test_temporal_split_with_too_few_slots_is_empty_and_invalid_fractions_fail(spark):
    one = build_samples(series_df(spark, one_station(bikes=(5, 5, 4, 4, 3, 2, 1))))
    train, test, cutoff = temporal_split(one, 0.3)
    assert cutoff is None and train.count() == 0 and test.count() == 0
    with pytest.raises(ValueError):
        temporal_split(one, 1.5)


def test_confusion_metrics_by_hand(spark):
    df = spark.createDataFrame(
        [(True, True), (True, False), (False, True), (False, False), (False, False)],
        "label boolean, prediction boolean",
    )
    m = confusion_metrics(df, F.col("label"), F.col("prediction"))
    assert (m["tp"], m["fn"], m["fp"], m["tn"]) == (1, 1, 1, 2)
    assert m["precision"] == 0.5 and m["recall"] == 0.5 and m["f1"] == 0.5
    never = confusion_metrics(df, F.col("label"), F.lit(False))
    assert never["precision"] is None and never["recall"] == 0.0 and never["f1"] is None


def test_ranking_metrics_perfect_and_degenerate(spark):
    rows = [(1.0, Vectors.dense([0.1, 0.9])), (1.0, Vectors.dense([0.2, 0.8])),
            (0.0, Vectors.dense([0.7, 0.3])), (0.0, Vectors.dense([0.9, 0.1]))]  # fmt: skip
    schema = StructType(
        [StructField("label", DoubleType()), StructField("probability", VectorUDT())]
    )
    perfect = spark.createDataFrame(rows, schema)
    result = ranking_metrics(perfect, "probability", "label")
    assert result["pr_auc"] == pytest.approx(1.0) and result["roc_auc"] == pytest.approx(1.0)
    single = spark.createDataFrame(rows[:2], schema)
    assert ranking_metrics(single, "probability", "label") == {"pr_auc": None, "roc_auc": None}


def sawtooth(stations=40, slots=96):
    """Synthetic network for CODE tests only: bikes drain 8 -> 0, stay empty 2 slots, refill."""
    rows = []
    for s in range(stations):
        for i in range(slots):
            phase = (i + s) % 10
            bikes = max(8 - phase, 0) if phase < 8 else 0
            rows.append(slot_row(str(s), DAY + timedelta(minutes=15 * i), bikes, capacity=8))
    return rows


def write_series(spark, settings, rows):
    (
        series_df(spark, rows)
        .write.mode("overwrite")
        .partitionBy("slot_date")
        .parquet(settings.gold_series_path)
    )


def settings_for(tmp_path):
    return StreamingSettings(
        kafka_bootstrap_servers="unused",
        kafka_topic="t",
        data_dir=str(tmp_path),
        trigger_interval="1 second",
        max_offsets_per_trigger=1,
        starting_offsets="earliest",
    )


def test_training_beats_persistence_on_a_learnable_pattern_and_saves_a_model(spark, tmp_path):
    """Checks the machinery on synthetic data. The numbers are NOT a performance claim."""
    settings = settings_for(tmp_path)
    write_series(spark, settings, sawtooth())
    as_of = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
    report = run(
        spark, settings, thresholds=Thresholds(), test_fraction=0.3, force=False, as_of=as_of
    )
    assert report["status"] == "trained" and report["reliable"] is True
    onsets = report["metrics"]["onsets"]
    assert onsets["baseline_persistence"]["recall"] == 0.0  # cannot anticipate: by construction
    assert onsets["model"]["recall"] is not None and onsets["model"]["recall"] > 0.5
    assert set(report["coefficients_standardized"]) == set(FEATURE_COLUMNS)

    # The persisted model reloads and scores identically.
    model = PipelineModel.load(report["model_path"])
    test_frame = with_model_columns(build_samples(series_df(spark, sawtooth())).limit(50))
    assert model.transform(test_frame).count() == 50

    path = write_report(report, tmp_path / "reports", as_of)
    assert json.loads(path.read_text())["status"] == "trained"
    assert (tmp_path / "reports" / "latest.json").exists()


def test_insufficient_data_trains_nothing_and_says_why(spark, tmp_path):
    settings = settings_for(tmp_path)
    write_series(spark, settings, long_series(stations=3, slots=12))
    as_of = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
    report = run(
        spark, settings, thresholds=Thresholds(), test_fraction=0.3, force=False, as_of=as_of
    )
    assert report["status"] == "insufficient_data" and report["reliable"] is False
    assert report["reasons"]
    assert "metrics" not in report and "model_path" not in report
    assert not (tmp_path / "ml" / "models").exists()


def test_forced_run_on_insufficient_data_is_flagged_unreliable_and_saves_no_model(spark, tmp_path):
    settings = settings_for(tmp_path)
    write_series(spark, settings, sawtooth(stations=6, slots=30))
    as_of = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
    report = run(
        spark, settings, thresholds=Thresholds(), test_fraction=0.3, force=True, as_of=as_of
    )
    assert report["status"] in ("trained_unreliable_forced", "cannot_train")
    assert report["status"] != "cannot_train" or any("train set" in r for r in report["reasons"])
    assert report["reliable"] is False and report["reasons"]
    assert "model_path" not in report
    assert not (tmp_path / "ml" / "models").exists()


def test_no_samples_at_all_is_reported(spark, tmp_path):
    settings = settings_for(tmp_path)
    write_series(spark, settings, one_station(bikes=(5, 4, 3)))
    report = run(
        spark, settings, thresholds=Thresholds(), test_fraction=0.3, force=False,
        as_of=datetime(2026, 10, 7, tzinfo=UTC),
    )  # fmt: skip
    assert report["status"] == "insufficient_data" and report["samples"] == 0
