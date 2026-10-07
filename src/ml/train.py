"""Build the training set, split it chronologically, train and evaluate against baselines.

Run: ``python -m ml.train [--force] [--test-fraction 0.3]`` (compose service ``ml-job``).

Only measured numbers are ever reported. When the data does not meet the explicit sufficiency
thresholds (``ml.guards``) the job writes the counts and the reasons and trains nothing, unless
``--force`` asks for a mechanical run: its metrics are then flagged ``reliable: false``.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from common.config import StreamingSettings
from common.logging_utils import configure_logging, log_fields
from ml.evaluate import confusion_metrics, ranking_metrics
from ml.features import build_samples, temporal_split
from ml.guards import Assessment, SplitCounts, Thresholds, assess
from ml.schemas import HORIZON_MINUTES, LAG_MINUTES
from streaming.schemas import GOLD_SERIES_15MIN_SCHEMA

logger = logging.getLogger("ml_train")

FEATURE_COLUMNS = [
    "bikes_available",
    "bikes_lag_15",
    "bikes_lag_30",
    "bikes_lag_60",
    "delta_15",
    "delta_60",
    "fill_rate",
    "capacity",
    "is_operational_f",
    "is_critical_now_f",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
]


# pyspark.ml.functions.vector_to_array needs pandas, which the image does not ship.
_positive_probability = F.udf(lambda v: float(v[1]), DoubleType())


def with_model_columns(samples: DataFrame) -> DataFrame:
    """Numeric model inputs derived from the samples (nothing here looks beyond slot t)."""
    hour_angle = 2 * math.pi * F.col("hour_local") / 24
    dow_angle = 2 * math.pi * (F.col("day_of_week_local") - 1) / 7
    return (
        samples.withColumn("label", F.col("label_critical_30min").cast("double"))
        .withColumn("delta_15", (F.col("bikes_available") - F.col("bikes_lag_15")).cast("double"))
        .withColumn("delta_60", (F.col("bikes_available") - F.col("bikes_lag_60")).cast("double"))
        .withColumn("is_operational_f", F.col("is_operational").cast("double"))
        .withColumn("is_critical_now_f", F.col("is_critical_now").cast("double"))
        .withColumn("hour_sin", F.sin(hour_angle))
        .withColumn("hour_cos", F.cos(hour_angle))
        .withColumn("dow_sin", F.sin(dow_angle))
        .withColumn("dow_cos", F.cos(dow_angle))
        .withColumn("bikes_available", F.col("bikes_available").cast("double"))
        .withColumn("bikes_lag_15", F.col("bikes_lag_15").cast("double"))
        .withColumn("bikes_lag_30", F.col("bikes_lag_30").cast("double"))
        .withColumn("bikes_lag_60", F.col("bikes_lag_60").cast("double"))
        .withColumn("capacity", F.col("capacity").cast("double"))
    )


def train_logistic_regression(train: DataFrame) -> PipelineModel:
    """Logistic regression on standardized features, classes weighted to balance the loss."""
    total = train.count()
    positives = train.filter(F.col("label") == 1.0).count()
    if total == 0:
        raise ValueError("the train set is empty: not enough history for lags, horizon and embargo")
    if positives == 0 or positives == total:
        raise ValueError("the train set has a single class: cannot fit a classifier")
    w_pos = total / (2 * positives)
    w_neg = total / (2 * (total - positives))
    weighted = train.withColumn("weight", F.when(F.col("label") == 1.0, w_pos).otherwise(w_neg))
    pipeline = Pipeline(
        stages=[
            VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features_raw"),
            StandardScaler(inputCol="features_raw", outputCol="features", withMean=True),
            LogisticRegression(
                featuresCol="features",
                labelCol="label",
                weightCol="weight",
                regParam=0.01,
                maxIter=100,
            ),
        ]
    )
    return pipeline.fit(weighted)


def evaluate_on(test: DataFrame, model: PipelineModel | None) -> dict[str, Any]:
    """Baselines and (optionally) the model, on all test samples and on onsets only.

    "Onsets" are the samples that are NOT critical at prediction time: the hard, useful case,
    where persistence cannot predict anything. Returns plain numbers only.
    """
    scored = test
    if model is not None:
        scored = (
            model.transform(test)
            .withColumn("p_critical", _positive_probability("probability"))
            .withColumn("model_prediction", F.col("prediction") == 1.0)
        )
    label = F.col("label_critical_30min")
    subsets = {"all": scored, "onsets": scored.filter(~F.col("is_critical_now"))}
    result: dict[str, Any] = {}
    for name, frame in subsets.items():
        frame = frame.cache()
        entry = {
            "baseline_persistence": confusion_metrics(frame, label, F.col("is_critical_now")),
            "baseline_always_negative": confusion_metrics(frame, label, F.lit(False)),
        }
        if model is not None:
            entry["model"] = {
                **confusion_metrics(frame, label, F.col("model_prediction")),
                **ranking_metrics(frame, "probability", "label"),
            }
        result[name] = entry
        frame.unpersist()
    return result


def _split_counts(train: DataFrame, test: DataFrame) -> SplitCounts:
    def positives(df: DataFrame) -> int:
        return df.filter(F.col("label_critical_30min")).count()

    test_pos = positives(test)
    test_total = test.count()
    onsets = test.filter(F.col("label_critical_30min") & ~F.col("is_critical_now")).count()
    return SplitCounts(
        train_samples=train.count(),
        train_positives=positives(train),
        test_samples=test_total,
        test_positives=test_pos,
        test_onsets=onsets,
        test_negatives=test_total - test_pos,
    )


def run(
    spark: SparkSession,
    settings: StreamingSettings,
    *,
    thresholds: Thresholds,
    test_fraction: float,
    force: bool,
    as_of: datetime,
) -> dict[str, Any]:
    series = spark.read.schema(GOLD_SERIES_15MIN_SCHEMA).parquet(settings.gold_series_path)
    samples = build_samples(series).cache()
    n_samples = samples.count()
    series_bounds = series.agg(F.min("slot_start"), F.max("slot_start")).first()

    report: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "horizon_minutes": HORIZON_MINUTES,
        "lag_minutes": list(LAG_MINUTES),
        "series": {
            "first_slot": series_bounds[0].isoformat() if series_bounds[0] else None,
            "last_slot": series_bounds[1].isoformat() if series_bounds[1] else None,
            "rows": series.count(),
            "stations": series.select("source", "station_id").distinct().count(),
        },
        "samples": n_samples,
        "thresholds": thresholds.__dict__,
        "test_fraction": test_fraction,
    }
    if n_samples == 0:
        report.update(status="insufficient_data", reasons=["no sample: history too short"])
        return report

    (
        samples.repartition("slot_date")
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("slot_date")
        .parquet(settings.ml_training_path)
    )
    train, test, cutoff = temporal_split(samples, test_fraction)
    train, test = train.cache(), test.cache()
    counts = _split_counts(train, test)
    assessment: Assessment = assess(counts, thresholds)
    report.update(
        cutoff=cutoff.isoformat() if cutoff else None,
        split=counts.__dict__,
        reliable=assessment.reliable,
        reasons=list(assessment.reasons),
    )
    if not assessment.reliable and not force:
        report["status"] = "insufficient_data"
        return report

    try:
        model = train_logistic_regression(with_model_columns(train))
    except ValueError as exc:
        report.update(status="cannot_train", reasons=[*assessment.reasons, str(exc)])
        report["reliable"] = False
        return report
    lr: LogisticRegressionModel = model.stages[-1]
    report["status"] = "trained" if assessment.reliable else "trained_unreliable_forced"
    report["feature_columns"] = FEATURE_COLUMNS
    report["coefficients_standardized"] = dict(
        zip(FEATURE_COLUMNS, (float(c) for c in lr.coefficients.toArray()), strict=True)
    )
    report["intercept"] = float(lr.intercept)
    report["metrics"] = evaluate_on(with_model_columns(test), model)
    if assessment.reliable:
        path = f"{settings.ml_models_path}/logreg_{as_of:%Y%m%dT%H%M%SZ}"
        model.write().overwrite().save(path)
        report["model_path"] = path
    return report


def write_report(report: dict[str, Any], reports_dir: Path, as_of: datetime) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2, ensure_ascii=False, default=str)
    path = reports_dir / f"report_{as_of:%Y%m%dT%H%M%SZ}.json"
    path.write_text(text + "\n", encoding="utf-8")
    (reports_dir / "latest.json").write_text(text + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-fraction", type=float, default=0.3)
    parser.add_argument("--force", action="store_true", help="train even if data is insufficient")
    defaults = Thresholds()
    for name, value in defaults.__dict__.items():
        parser.add_argument(f"--{name.replace('_', '-')}", type=int, default=value)
    args = parser.parse_args()
    thresholds = Thresholds(**{name: getattr(args, name) for name in defaults.__dict__})

    configure_logging()
    settings = StreamingSettings.from_env()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("mobilitypulse-ml")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    as_of = datetime.now(UTC)
    try:
        report = run(
            spark,
            settings,
            thresholds=thresholds,
            test_fraction=args.test_fraction,
            force=args.force,
            as_of=as_of,
        )
    finally:
        pass
    path = write_report(report, Path(settings.ml_reports_path), as_of)
    logger.info(
        "ML run finished",
        extra=log_fields(
            status=report["status"], report=str(path), reasons=report.get("reasons", [])
        ),
    )
    spark.stop()
    return 0 if report["status"].startswith("trained") else 3


if __name__ == "__main__":
    raise SystemExit(main())
