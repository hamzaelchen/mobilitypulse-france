"""Classification metrics computed from a predictions DataFrame (nothing is hard-coded)."""

from __future__ import annotations

from typing import Any

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def confusion_metrics(df: DataFrame, label: Column, prediction: Column) -> dict[str, Any]:
    """Counts and precision / recall / F1 of the positive class (None when undefined)."""
    row = df.agg(
        F.sum((label & prediction).cast("int")).alias("tp"),
        F.sum((~label & prediction).cast("int")).alias("fp"),
        F.sum((label & ~prediction).cast("int")).alias("fn"),
        F.sum((~label & ~prediction).cast("int")).alias("tn"),
    ).first()
    tp, fp, fn, tn = (int(row[k] or 0) for k in ("tp", "fp", "fn", "tn"))
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and (precision + recall) > 0
        else None
    )
    return {
        "samples": tp + fp + fn + tn,
        "positives": tp + fn,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def ranking_metrics(df: DataFrame, probability_col: str, label_col: str) -> dict[str, float | None]:
    """Threshold-free metrics; undefined (None) when the set has a single class."""
    positives = df.filter(F.col(label_col) == 1.0).count()
    total = df.count()
    if positives == 0 or positives == total:
        return {"pr_auc": None, "roc_auc": None}
    out: dict[str, float | None] = {}
    for name, metric in (("pr_auc", "areaUnderPR"), ("roc_auc", "areaUnderROC")):
        evaluator = BinaryClassificationEvaluator(
            rawPredictionCol=probability_col, labelCol=label_col, metricName=metric
        )
        out[name] = float(evaluator.evaluate(df))
    return out
