"""Is there enough data to train and evaluate? (pure Python, no Spark)

The thresholds are conventions, not measured facts: with fewer positive events than this, a
precision / recall is dominated by chance. They are explicit, configurable, and reported with
every run.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Thresholds:
    min_train_samples: int = 500
    min_train_positives: int = 30
    min_test_positives: int = 30
    min_test_onsets: int = 30  # positives among samples NOT critical at prediction time


@dataclass(frozen=True)
class SplitCounts:
    train_samples: int
    train_positives: int
    test_samples: int
    test_positives: int
    test_onsets: int  # test samples critical at t+30 but not at t
    test_negatives: int


@dataclass(frozen=True)
class Assessment:
    reliable: bool
    reasons: tuple[str, ...]  # why not, empty when reliable


def assess(counts: SplitCounts, thresholds: Thresholds | None = None) -> Assessment:
    thresholds = thresholds or Thresholds()
    reasons: list[str] = []
    if counts.train_samples < thresholds.min_train_samples:
        reasons.append(f"train samples {counts.train_samples} < {thresholds.min_train_samples}")
    if counts.train_positives < thresholds.min_train_positives:
        reasons.append(
            f"train positives {counts.train_positives} < {thresholds.min_train_positives}"
        )
    if counts.test_positives < thresholds.min_test_positives:
        reasons.append(f"test positives {counts.test_positives} < {thresholds.min_test_positives}")
    if counts.test_onsets < thresholds.min_test_onsets:
        reasons.append(
            f"test onsets (not critical now, critical in 30 min) {counts.test_onsets} "
            f"< {thresholds.min_test_onsets}"
        )
    if counts.test_negatives == 0 or counts.test_positives == 0:
        reasons.append("test set has a single class: AUC is undefined")
    return Assessment(reliable=not reasons, reasons=tuple(reasons))
