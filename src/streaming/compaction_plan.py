"""Which Bronze partitions can be compacted (pure Python, no Spark).

File naming is the contract between the writers and the readers of Bronze:
- the streaming job writes ``part-*`` files (one per micro-batch);
- the compaction job replaces closed partitions' ``part-*`` files with one ``compacted-*`` file;
- the Silver stream only reads ``part-*``, so it never re-reads compacted rows.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

PARTITION_PREFIX = "ingestion_date="
SMALL_FILE_GLOB = "part-*"
SMALL_FILE_SUFFIX = ".parquet"
COMPACTED_PREFIX = "compacted-"
TMP_DIR_NAME = "_compaction_tmp"  # underscore prefix: ignored by Spark readers


@dataclass(frozen=True)
class PartitionPlan:
    partition_dir: Path
    partition_date: date
    small_files: tuple[Path, ...]

    @property
    def target_name(self) -> str:
        """Deterministic name: a re-run after a crash finds the file it already produced."""
        digest = hashlib.sha1("\n".join(f.name for f in self.small_files).encode()).hexdigest()
        return f"{COMPACTED_PREFIX}{digest[:16]}.snappy.parquet"


def small_files(partition_dir: Path) -> list[Path]:
    return sorted(
        p
        for p in partition_dir.glob(SMALL_FILE_GLOB)
        if p.is_file() and p.suffix == SMALL_FILE_SUFFIX
    )


def plan_compaction(
    bronze_dir: Path,
    *,
    today: date,
    now: float,
    min_file_age_seconds: float = 600,
    min_files: int = 2,
) -> list[PartitionPlan]:
    """Partitions strictly before ``today`` with at least ``min_files`` small files, none of
    which was modified in the last ``min_file_age_seconds`` (Silver must have consumed them)."""
    plans: list[PartitionPlan] = []
    for partition_dir in sorted(bronze_dir.glob(f"{PARTITION_PREFIX}*")):
        try:
            partition_date = date.fromisoformat(partition_dir.name.removeprefix(PARTITION_PREFIX))
        except ValueError:
            continue
        if partition_date >= today:
            continue
        files = small_files(partition_dir)
        if len(files) < min_files:
            continue
        if any(now - f.stat().st_mtime < min_file_age_seconds for f in files):
            continue
        plans.append(PartitionPlan(partition_dir, partition_date, tuple(files)))
    return plans
