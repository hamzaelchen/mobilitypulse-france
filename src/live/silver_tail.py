"""Tail the Silver Parquet dataset: find the files written recently and turn rows into readings.

Silver is written by a Spark file sink: finished files are ``part-*.parquet`` under
``ingestion_date=*``; in-flight output lives in ``_temporary`` and the sink log in
``_spark_metadata``. A file is only read once it has been quiet for ``settle_seconds``, and a
file that cannot be read (partially written, corrupt) is skipped with a warning, never fatal:
the next cycle's overlap window gives it another chance.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from live.state import Reading

logger = logging.getLogger("live.silver_tail")

COLUMNS = [
    "source",
    "station_id",
    "station_name",
    "latitude",
    "longitude",
    "capacity",
    "num_bikes_available",
    "num_docks_available",
    "num_bikes_disabled",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
    "event_timestamp",
    "observed_at",
    "kafka_offset",
]
# A reading without these cannot be shown (they are NOT NULL in station_current).
REQUIRED = (
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
    "observed_at",
)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _partition_date(part: str) -> date | None:
    if not part.startswith("ingestion_date="):
        return None
    try:
        return date.fromisoformat(part.split("=", 1)[1])
    except ValueError:
        return None


def list_new_files(
    root: Path,
    *,
    since: datetime,
    now: datetime,
    settle_seconds: float,
    skip: set[Path] | None = None,
) -> list[Path]:
    """Finished Parquet files modified in ``[since, now - settle_seconds]``, oldest first.

    ``stat`` is the slow part on a bind mount (measured: ~4.5 ms per file, ~7 s for 1,500 files),
    so partitions older than the window are not visited, files in ``skip`` are not stat-ed, and
    files found older than the window are added to ``skip`` (they can only get older).
    """
    if not root.exists():
        return []
    lower, upper = since.timestamp(), now.timestamp() - settle_seconds
    # One hour of slack: a file written just after midnight can hold polls of the day before.
    first_day = (since - timedelta(hours=1)).astimezone(UTC).date()
    found: list[tuple[float, str, Path]] = []
    for path in root.rglob("*.parquet"):
        relative = path.relative_to(root).parts
        if any(part.startswith(("_", ".")) for part in relative):
            continue
        day = next((d for d in map(_partition_date, relative) if d), None)
        if day is not None and day < first_day:
            continue
        if skip is not None and path in skip:
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:  # removed between the listing and the stat
            continue
        if mtime < lower:
            if skip is not None:
                skip.add(path)
            continue
        if mtime <= upper:
            found.append((mtime, path.name, path))
    return [path for _, _, path in sorted(found)]


def read_readings(paths: list[Path]) -> list[Reading]:
    return read_files(paths)[0]


def read_files(paths: list[Path]) -> tuple[list[Reading], list[Path]]:
    """Readings of every readable file, and the files that could not be read (to retry later)."""
    import pyarrow as pa
    import pyarrow.parquet as pq  # lazy: only the loader image needs pyarrow

    readings: list[Reading] = []
    failed: list[Path] = []
    for path in paths:
        try:
            records = pq.ParquetFile(path).read(columns=COLUMNS).to_pylist()
        except (OSError, pa.ArrowException) as exc:
            logger.warning("Skipping unreadable Silver file %s: %s", path.name, exc)
            failed.append(path)
            continue
        for record in records:
            if any(record[name] is None for name in REQUIRED):
                continue
            readings.append(
                Reading(
                    source=record["source"],
                    station_id=record["station_id"],
                    station_name=record["station_name"],
                    latitude=record["latitude"],
                    longitude=record["longitude"],
                    capacity=record["capacity"],
                    bikes=record["num_bikes_available"],
                    docks=record["num_docks_available"],
                    bikes_disabled=record["num_bikes_disabled"],
                    docks_disabled=record["num_docks_disabled"],
                    is_installed=record["is_installed"],
                    is_renting=record["is_renting"],
                    is_returning=record["is_returning"],
                    last_reported_at=_as_utc(record["event_timestamp"]),
                    observed_at=_as_utc(record["observed_at"]),
                    kafka_offset=record["kafka_offset"] or 0,
                )
            )
    return readings, failed
