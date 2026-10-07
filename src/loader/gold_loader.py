"""Gold Parquet -> PostgreSQL, as idempotent upserts in a single transaction.

Per table: rows are COPYed into a temporary staging table, then merged with
``INSERT ... ON CONFLICT DO UPDATE ... WHERE <data> IS DISTINCT FROM <staged>``. Re-running the
load on unchanged Gold data therefore inserts and updates nothing.

The table is then mirrored: rows that Gold no longer contains (inside the loaded time range) are
deleted, so a corrected Gold cannot leave stale rows behind in PostgreSQL. An EMPTY Gold dataset
deletes nothing: it more likely means a failed job than a network without stations.
The whole dataset of each table is read in memory: fine for a development-scale Gold.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from loader.migrations import validate_identifier

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableSpec:
    name: str  # PostgreSQL table and Gold directory name
    columns: tuple[str, ...]
    key: tuple[str, ...]
    partition_column: str  # Hive partition column of the Gold dataset (used by --days)
    timestamp_columns: tuple[str, ...]
    time_column: str = ""  # bounds the mirror deletion when only recent partitions are loaded

    @property
    def data_columns(self) -> tuple[str, ...]:
        return tuple(c for c in self.columns if c not in self.key)


SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        name="station_current",
        columns=(
            "source", "station_id", "station_name", "latitude", "longitude", "capacity",
            "num_bikes_available", "num_docks_available", "num_bikes_disabled",
            "num_docks_disabled", "fill_rate", "is_empty", "is_full", "is_operational",
            "observed_at", "last_reported_at", "first_observed_at", "last_change_at",
            "minutes_since_observed", "hours_in_current_state", "state_age_is_lower_bound",
            "as_of",
        ),
        key=("source", "station_id"),
        partition_column="",  # not partitioned
        timestamp_columns=(
            "observed_at", "last_reported_at", "first_observed_at", "last_change_at", "as_of",
        ),
    ),
    TableSpec(
        name="station_series_15min",
        columns=(
            "source", "station_id", "slot_start", "bikes_available", "docks_available",
            "capacity", "fill_rate", "is_empty", "is_full", "is_operational",
            "min_bikes_available", "max_bikes_available", "mean_bikes_available",
            "n_readings", "n_changes", "is_complete",
        ),
        key=("source", "station_id", "slot_start"),
        partition_column="slot_date",
        timestamp_columns=("slot_start",),
        time_column="slot_start",
    ),
    TableSpec(
        name="station_hourly_rates",
        columns=(
            "source", "hour_start", "stations_observed", "station_coverage", "slot_observations",
            "operational_observations", "empty_rate", "full_rate", "non_operational_rate",
            "is_complete",
        ),
        key=("source", "hour_start"),
        partition_column="hour_date",
        timestamp_columns=("hour_start",),
        time_column="hour_start",
    ),
)  # fmt: skip


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows_read: int
    inserted: int
    updated: int
    deleted: int = 0

    @property
    def unchanged(self) -> int:
        return self.rows_read - self.inserted - self.updated


def build_upsert_sql(schema: str, spec: TableSpec) -> tuple[str, str, str]:
    """Returns (create staging table, COPY into staging, merge) statements."""
    schema = validate_identifier(schema)
    table = f"{schema}.{validate_identifier(spec.name)}"
    columns = ", ".join(spec.columns)
    key = ", ".join(spec.key)
    data = spec.data_columns
    create_stage = (
        f"CREATE TEMP TABLE _stage_{spec.name} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
    )
    copy_stage = f"COPY _stage_{spec.name} ({columns}) FROM STDIN"
    assignments = ", ".join([f"{c} = EXCLUDED.{c}" for c in data] + ["updated_at = now()"])
    stored = ", ".join(f"t.{c}" for c in data)
    staged = ", ".join(f"EXCLUDED.{c}" for c in data)
    merge = (
        f"INSERT INTO {table} AS t ({columns}) SELECT {columns} FROM _stage_{spec.name} "
        f"ON CONFLICT ({key}) DO UPDATE SET {assignments} "
        f"WHERE ({stored}) IS DISTINCT FROM ({staged}) "
        "RETURNING (xmax = 0) AS inserted"
    )
    return create_stage, copy_stage, merge


def build_mirror_delete_sql(schema: str, spec: TableSpec, bounded: bool) -> str:
    """Deletes the rows of the table that are not in the staging table (same key).

    ``bounded``: only rows whose ``time_column`` is >= a lower bound (one ``%s`` parameter).
    """
    schema = validate_identifier(schema)
    table = f"{schema}.{validate_identifier(spec.name)}"
    same_key = " AND ".join(f"s.{c} = t.{c}" for c in spec.key)
    sql = (
        f"DELETE FROM {table} t WHERE NOT EXISTS "
        f"(SELECT 1 FROM _stage_{spec.name} s WHERE {same_key})"
    )
    if bounded:
        sql += f" AND t.{validate_identifier(spec.time_column)} >= %s"
    return sql


def specs_without_current() -> list[TableSpec]:
    """Every table except ``station_current``, which the live updater keeps up to date."""
    return [spec for spec in SPECS if spec.name != "station_current"]


def _as_utc(value: datetime) -> datetime:
    # Spark writes UTC instants; pyarrow may return them without a timezone.
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def read_gold_rows(
    gold_dir: Path, spec: TableSpec, since: date | None = None
) -> list[tuple[Any, ...]]:
    """Rows of one Gold dataset as tuples ordered like ``spec.columns``."""
    import pyarrow as pa
    import pyarrow.dataset as ds  # lazy: only the loader image needs pyarrow

    path = gold_dir / spec.name
    if not path.exists():
        raise FileNotFoundError(f"Gold dataset not found: {path}")

    # Declare the partition type: inferred as string, a date filter would not compare.
    partitioning = (
        ds.partitioning(pa.schema([(spec.partition_column, pa.date32())]), flavor="hive")
        if spec.partition_column
        else None
    )
    dataset = ds.dataset(path, format="parquet", partitioning=partitioning)
    missing = [c for c in spec.columns if c not in dataset.schema.names]
    if missing:
        raise ValueError(f"{spec.name}: columns missing in the Parquet files: {missing}")
    flt = None
    if since is not None and spec.partition_column:
        flt = ds.field(spec.partition_column) >= since
    table = dataset.to_table(columns=list(spec.columns), filter=flt)
    stamps = {spec.columns.index(c) for c in spec.timestamp_columns}
    rows = []
    for record in table.to_pylist():
        values = [record[c] for c in spec.columns]
        for i in stamps:
            if values[i] is not None:
                values[i] = _as_utc(values[i])
        rows.append(tuple(values))
    return rows


def load_table(
    conn,
    schema: str,
    spec: TableSpec,
    rows: Sequence[Sequence[Any]],
    *,
    since: date | None = None,
) -> LoadResult:
    """Upsert ``rows`` (ordered like ``spec.columns``) and mirror deletions. In a transaction.

    ``since``: the load only covers partitions from that date on, so deletions are limited to
    rows at or after it (tables without a ``time_column`` are always mirrored entirely).
    """
    key_positions = [spec.columns.index(c) for c in spec.key]
    keys = {tuple(row[i] for i in key_positions) for row in rows}
    if len(keys) != len(rows):
        raise ValueError(f"{spec.name}: {len(rows) - len(keys)} duplicated primary keys in input")
    create_stage, copy_stage, merge = build_upsert_sql(schema, spec)
    conn.execute(create_stage)
    with conn.cursor() as cur:
        with cur.copy(copy_stage) as copy:
            for row in rows:
                copy.write_row(row)
        flags = [r[0] for r in cur.execute(merge).fetchall()]
        deleted = 0
        if rows:
            bounded = since is not None and bool(spec.time_column)
            lower = datetime(since.year, since.month, since.day, tzinfo=UTC) if bounded else None
            delete_sql = build_mirror_delete_sql(schema, spec, bounded)
            cur.execute(delete_sql, (lower,) if bounded else None)
            deleted = cur.rowcount
        else:
            logger.warning("%s: empty Gold dataset, nothing deleted", spec.name)
    inserted = sum(flags)
    return LoadResult(spec.name, len(rows), inserted, len(flags) - inserted, deleted)


def record_load(conn, schema: str, result: LoadResult) -> None:
    schema = validate_identifier(schema)
    conn.execute(
        f"INSERT INTO {schema}.load_log "
        "(dataset, rows_read, inserted, updated, unchanged, deleted) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            result.table,
            result.rows_read,
            result.inserted,
            result.updated,
            result.unchanged,
            result.deleted,
        ),
    )


def load_gold(
    conn,
    schema: str,
    gold_dir: Path,
    *,
    since: date | None = None,
    specs: Iterable[TableSpec] = SPECS,
) -> list[LoadResult]:
    """Load every Gold dataset atomically: either all tables are updated or none."""
    results: list[LoadResult] = []
    with conn.transaction():
        for spec in specs:
            rows = read_gold_rows(gold_dir, spec, since)
            result = load_table(conn, schema, spec, rows, since=since)
            record_load(conn, schema, result)
            results.append(result)
            logger.info(
                "Table loaded: %s rows=%d inserted=%d updated=%d unchanged=%d deleted=%d",
                result.table, result.rows_read, result.inserted, result.updated,
                result.unchanged, result.deleted,
            )  # fmt: skip
    return results
