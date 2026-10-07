"""Compute station priorities and bike transfer recommendations, and load them in PostgreSQL.

Run: ``python -m prescriptive.job [--force] [--max-data-age-minutes 30]``
(compose service ``rebalance-job``, profile ``tools``). Reads ``station_current`` and the 15-minute
series already loaded in PostgreSQL; writes ``rebalancing_priority`` and ``rebalancing_transfers``
(mirrored, in one transaction). The risk is a heuristic: see ``prescriptive.scoring``.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from common.config import ConfigError, PostgresSettings
from common.logging_utils import configure_logging, log_fields
from loader.gold_loader import LoadResult, load_table, record_load
from loader.main import connect
from loader.migrations import validate_identifier
from prescriptive.outputs import PRIORITY_SPEC, TRANSFER_SPEC, build_outputs
from prescriptive.scoring import Params, StationState

logger = logging.getLogger("rebalance_job")

TREND_MAX_STALENESS = timedelta(minutes=45)


class StaleDataError(RuntimeError):
    """The newest station reading is too old to recommend anything."""


def fetch_states(conn, schema: str) -> tuple[list[dict[str, Any]], datetime, datetime]:
    schema = validate_identifier(schema)
    rows = conn.execute(
        "SELECT source, station_id, station_name, latitude, longitude, capacity, "
        "num_bikes_available, num_docks_available, fill_rate, is_operational, "
        "hours_in_current_state, state_age_is_lower_bound, as_of, observed_at "
        f"FROM {schema}.station_current ORDER BY source, station_id"
    ).fetchall()
    if not rows:
        raise StaleDataError("station_current is empty: load Gold first")
    as_of = rows[0][12]
    newest = max(r[13] for r in rows)
    return [dict(zip(_STATE_COLUMNS, r, strict=False)) for r in rows], as_of, newest


_STATE_COLUMNS = (
    "source", "station_id", "station_name", "latitude", "longitude", "capacity", "bikes", "docks",
    "fill_rate", "is_operational", "hours_in_state", "state_age_is_lower_bound",
)  # fmt: skip


def fetch_trends(conn, schema: str, as_of: datetime) -> dict[tuple[str, str], float]:
    """Change of fill rate over the last 30 minutes, from the last complete slot.

    Empty when that slot is older than 45 minutes before ``as_of`` (pipeline stopped).
    """
    schema = validate_identifier(schema)
    last = conn.execute(
        f"SELECT max(slot_start) FROM {schema}.station_series_15min WHERE is_complete"
    ).fetchone()[0]
    if last is None or as_of - last > TREND_MAX_STALENESS:
        return {}
    rows = conn.execute(
        "SELECT a.source, a.station_id, a.fill_rate - b.fill_rate "
        f"FROM {schema}.station_series_15min a "
        f"JOIN {schema}.station_series_15min b ON b.source = a.source "
        "AND b.station_id = a.station_id AND b.slot_start = a.slot_start - interval '30 minutes' "
        "WHERE a.slot_start = %s AND a.is_complete AND b.is_complete "
        "AND a.fill_rate IS NOT NULL AND b.fill_rate IS NOT NULL",
        (last,),
    ).fetchall()
    return {(r[0], r[1]): float(r[2]) for r in rows}


def to_states(
    records: list[dict[str, Any]], trends: dict[tuple[str, str], float]
) -> list[StationState]:
    return [
        StationState(
            source=r["source"],
            station_id=r["station_id"],
            station_name=r["station_name"],
            latitude=r["latitude"],
            longitude=r["longitude"],
            capacity=r["capacity"],
            bikes=r["bikes"],
            docks=r["docks"],
            fill_rate=r["fill_rate"],
            is_operational=r["is_operational"],
            hours_in_state=r["hours_in_state"],
            state_age_is_lower_bound=r["state_age_is_lower_bound"],
            trend_30min=trends.get((r["source"], r["station_id"])),
        )
        for r in records
    ]


def run(
    conn,
    schema: str,
    params: Params,
    *,
    now: datetime,
    max_data_age_minutes: float = 30,
    force: bool = False,
) -> dict[str, Any]:
    results: list[LoadResult] = []
    # One real transaction for the reads and the writes: a consistent snapshot, and all-or-nothing.
    with conn.transaction():
        records, as_of, newest = fetch_states(conn, schema)
        age_minutes = (now - newest).total_seconds() / 60
        if age_minutes > max_data_age_minutes and not force:
            raise StaleDataError(
                f"newest reading is {age_minutes:.0f} min old (> {max_data_age_minutes:.0f} min): "
                "the pipeline may be stopped; refusing to recommend (use --force to override)"
            )
        trends = fetch_trends(conn, schema, as_of) if params.use_trend else {}
        states = to_states(records, trends)
        priority_rows, transfer_rows, scores, transfers = build_outputs(states, params, as_of)
        for spec, rows in ((PRIORITY_SPEC, priority_rows), (TRANSFER_SPEC, transfer_rows)):
            result = load_table(conn, schema, spec, rows)
            record_load(conn, schema, result)
            results.append(result)
    return {
        "computed_at": as_of.isoformat(),
        "data_age_minutes": round(age_minutes, 1),
        "stations": len(scores),
        "trend_available": sum(1 for s in states if s.trend_30min is not None),
        "receivers": sum(1 for s in scores if s.need_type == "needs_bikes"),
        "needs_docks": sum(1 for s in scores if s.need_type == "needs_docks"),
        "transfers": len(transfers),
        "bikes_moved": sum(t.bikes for t in transfers),
        "loaded": {r.table: r.__dict__ | {"unchanged": r.unchanged} for r in results},
    }


def main() -> int:
    defaults = Params()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="ignore the data freshness guard")
    parser.add_argument("--max-data-age-minutes", type=float, default=30)
    parser.add_argument("--max-distance-km", type=float, default=defaults.max_distance_km)
    parser.add_argument("--max-transfer", type=int, default=defaults.max_transfer)
    parser.add_argument("--min-transfer", type=int, default=defaults.min_transfer)
    parser.add_argument("--no-trend", action="store_true", help="ignore the 30-minute trend")
    args = parser.parse_args()

    configure_logging()
    try:
        settings = PostgresSettings.from_env()
        params = Params(
            max_distance_km=args.max_distance_km,
            max_transfer=args.max_transfer,
            min_transfer=args.min_transfer,
            use_trend=not args.no_trend,
        )
    except (ConfigError, ValueError) as exc:
        logger.error("Invalid configuration: %s", exc)
        return 2
    with connect(settings) as conn:
        try:
            summary = run(
                conn,
                settings.schema,
                params,
                now=datetime.now(UTC),
                max_data_age_minutes=args.max_data_age_minutes,
                force=args.force,
            )
        except StaleDataError as exc:
            logger.error("Not computed: %s", exc)
            return 3
    logger.info("Rebalancing computed", extra=log_fields(**summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
