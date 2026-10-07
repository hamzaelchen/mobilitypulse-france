"""From station states to the two output tables (pure Python, no I/O)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from loader.gold_loader import TableSpec
from prescriptive.matching import Transfer, recommend_transfers
from prescriptive.scoring import RISK_SOURCE, Params, StationScore, StationState, score_station

PRIORITY_SPEC = TableSpec(
    name="rebalancing_priority",
    columns=(
        "source", "station_id", "station_name", "latitude", "longitude", "capacity",
        "num_bikes_available", "fill_rate", "state", "hours_in_current_state",
        "state_age_is_lower_bound", "trend_30min", "projected_fill", "urgency", "criticality",
        "priority_score", "priority_rank", "need_type", "bikes_gap", "planned_in", "planned_out",
        "unplanned_gap", "risk_source", "computed_at",
    ),
    key=("source", "station_id"),
    partition_column="",
    timestamp_columns=("computed_at",),
)  # fmt: skip

TRANSFER_SPEC = TableSpec(
    name="rebalancing_transfers",
    columns=(
        "source", "from_station_id", "to_station_id", "from_station_name", "to_station_name",
        "from_latitude", "from_longitude", "to_latitude", "to_longitude", "bikes", "distance_km",
        "from_state", "to_state", "priority_score", "transfer_rank", "risk_source", "computed_at",
    ),
    key=("source", "from_station_id", "to_station_id"),
    partition_column="",
    timestamp_columns=("computed_at",),
)  # fmt: skip


def build_outputs(
    states: list[StationState],
    params: Params,
    computed_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[StationScore], list[Transfer]]:
    """Returns (priority rows, transfer rows, scores, transfers); rows follow the table specs."""
    scores = [score_station(s, params) for s in states]
    transfers = recommend_transfers(scores, params)
    by_id = {(sc.station.source, sc.station.station_id): sc.station for sc in scores}

    planned_in: dict[tuple[str, str], int] = {}
    planned_out: dict[tuple[str, str], int] = {}
    for t in transfers:
        planned_in[(t.source, t.to_station_id)] = (
            planned_in.get((t.source, t.to_station_id), 0) + t.bikes
        )
        key = (t.source, t.from_station_id)
        planned_out[key] = planned_out.get(key, 0) + t.bikes

    ranked = sorted(scores, key=lambda sc: (-sc.priority_score, sc.station.station_id))
    priority_rows: list[tuple[Any, ...]] = []
    for rank, sc in enumerate(ranked, start=1):
        s = sc.station
        key = (s.source, s.station_id)
        p_in, p_out = planned_in.get(key, 0), planned_out.get(key, 0)
        if sc.need_type == "needs_bikes":
            unplanned = max(0, sc.bikes_gap - p_in)
        elif sc.need_type == "needs_docks":
            unplanned = max(0, sc.bikes_gap - p_out)
        else:
            unplanned = 0
        priority_rows.append(
            (
                s.source, s.station_id, s.station_name, s.latitude, s.longitude, s.capacity,
                s.bikes, s.fill_rate, sc.state, s.hours_in_state, s.state_age_is_lower_bound,
                s.trend_30min if params.use_trend else None, sc.projected_fill, sc.urgency,
                sc.criticality, sc.priority_score, rank, sc.need_type, sc.bikes_gap, p_in, p_out,
                unplanned, RISK_SOURCE, computed_at,
            )
        )  # fmt: skip

    transfer_rows = []
    for t in transfers:
        a, b = by_id[(t.source, t.from_station_id)], by_id[(t.source, t.to_station_id)]
        transfer_rows.append(
            (
                t.source, t.from_station_id, t.to_station_id, a.station_name, b.station_name,
                a.latitude, a.longitude, b.latitude, b.longitude, t.bikes, t.distance_km,
                t.from_state, t.to_state, t.priority_score, t.rank, RISK_SOURCE, computed_at,
            )
        )  # fmt: skip
    return priority_rows, transfer_rows, scores, transfers
