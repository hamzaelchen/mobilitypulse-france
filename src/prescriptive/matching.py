"""Greedy transfer recommendations from donor stations to receiving stations (pure Python).

Receivers (empty or low stations) are served by decreasing priority score. For each one, the donors
are tried with those that themselves lack free docks first (a transfer then fixes two problems),
then by increasing haversine distance. A donor never gives more than its surplus, and a transfer
respects ``max_distance_km``, ``min_transfer`` and ``max_transfer``. The result is deterministic:
ties are broken by station id.

This is a heuristic, not an optimal routing: it does not model vehicles, routes or time.
"""

from __future__ import annotations

from dataclasses import dataclass

from prescriptive.geo import haversine_km
from prescriptive.scoring import Params, StationScore


@dataclass(frozen=True)
class Transfer:
    source: str
    from_station_id: str
    to_station_id: str
    bikes: int
    distance_km: float
    from_state: str
    to_state: str
    priority_score: float  # of the receiving station
    rank: int  # 1 = first to do


def recommend_transfers(scores: list[StationScore], p: Params) -> list[Transfer]:
    receivers = sorted(
        (s for s in scores if s.need_type == "needs_bikes" and s.bikes_gap >= 1),
        key=lambda s: (-s.priority_score, s.station.station_id),
    )
    donors = [s for s in scores if s.donor_surplus >= p.min_transfer]
    surplus = {(d.station.source, d.station.station_id): d.donor_surplus for d in donors}

    found: list[Transfer] = []
    for receiver in receivers:
        remaining = receiver.bikes_gap
        r = receiver.station
        candidates = []
        for donor in donors:
            d = donor.station
            if d.source != r.source or d.station_id == r.station_id:
                continue
            if surplus[(d.source, d.station_id)] < p.min_transfer:
                continue
            distance = haversine_km(r.latitude, r.longitude, d.latitude, d.longitude)
            if distance <= p.max_distance_km:
                candidates.append((donor.need_type != "needs_docks", distance, d.station_id, donor))
        candidates.sort(key=lambda c: c[:3])
        for _, distance, _, donor in candidates:
            if remaining < p.min_transfer:
                break
            key = (donor.station.source, donor.station.station_id)
            quantity = min(remaining, surplus[key], p.max_transfer)
            if quantity < p.min_transfer:
                continue
            surplus[key] -= quantity
            remaining -= quantity
            found.append(
                Transfer(
                    source=r.source,
                    from_station_id=donor.station.station_id,
                    to_station_id=r.station_id,
                    bikes=quantity,
                    distance_km=round(distance, 6),
                    from_state=donor.state,
                    to_state=receiver.state,
                    priority_score=receiver.priority_score,
                    rank=0,
                )
            )

    found.sort(key=lambda t: (-t.priority_score, t.distance_km, t.to_station_id, t.from_station_id))
    return [Transfer(**{**t.__dict__, "rank": i}) for i, t in enumerate(found, start=1)]
