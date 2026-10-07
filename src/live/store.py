"""PostgreSQL side of the live path: station_current upserts, derived times, heartbeats."""

from __future__ import annotations

from datetime import datetime, timedelta

from psycopg import sql
from psycopg.types.json import Jsonb

from live.state import Key, StationRow, state_times

# Insert order; the first two are the primary key.
COLUMNS = (
    "source", "station_id", "station_name", "latitude", "longitude", "capacity",
    "num_bikes_available", "num_docks_available", "num_bikes_disabled", "num_docks_disabled",
    "is_installed", "is_renting", "is_returning", "fill_rate", "is_empty", "is_full",
    "is_operational", "observed_at", "last_reported_at", "first_observed_at", "last_change_at",
    "minutes_since_observed", "hours_in_current_state", "state_age_is_lower_bound", "as_of",
)  # fmt: skip


def _table(schema: str, name: str) -> sql.Identifier:
    return sql.Identifier(schema, name)


def _values(row: StationRow, as_of: datetime) -> tuple:
    minutes, hours, lower_bound = state_times(row, as_of)
    return (
        row.source, row.station_id, row.station_name, row.latitude, row.longitude, row.capacity,
        row.bikes, row.docks, row.bikes_disabled, row.docks_disabled,
        row.is_installed, row.is_renting, row.is_returning, row.fill_rate, row.is_empty,
        row.is_full, row.is_operational, row.observed_at, row.last_reported_at,
        row.first_observed_at, row.last_change_at,
        max(minutes, 0.0), max(hours, 0.0), lower_bound, as_of,
    )  # fmt: skip


def upsert_rows(conn, schema: str, rows: list[StationRow], as_of: datetime) -> None:
    """Upsert; a row never replaces a newer one already stored (observed_at only moves forward)."""
    if not rows:
        return
    table = _table(schema, "station_current")
    updates = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in COLUMNS[2:]
    )
    query = sql.SQL(
        """
        INSERT INTO {t} ({cols}) VALUES ({marks})
        ON CONFLICT (source, station_id) DO UPDATE SET {updates}, updated_at = now()
        WHERE EXCLUDED.observed_at >= {t}.observed_at
        """
    ).format(
        t=table,
        cols=sql.SQL(", ").join(map(sql.Identifier, COLUMNS)),
        marks=sql.SQL(", ").join(sql.Placeholder() * len(COLUMNS)),
        updates=updates,
    )
    with conn.cursor() as cur:
        cur.executemany(query, [_values(row, as_of) for row in rows])


def refresh_derived(conn, schema: str, as_of: datetime) -> None:
    """Move the clock-dependent columns forward for every station (no new data needed)."""
    conn.execute(
        sql.SQL(
            """
            UPDATE {t} SET
                as_of = %(as_of)s,
                minutes_since_observed =
                    greatest(extract(epoch FROM %(as_of)s - observed_at) / 60, 0),
                hours_in_current_state =
                    greatest(extract(epoch FROM %(as_of)s
                                     - coalesce(last_change_at, first_observed_at)) / 3600, 0)
            """
        ).format(t=_table(schema, "station_current")),
        {"as_of": as_of},
    )


def prune_stale(conn, schema: str, now: datetime, older_than: timedelta) -> list[Key]:
    """Drop stations not seen for ``older_than`` (removed from the feed)."""
    rows = conn.execute(
        sql.SQL("DELETE FROM {t} WHERE observed_at < %s RETURNING source, station_id").format(
            t=_table(schema, "station_current")
        ),
        (now - older_than,),
    ).fetchall()
    return [(source, station_id) for source, station_id in rows]


def write_heartbeat(conn, schema: str, component: str, beat_at: datetime, detail: dict) -> None:
    conn.execute(
        sql.SQL(
            """
            INSERT INTO {t} (component, beat_at, detail) VALUES (%s, %s, %s)
            ON CONFLICT (component) DO UPDATE SET beat_at = EXCLUDED.beat_at,
                                                  detail = EXCLUDED.detail
            """
        ).format(t=_table(schema, "pipeline_status")),
        (component, beat_at, Jsonb(detail)),
    )


def load_rows(conn, schema: str) -> dict[Key, StationRow]:
    """Current rows as in-memory state. The Kafka offset is not stored: -1 sorts before any."""
    records = conn.execute(
        sql.SQL(
            """
            SELECT source, station_id, station_name, latitude, longitude, capacity,
                   num_bikes_available, num_docks_available, num_bikes_disabled,
                   num_docks_disabled, is_installed, is_renting, is_returning, is_operational,
                   observed_at, last_reported_at, first_observed_at, last_change_at
            FROM {t}
            """
        ).format(t=_table(schema, "station_current"))
    ).fetchall()
    rows: dict[Key, StationRow] = {}
    for rec in records:
        flags = rec[10:13]
        if None in flags and rec[13]:  # seeded by the Gold loader: operational means all true
            flags = (True, True, True)
        rows[(rec[0], rec[1])] = StationRow(
            source=rec[0], station_id=rec[1], station_name=rec[2], latitude=rec[3],
            longitude=rec[4], capacity=rec[5], bikes=rec[6], docks=rec[7],
            bikes_disabled=rec[8], docks_disabled=rec[9], is_installed=flags[0],
            is_renting=flags[1], is_returning=flags[2], last_reported_at=rec[15],
            observed_at=rec[14], kafka_offset=-1, first_observed_at=rec[16],
            last_change_at=rec[17],
        )  # fmt: skip
    return rows
