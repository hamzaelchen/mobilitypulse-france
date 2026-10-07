-- Dashboard-facing view. critical_type is null for a station in normal state.
-- An out-of-service station is reported as such, never as empty/full: its counters are
-- not meaningful for service level.

CREATE VIEW {{schema}}.v_station_state AS
SELECT
    source,
    station_id,
    station_name,
    latitude,
    longitude,
    capacity,
    num_bikes_available,
    num_docks_available,
    fill_rate,
    is_operational,
    CASE
        WHEN NOT is_operational THEN 'out_of_service'
        WHEN is_empty THEN 'empty'
        WHEN is_full THEN 'full'
    END AS critical_type,
    event_timestamp,
    hours_since_last_change,
    as_of
FROM {{schema}}.station_current;
