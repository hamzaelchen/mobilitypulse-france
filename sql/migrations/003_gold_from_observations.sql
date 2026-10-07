-- Gold semantics changed: the time axis is now the observation time (poll time), no longer the
-- feed's last_reported, which turned out not to move when the counters do (measured on the real
-- feed: 14 of 1016 counter changes came with a new last_reported).
--
-- Gold is derived data and fully reloadable from the Parquet files, so the affected tables are
-- recreated and the hourly table is emptied: its previous content was computed with the old,
-- wrong semantics and must not survive. Run `python -m loader.main` afterwards to reload.

DROP VIEW {{schema}}.v_station_state;
DROP TABLE {{schema}}.station_series_15min;
DROP TABLE {{schema}}.station_current;
TRUNCATE {{schema}}.station_hourly_rates;

ALTER TABLE {{schema}}.load_log ADD COLUMN deleted integer NOT NULL DEFAULT 0;

CREATE TABLE {{schema}}.station_current (
    source                    text             NOT NULL,
    station_id                text             NOT NULL,
    station_name              text             NOT NULL,
    latitude                  double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude                 double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    capacity                  integer          CHECK (capacity >= 0),
    num_bikes_available       integer          NOT NULL CHECK (num_bikes_available >= 0),
    num_docks_available       integer          NOT NULL CHECK (num_docks_available >= 0),
    num_bikes_disabled        integer,
    num_docks_disabled        integer,
    fill_rate                 double precision CHECK (fill_rate >= 0),
    is_empty                  boolean          NOT NULL,
    is_full                   boolean          NOT NULL,
    is_operational            boolean          NOT NULL,
    observed_at               timestamptz      NOT NULL,
    last_reported_at          timestamptz,
    first_observed_at         timestamptz      NOT NULL,
    last_change_at            timestamptz,
    minutes_since_observed    double precision NOT NULL,
    hours_in_current_state    double precision NOT NULL CHECK (hours_in_current_state >= 0),
    state_age_is_lower_bound  boolean          NOT NULL,
    as_of                     timestamptz      NOT NULL,
    loaded_at                 timestamptz      NOT NULL DEFAULT now(),
    updated_at                timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (source, station_id)
);

CREATE TABLE {{schema}}.station_series_15min (
    source                text             NOT NULL,
    station_id            text             NOT NULL,
    slot_start            timestamptz      NOT NULL,
    bikes_available       integer          NOT NULL CHECK (bikes_available >= 0),
    docks_available       integer          NOT NULL CHECK (docks_available >= 0),
    capacity              integer,
    fill_rate             double precision,
    is_empty              boolean          NOT NULL,
    is_full               boolean          NOT NULL,
    is_operational        boolean          NOT NULL,
    min_bikes_available   integer,
    max_bikes_available   integer,
    mean_bikes_available  double precision,
    n_readings            integer          NOT NULL CHECK (n_readings >= 1),
    n_changes             integer          NOT NULL CHECK (n_changes >= 0),
    is_complete           boolean          NOT NULL,
    loaded_at             timestamptz      NOT NULL DEFAULT now(),
    updated_at            timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (source, station_id, slot_start)
);
CREATE INDEX station_series_15min_slot_idx ON {{schema}}.station_series_15min (slot_start);

-- Dashboard-facing view. critical_type is null for a station in normal state. An out-of-service
-- station is reported as such, never as empty/full: its counters say nothing about service level.
-- hours_in_current_state is a lower bound when state_age_is_lower_bound is true.
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
    observed_at,
    minutes_since_observed,
    hours_in_current_state,
    state_age_is_lower_bound,
    as_of
FROM {{schema}}.station_current;
