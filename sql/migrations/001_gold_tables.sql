-- Gold tables served to Grafana. {{schema}} is replaced by the migration runner.
-- Rule: an applied migration is never edited (its checksum is verified); add a new file.

CREATE TABLE {{schema}}.station_current (
    source                   text             NOT NULL,
    station_id               text             NOT NULL,
    station_name             text             NOT NULL,
    latitude                 double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude                double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    capacity                 integer          CHECK (capacity >= 0),
    num_bikes_available      integer          NOT NULL CHECK (num_bikes_available >= 0),
    num_docks_available      integer          NOT NULL CHECK (num_docks_available >= 0),
    num_bikes_disabled       integer,
    num_docks_disabled       integer,
    fill_rate                double precision CHECK (fill_rate >= 0),
    is_empty                 boolean          NOT NULL,
    is_full                  boolean          NOT NULL,
    is_operational           boolean          NOT NULL,
    event_timestamp          timestamptz      NOT NULL,
    hours_since_last_change  double precision NOT NULL,
    as_of                    timestamptz      NOT NULL,
    loaded_at                timestamptz      NOT NULL DEFAULT now(),
    updated_at               timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (source, station_id)
);

CREATE TABLE {{schema}}.station_series_15min (
    source               text             NOT NULL,
    station_id           text             NOT NULL,
    slot_start           timestamptz      NOT NULL,
    bikes_available      integer          NOT NULL CHECK (bikes_available >= 0),
    docks_available      integer          NOT NULL CHECK (docks_available >= 0),
    capacity             integer,
    fill_rate            double precision,
    is_empty             boolean          NOT NULL,
    is_full              boolean          NOT NULL,
    is_operational       boolean          NOT NULL,
    min_bikes_available  integer,
    max_bikes_available  integer,
    n_updates            integer          NOT NULL CHECK (n_updates >= 0),
    is_complete          boolean          NOT NULL,
    loaded_at            timestamptz      NOT NULL DEFAULT now(),
    updated_at           timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (source, station_id, slot_start)
);
CREATE INDEX station_series_15min_slot_idx ON {{schema}}.station_series_15min (slot_start);

CREATE TABLE {{schema}}.station_hourly_rates (
    source                    text             NOT NULL,
    hour_start                timestamptz      NOT NULL,
    stations_observed         integer          NOT NULL,
    station_coverage          double precision NOT NULL CHECK (station_coverage BETWEEN 0 AND 1),
    slot_observations         integer          NOT NULL,
    operational_observations  integer          NOT NULL,
    empty_rate                double precision CHECK (empty_rate BETWEEN 0 AND 1),
    full_rate                 double precision CHECK (full_rate BETWEEN 0 AND 1),
    non_operational_rate      double precision NOT NULL CHECK (non_operational_rate BETWEEN 0 AND 1),
    is_complete               boolean          NOT NULL,
    loaded_at                 timestamptz      NOT NULL DEFAULT now(),
    updated_at                timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (source, hour_start)
);

-- One row per dataset and per load: used to show data freshness.
CREATE TABLE {{schema}}.load_log (
    id          bigserial   PRIMARY KEY,
    dataset     text        NOT NULL,
    loaded_at   timestamptz NOT NULL DEFAULT now(),
    rows_read   integer     NOT NULL,
    inserted    integer     NOT NULL,
    updated     integer     NOT NULL,
    unchanged   integer     NOT NULL
);
