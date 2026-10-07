-- Prescriptive layer: station priority and recommended bike transfers.
-- The "risk" behind the score is a transparent heuristic (current fill rate + 30-minute trend),
-- NOT a trained model: risk_source says so on every row, so that a model probability can replace
-- it later without ambiguity. Thresholds and weights are conventions, not measurements.

CREATE TABLE {{schema}}.rebalancing_priority (
    source                    text             NOT NULL,
    station_id                text             NOT NULL,
    station_name              text             NOT NULL,
    latitude                  double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude                 double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    capacity                  integer,
    num_bikes_available       integer          NOT NULL CHECK (num_bikes_available >= 0),
    fill_rate                 double precision CHECK (fill_rate >= 0),
    state                     text             NOT NULL CHECK (state IN
        ('empty', 'low', 'ok', 'high', 'full', 'out_of_service', 'unknown_capacity')),
    hours_in_current_state    double precision NOT NULL CHECK (hours_in_current_state >= 0),
    state_age_is_lower_bound  boolean          NOT NULL,
    trend_30min               double precision,
    projected_fill            double precision CHECK (projected_fill BETWEEN 0 AND 1),
    urgency                   double precision NOT NULL CHECK (urgency BETWEEN 0 AND 1),
    criticality               double precision NOT NULL CHECK (criticality BETWEEN 0 AND 1),
    priority_score            double precision NOT NULL CHECK (priority_score BETWEEN 0 AND 100),
    priority_rank             integer          NOT NULL CHECK (priority_rank >= 1),
    need_type                 text             CHECK (need_type IN ('needs_bikes', 'needs_docks')),
    bikes_gap                 integer          NOT NULL CHECK (bikes_gap >= 0),
    planned_in                integer          NOT NULL CHECK (planned_in >= 0),
    planned_out               integer          NOT NULL CHECK (planned_out >= 0),
    unplanned_gap             integer          NOT NULL CHECK (unplanned_gap >= 0),
    risk_source               text             NOT NULL,
    computed_at               timestamptz      NOT NULL,
    loaded_at                 timestamptz      NOT NULL DEFAULT now(),
    updated_at                timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (source, station_id)
);
CREATE INDEX rebalancing_priority_rank_idx ON {{schema}}.rebalancing_priority (priority_rank);

CREATE TABLE {{schema}}.rebalancing_transfers (
    source             text             NOT NULL,
    from_station_id    text             NOT NULL,
    to_station_id      text             NOT NULL,
    from_station_name  text             NOT NULL,
    to_station_name    text             NOT NULL,
    from_latitude      double precision NOT NULL,
    from_longitude     double precision NOT NULL,
    to_latitude        double precision NOT NULL,
    to_longitude       double precision NOT NULL,
    bikes              integer          NOT NULL CHECK (bikes > 0),
    distance_km        double precision NOT NULL CHECK (distance_km >= 0),
    from_state         text             NOT NULL,
    to_state           text             NOT NULL,
    priority_score     double precision NOT NULL CHECK (priority_score BETWEEN 0 AND 100),
    transfer_rank      integer          NOT NULL CHECK (transfer_rank >= 1),
    risk_source        text             NOT NULL,
    computed_at        timestamptz      NOT NULL,
    loaded_at          timestamptz      NOT NULL DEFAULT now(),
    updated_at         timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (source, from_station_id, to_station_id),
    CHECK (from_station_id <> to_station_id)
);
CREATE INDEX rebalancing_transfers_rank_idx ON {{schema}}.rebalancing_transfers (transfer_rank);
