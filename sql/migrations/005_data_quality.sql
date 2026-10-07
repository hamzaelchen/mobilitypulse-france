-- Data-quality check results, one row per check and per run. Thresholds are conventions (see the
-- `threshold` column of each row), not measurements.

CREATE TABLE {{schema}}.data_quality_results (
    run_id      uuid             NOT NULL,
    checked_at  timestamptz      NOT NULL,
    layer       text             NOT NULL CHECK (layer IN ('lake', 'serving')),
    name        text             NOT NULL,
    status      text             NOT NULL CHECK (status IN ('PASS', 'WARN', 'FAIL')),
    value       double precision,
    threshold   text             NOT NULL,
    detail      text             NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, layer, name)
);
CREATE INDEX data_quality_results_time_idx ON {{schema}}.data_quality_results (checked_at);

-- Results of the most recent run only.
CREATE VIEW {{schema}}.v_data_quality_latest AS
SELECT *
FROM {{schema}}.data_quality_results
WHERE run_id = (
    SELECT run_id FROM {{schema}}.data_quality_results ORDER BY checked_at DESC LIMIT 1
);
