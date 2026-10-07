-- Heartbeats of the live pipeline components, so that the UI can tell a stopped collection from a
-- stopped updater instead of presenting old data as fresh. One row per component, overwritten at
-- each beat; `detail` carries small facts (last file applied, rows applied, last error).

CREATE TABLE {{schema}}.pipeline_status (
    component   text         PRIMARY KEY,
    beat_at     timestamptz  NOT NULL,
    detail      jsonb        NOT NULL DEFAULT '{}'::jsonb
);

-- The live updater keeps station_current up to date between two Gold runs. It must recognise a
-- change of these service flags after a restart, so it stores them (the Gold loader leaves them
-- null: it only knows is_operational).
ALTER TABLE {{schema}}.station_current
    ADD COLUMN is_installed boolean,
    ADD COLUMN is_renting   boolean,
    ADD COLUMN is_returning boolean;
