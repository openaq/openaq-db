-- Stage table matches the SOURCE schema (columns present in the CSV).
CREATE TEMP TABLE stage_sensors (
    sensors_id                     int,
    sensor_systems_id              int,
    measurands_id                  int,
    source_id                      text,
    metadata                       jsonb,
    is_public                      boolean,
    data_averaging_period_seconds  int,
    data_logging_period_seconds    int,
    added_on                       timestamptz,
    modified_on                    timestamptz,
    sensor_statuses_id             int
);

\COPY stage_sensors FROM PSTDIN WITH (FORMAT CSV, HEADER)

-- Only load sensors whose parent sensor_system actually made it in.
-- Because sensor_systems was filtered by sensor_nodes existence, some
-- staged sensors will point at sensor_systems_id values that were
-- orphaned away.
INSERT INTO public.sensors AS s (
    sensors_id,
    sensor_systems_id,
    measurands_id,
    source_id,
    metadata,
    is_public,
    data_averaging_period_seconds,
    data_logging_period_seconds,
    added_on,
    modified_on,
    sensor_statuses_id
)
OVERRIDING SYSTEM VALUE
SELECT
    sensors_id,
    sensor_systems_id,
    CASE WHEN measurands_id = 132 THEN 95 ELSE measurands_id END,
    source_id,
    metadata,
    is_public,
    data_averaging_period_seconds,
    data_logging_period_seconds,
    added_on,
    modified_on,
    sensor_statuses_id
FROM stage_sensors
WHERE sensor_systems_id IN (SELECT sensor_systems_id FROM public.sensor_systems);


\echo Sensors loaded:
WITH orphans AS (
    SELECT count(*) AS n
    FROM stage_sensors s
    WHERE s.sensors_id NOT IN (SELECT sensors_id FROM public.sensors)
)
SELECT
    (SELECT count(*) FROM stage_sensors)  AS staged,
    (SELECT n FROM orphans)                AS skipped_orphans,
    (SELECT count(*) FROM public.sensors)  AS total;
