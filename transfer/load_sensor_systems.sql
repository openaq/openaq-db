-- Stage table matches the SOURCE schema (columns present in the CSV).
CREATE TEMP TABLE stage_sensor_systems (
    sensor_systems_id  int,
    sensor_nodes_id    int,
    metadata           jsonb,
    source_id          text,
    added_on           timestamptz,
    modified_on        timestamptz,
    instruments_id     int,
    deployed_by        int,
    deployed_on        date
);

\COPY stage_sensor_systems FROM PSTDIN WITH (FORMAT CSV, HEADER)

-- Only load sensor_systems whose parent sensor_node actually made it in.
-- Because sensor_nodes was deduped, some staged systems will point at
-- sensor_nodes_id values that were collapsed away.
INSERT INTO public.sensor_systems AS s (
    sensor_systems_id,
    sensor_nodes_id,
    metadata,
    source_id,
    added_on,
    modified_on,
    instruments_id,
    deployed_by,
    deployed_on
)
OVERRIDING SYSTEM VALUE
SELECT
    sensor_systems_id,
    sensor_nodes_id,
    metadata,
    source_id,
    added_on,
    modified_on,
    instruments_id,
    deployed_by,
    deployed_on
FROM stage_sensor_systems
WHERE sensor_nodes_id IN (SELECT sensor_nodes_id FROM public.sensor_nodes)
  --AND instruments_id  IN (SELECT instruments_id  FROM public.instruments)
  --AND (deployed_by IS NULL OR deployed_by IN (SELECT entities_id FROM public.entities))
  ;


\echo Sensor systems loaded:
WITH orphans AS (
    SELECT count(*) AS n
    FROM stage_sensor_systems s
    WHERE s.sensor_nodes_id NOT IN (SELECT sensor_nodes_id FROM public.sensor_nodes)
)
SELECT
    (SELECT count(*) FROM stage_sensor_systems)  AS staged,
    (SELECT n FROM orphans)                       AS skipped_orphans,
    (SELECT count(*) FROM public.sensor_systems)  AS total;
