-- Stage table matches the SOURCE schema (columns present in the CSV).
-- Note: geom comes in as EWKB hex text; cast on insert.
CREATE TEMP TABLE stage_sensor_nodes (
    sensor_nodes_id    int,
    ismobile           boolean,
    geom               text,           -- EWKB hex; cast to geometry on INSERT
    site_name          text,
    source_name        text,
    city               text,
    geocoding_result   jsonb,
    country            text,
    metadata           jsonb,
    source_id          text,
    origin             text,
    is_public          boolean,
    added_on           timestamptz,
    modified_on        timestamptz,
    timezones_id       int,
    providers_id       int,
    countries_id       int,
    owner_entities_id  int
);

\COPY stage_sensor_nodes FROM PSTDIN WITH (FORMAT CSV, HEADER)

-- DELETE THE source id from realtime
-- UPDATE stage_sensor_nodes
--   SET source_id = NULL
--   WHERE source_name ~* 'airnow';

UPDATE stage_sensor_nodes
  SET source_id = sensor_nodes_id
  --WHERE providers_id IN (118,119,151,152,16,162,164,17,202,206,210,224,283,35,52,62,69,70,223);
  WHERE providers_id NOT IN (443,66,21,11,479,445,166,168,14,200,440,15,444,222,10,13); -- all the lcs providers


\echo Constraint check:
  WITH duplicates AS (
SELECT source_name
  , COALESCE(source_id, 'no source id') as source_id
  , st_astext(geom) as coords
  , COUNT(1) as n
  , string_agg(site_name, '|')
  --, MIN(added_on) as added_first
  --, MAX(added_on) as added_last
  FROM stage_sensor_nodes
  GROUP BY 1,2,3
  HAVING COUNT(1) > 1
  ORDER BY COUNT(1) DESC)
  SELECT COUNT(*) FROM duplicates;


WITH deduped AS (
    SELECT DISTINCT ON (source_name, source_id, geom) *
    FROM stage_sensor_nodes
    ORDER BY
        source_name,
        source_id,
        geom,
        added_on ASC NULLS LAST,
        sensor_nodes_id ASC
)
INSERT INTO public.sensor_nodes AS n (
    sensor_nodes_id,
    ismobile,
    geom,
    site_name,
    source_name,
    city,
    geocoding_result,
    country,
    metadata,
    source_id,
    origin,
    is_public,
    added_on,
    modified_on,
    timezones_id,
    providers_id,
    countries_id,
    owner_entities_id
)
OVERRIDING SYSTEM VALUE
SELECT
    sensor_nodes_id,
    ismobile,
    geom::geometry,
    site_name,
    source_name,
    city,
    geocoding_result,
    country,
    metadata,
    source_id,
    origin,
    is_public,
    added_on,
    modified_on,
    timezones_id,
    providers_id,
    countries_id,
    owner_entities_id
FROM deduped;


WITH orphans AS (
    SELECT count(*) AS n
    FROM stage_sensor_nodes s
    WHERE s.sensor_nodes_id NOT IN (SELECT sensor_nodes_id FROM public.sensor_nodes)
)
SELECT
    (SELECT count(*) FROM stage_sensor_nodes)  AS staged,
    (SELECT n FROM orphans)                AS skipped_orphans,
    (SELECT count(*) FROM public.sensor_nodes)  AS total;
