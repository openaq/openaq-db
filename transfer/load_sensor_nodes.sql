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
