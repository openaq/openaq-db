
BEGIN;

--------------------------------------------------------
-- To revert back to using the groups_id we just need --
-- to uncomment out the groups views and then         --
-- uncomment out the lines in the sensor_stats view   --
-- which is around line 212
--------------------------------------------------------

CREATE TABLE IF NOT EXISTS analyses_summary as
SELECT sensors_id
, min(datetime) as first_datetime
, max(datetime) as last_datetime
, last(value,datetime)as last_value
, count(*) as value_count
, sum(value) as value_sum
, min(lon) as minx
, min(lat) as miny
, max(lon) as maxx
, max(lat) as maxy
, st_makepoint(last(lon, datetime)
, last(lat, datetime))::geography as last_point
FROM analyses
GROUP BY sensors_id;

DROP MATERIALIZED VIEW IF EXISTS sensors_first_last;
CREATE MATERIALIZED VIEW sensors_first_last AS
SELECT
    sensors_id,
    first_datetime,
    last_datetime,
    last_value
FROM
    rollups
WHERE rollup='total'
;
CREATE INDEX ON sensors_first_last (sensors_id);

DROP VIEW IF EXISTS sensor_nodes_sources_view CASCADE;
CREATE OR REPLACE VIEW sensor_nodes_sources_view AS
SELECT
    sensor_nodes_id,
    coalesce(jsonb_agg(jsonb_build_object(
        'id', slug,
        'name', name,
        'readme', CASE WHEN readme is null then null else '/v2/sources/readme/' || slug END
    ) || coalesce(metadata, '{}'::jsonb)
    ), '{}'::jsonb) as sources
FROM sensor_nodes_sources
LEFT JOIN sources USING (sources_id)
GROUP BY sensor_nodes_id;

DROP VIEW IF EXISTS sensor_nodes_ext CASCADE;
CREATE OR REPLACE VIEW sensor_nodes_ext AS
SELECT
    sn.sensor_nodes_id,
    sn.country,
    sn.city,
    sn.geom,
    sn.ismobile,
    case when sn.geom is null and not ismobile then false else true end as has_geo,
    sn.source_name,
    sn.origin,
    jsonb_strip_nulls(coalesce(sn.metadata-'{attribution}'::text[], '{}'::jsonb)
    || o.metadata
    || jsonb_build_object('pvals', to_jsonb(h)-'{sensor_nodes_id}'::text[])
    || jsonb_build_object('sources', sources)
    )
    as metadata

FROM
    sensor_nodes sn
    LEFT JOIN
    sensor_nodes_harrays h
    USING (sensor_nodes_id)
    LEFT JOIN
    origins o USING (origin)
    LEFT JOIN
    sensor_nodes_sources_view USING (sensor_nodes_id)
;


DROP MATERIALIZED VIEW IF EXISTS groups_view_pre CASCADE;
CREATE OR REPLACE VIEW groups_view_pre AS
SELECT
groups_id,
groups.type,
groups.name,
groups.subtitle,
groups.metadata,
measurands_id,
measurand,
units,
sensors_id,
sensor_nodes_id,
country
FROM groups
LEFT JOIN groups_sensors USING (groups_id)
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN measurands USING (measurands_id)
LEFT JOIN sensor_systems using (sensor_systems_id)
LEFT JOIN sensor_nodes_ext USING (sensor_nodes_id)
;

DROP MATERIALIZED VIEW IF EXISTS groups_sources_classify CASCADE;
CREATE MATERIALIZED VIEW groups_sources_classify AS
SELECT
    groups_id,
    m.ismobile as "isMobile",
    a.is_analysis as "isAnalysis",
    e.entity,
    s."sensorType"
FROM groups
LEFT JOIN LATERAL(
    SELECT EXISTS (
        SELECT 1 FROM sensor_nodes LEFT JOIN
        sensor_systems USING (sensor_nodes_id) LEFT JOIN
        sensors USING (sensor_systems_id)
        LEFT JOIN groups_sensors gs using (sensors_id)
        WHERE gs.groups_id=groups.groups_id and ismobile
    ) as ismobile
) as m ON TRUE
LEFT JOIN LATERAL(
    SELECT EXISTS (
        SELECT 1 FROM sensor_nodes sn LEFT JOIN
        sensor_systems USING (sensor_nodes_id) LEFT JOIN
        sensors USING (sensor_systems_id)
        LEFT JOIN groups_sensors gs using (sensors_id)
        WHERE gs.groups_id=groups.groups_id and (sn.metadata->>'is_analysis')::bool
    ) as is_analysis
) as a ON TRUE
LEFT JOIN LATERAL(
    SELECT sensor_nodes.metadata->>'entity' as entity, count(*) FROM sensor_nodes LEFT JOIN
        sensor_systems USING (sensor_nodes_id) LEFT JOIN
        sensors USING (sensor_systems_id)
        LEFT JOIN groups_sensors gs using (sensors_id)
        WHERE gs.groups_id=groups.groups_id group by 1 order by 2 desc limit 1
) as e ON TRUE
LEFT JOIN LATERAL (
        SELECT sensor_nodes.metadata->>'sensorType' as "sensorType", count(*) FROM sensor_nodes LEFT JOIN
        sensor_systems USING (sensor_nodes_id) LEFT JOIN
        sensors USING (sensor_systems_id)
        LEFT JOIN groups_sensors gs using (sensors_id)
        WHERE gs.groups_id=groups.groups_id group by 1 order by 2 desc limit 1
) as s ON TRUE
WHERE groups.type='source';
CREATE UNIQUE INDEX ON groups_sources_classify(groups_id);


DROP MATERIALIZED VIEW IF EXISTS groups_view CASCADE;
CREATE MATERIALIZED VIEW groups_view AS
SELECT
    groups_id,
    type,
    name,
    subtitle,
    coalesce(metadata, '{}'::jsonb) as metadata,
    measurands_id,
    measurand,
    units,
    "isMobile",
    "isAnalysis",
    "sensorType",
    "entity",
    array_agg(DISTINCT sensors_id) as sensors_id_arr,
    array_agg(DISTINCT sensor_nodes_id) as sensor_nodes_arr,
    array_agg(DISTINCT country) FILTER (WHERE country is not null) as countries,
    count(distinct sensor_nodes_id) as locations
FROM groups_view_pre
LEFT JOIN groups_sources_classify USING (groups_id)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

CREATE UNIQUE INDEX ON groups_view (groups_id, measurands_id);
ANALYZE groups_view;

DROP MATERIALIZED VIEW IF EXISTS sensor_stats;
CREATE MATERIALIZED VIEW sensor_stats AS
WITH analyses AS (
     SELECT
        sensors_id,
        value_count,
        value_sum,
        first_datetime,
        last_datetime,
        last_value,
        last_point,
        minx,
        miny,
        maxx,
        maxy
      FROM analyses_summary
    ), sensorsdata as (
      SELECT
        sensors_id,
        value_count,
        value_sum,
        first_datetime,
        last_datetime,
        last_value,
        last_point,
        minx,
        miny,
        maxx,
        maxy
      FROM rollups
      JOIN groups_view USING (groups_id, measurands_id)
      WHERE rollup='total'
      AND groups_view.type='node'
    ), out as (
      SELECT * FROM analyses
      UNION ALL
      SELECT * FROM sensorsdata
      WHERE sensors_id NOT IN (SELECT sensors_id FROM analyses)
    )
    SELECT
        out.*,
        sensor_nodes_id,
        country,
        city,
        measurands_id
    FROM out
    JOIN sensors using (sensors_id)
    JOIN sensor_systems using (sensor_systems_id)
    JOIN sensor_nodes using (sensor_nodes_id)
    ;

create unique index on sensor_stats (sensors_id);

DROP VIEW IF EXISTS sensors_full CASCADE;
CREATE OR REPLACE VIEW sensors_full AS
SELECT
    sensors_id,
    measurands_id,
    sensor_systems_id,
    sensor_nodes_id,
    measurand,
    units,
    value_count::numeric,
    value_sum / value_count as values_average,
    first_datetime,
    null::float as first_value,
    last_datetime,
    last_value,
    sensors.metadata as sensor_metadata
FROM
    sensors
    LEFT JOIN sensor_stats USING (sensors_id, measurands_id)
    LEFT JOIN measurands USING (measurands_id)
    --LEFT JOIN measurands USING (measurands_id)
    --LEFT JOIN rollups USING(sensors_id, measurands_id)
    --LEFT JOIN sensor_systems USING(sensor_systems_id)
    --WHERE rollup='total'
;


DROP VIEW IF EXISTS sensors_full_flat CASCADE;
CREATE MATERIALIZED VIEW sensors_full_flat AS
SELECT
    *
FROM
    sensors_full s
    LEFT JOIN
    sensor_nodes_ext USING (sensor_nodes_id);


DROP VIEW IF EXISTS sensors_full_json CASCADE;
CREATE OR REPLACE VIEW sensors_full_json AS
SELECT
    sensors_id,
    sensor_systems_id,
    (
        to_jsonb(sensors_full)
        - 'sensor_metadata'
    ) || coalesce(sensor_metadata, '{}'::jsonb) as json
FROM
    sensors_full
;

DROP VIEW IF EXISTS sensor_systems_json CASCADE;
CREATE OR REPLACE VIEW sensor_systems_json AS
SELECT
    sensor_systems_id,
    sensor_nodes_id,
    (to_jsonb(sensor_systems) - 'metadata') ||
    coalesce(metadata, '{}'::jsonb) ||
    jsonb_build_object(
        'sensors', json_agg(json)
    ) as json
FROM sensor_systems
LEFT JOIN sensors_full_json
USING (sensor_systems_id)
GROUP BY sensor_systems_id, sensor_nodes_id
;


DROP MATERIALIZED VIEW IF EXISTS sensor_nodes_json CASCADE;
CREATE MATERIALIZED VIEW sensor_nodes_json AS
SELECT
    sensor_nodes_id,
    geom::geography as geog,
    jsonb_strip_nulls(
        (to_jsonb(sensor_nodes_ext) - 'metadata') ||
        coalesce(metadata, '{}'::jsonb) ||
        jsonb_build_object(
            'sensor_systems', json_agg(json)
        )
    ) as json
FROM sensor_nodes_ext
LEFT JOIN sensor_systems_json
USING (sensor_nodes_id)
GROUP BY
    sensor_nodes_id,
    metadata,
    geog,
    geom,
    sensor_nodes_ext
;
CREATE INDEX ON sensor_nodes_json USING GIST (geog);
CREATE INDEX ON sensor_nodes_json USING GIN (json);


-- Stale sensor ids
-- latest version, regardless of life cycle,
-- ties are broken with sort order
-- meant to be used as:
-- sensors_id NOT IN (
DROP VIEW IF EXISTS version_ranks CASCADE;
CREATE OR REPLACE VIEW version_ranks AS
SELECT v.parent_sensors_id
, v.sensors_id
, lc.life_cycles_id
, v.version_date
, lc.sort_order
, lc.label
, row_number() OVER (
  PARTITION BY parent_sensors_id
  ORDER BY v.version_date DESC, lc.sort_order DESC
) as version_rank
FROM versions v
JOIN life_cycles lc USING (life_cycles_id);

CREATE OR REPLACE VIEW stale_versions AS
SELECT sensors_id
FROM version_ranks
WHERE version_rank > 1
UNION ALL
SELECT parent_sensors_id
FROM versions;

-- create a version of the sensor stats view that will not use the groups pattern
-- the purpose of this query is to be able to update other queries that use the
-- sensor_stats table without making too many changes that might be difficult to
-- undo if we change the way that we are doing the versions
DROP VIEW IF EXISTS sensor_stats_versioning CASCADE;
CREATE VIEW sensor_stats_versioning AS
-- start by rolling up the measurements
WITH m AS (
SELECT sensors_id
, COUNT(1) as value_count
, SUM(value) as value_sum
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, last(value, datetime) as last_value
, last(lon, datetime) as lastx
, last(lat, datetime) as lasty
, MIN(lon) as minx
, MAX(lon) as maxx
, MIN(lat) as miny
, MAX(lat) as maxy
FROM measurements
GROUP BY sensors_id)
-- and then add the attribution and versioning information
SELECT m.sensors_id
, m.value_count
, m.value_sum
, m.first_datetime
, m.last_datetime
, m.last_value
, COALESCE(m.minx, st_x(sn.geom)) as minx
, COALESCE(m.maxx, st_x(sn.geom)) as maxx
, COALESCE(m.miny, st_y(sn.geom)) as miny
, COALESCE(m.maxy, st_y(sn.geom)) as maxy
, st_setsrid(st_point(COALESCE(m.maxx, st_x(sn.geom)), COALESCE(m.maxy, st_y(sn.geom))), 4326)::geography as last_point
, v.parent_sensors_id IS NOT NULL as is_versioned
, v.parent_sensors_id
, (v.version_rank IS NULL OR v.version_rank = 1) as is_latest
, v.version_date
, v.life_cycles_id
, v.label as life_cycles_label
, sn.sensor_nodes_id
, s.measurands_id
, sn.city
, sn.country
FROM m
JOIN sensors s ON (s.sensors_id = m.sensors_id)
JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
LEFT JOIN version_ranks v ON (v.sensors_id = s.sensors_id);



DROP materialized view if exists measurements_fastapi_base;
CREATE MATERIALIZED VIEW measurements_fastapi_base AS
SELECT
    sensor_nodes_id,
    sensors_id,
    measurands_id,
    site_name,
    measurand,
    sensor_nodes.metadata->>'entity' as entity,
    sensor_nodes.metadata->>'sensorType' as "sensorType",
    sensor_nodes.metadata->>'timezone' as timezone,
    geom::geography as geog,
    units,
    country,
    city,
    ismobile,
    v.parent_sensors_id IS NOT NULL as is_versioned,
    v.parent_sensors_id,
    -- All non versioned sensors should be considered the latest
    (v.version_rank IS NULL OR v.version_rank = 1) as is_latest,
    v.version_date,
    v.life_cycles_id,
    v.label as life_cycles_label,
    (sensor_nodes.metadata->>'is_analysis')::boolean as is_analysis,
    source_name as "sourceName",
    sensor_nodes.metadata->'attribution' as attribution,
    jsonb_build_object('unit', 'seconds', 'value', sensors.metadata->'data_averaging_period_seconds') as "averagingPeriod"
FROM
    sensor_nodes
    LEFT JOIN sensor_systems USING (sensor_nodes_id)
    LEFT JOIN sensors USING (sensor_systems_id)
    LEFT JOIN measurands USING (measurands_id)
    LEFT JOIN version_ranks v using (sensors_id)
;
CREATE UNIQUE INDEX ON measurements_fastapi_base (sensors_id);
CREATE INDEX ON measurements_fastapi_base  (sensor_nodes_id);
CREATE INDEX ON measurements_fastapi_base  (site_name);
CREATE INDEX ON measurements_fastapi_base  (measurand);
CREATE INDEX ON measurements_fastapi_base  (country);
CREATE INDEX ON measurements_fastapi_base  (city);
CREATE INDEX ON measurements_fastapi_base USING GIST (geog);

-- 2021-11-30
-- Has been updated to use handle versions
-- to remove the versions method just change the sensor_stats table
-- and then remove the extra fields from the parameters section of the query
DROP MATERIALIZED VIEW IF EXISTS locations_base_v2;
CREATE MATERIALIZED VIEW locations_base_v2 AS
WITH base AS (
            SELECT
                sensor_nodes.sensor_nodes_id,
                sensors.sensors_id,
                site_name,
                json,
                ismobile,
                sensor_nodes.city,
                sensor_nodes.country,
                source_name,
                mfr(sensor_systems.metadata) mfr,
                value_count,
                value_sum,
                first_datetime,
                last_datetime,
                last_value,
                measurand,
                units,
                sensors.measurands_id,
                geom,
                last_point,
                minx,
                miny,
                maxx,
                maxy,
                (sensor_nodes.metadata->>'is_analysis')::bool as "isAnalysis"
                , ss.is_versioned
                , ss.parent_sensors_id
                , ss.is_latest
                , ss.version_date
                , ss.life_cycles_id
                , ss.life_cycles_label
            FROM
            sensor_nodes
            JOIN sensor_nodes_json USING (sensor_nodes_id)
            JOIN sensor_systems USING(sensor_nodes_id)
            JOIN sensors USING (sensor_systems_id)
            -- JOIN sensor_stats using (sensors_id)
            JOIN sensor_stats_versioning ss using (sensors_id)
            JOIN measurands on (measurands.measurands_id = sensors.measurands_id)
            LEFT JOIN version_ranks v using (sensors_id)
        ),
        overall AS (
        SELECT
            sensor_nodes_id as "id",
            site_name as "name",
            json->>'sensorType' as "sensorType",
            json->>'entity' as "entity",
            ismobile as "isMobile",
            "isAnalysis",
            city,
            country,
            json->'sources' as sources,
            jsonb_build_object(
                'longitude', st_x(coalesce((last(last_point, last_datetime))::geometry, geom)),
                'latitude', st_y(coalesce((last(last_point, last_datetime))::geometry, geom))
            ) as coordinates,
            jsonb_agg(DISTINCT mfr) FILTER (WHERE mfr is not Null) as manufacturers,
            sum(value_count) as measurements,
            min(first_datetime) as "firstUpdated",
            max(last_datetime) as "lastUpdated",
            json,
            coalesce((last(last_point, last_datetime))::geometry, geom) as sgeom,
            coalesce((last(last_point, last_datetime)), geom::geography) as geog,
            CASE WHEN ismobile THEN to_jsonb(ARRAY[min(minx), min(miny), max(maxx), max(maxy)]) ELSE NULL::jsonb END as bounds
        FROM base
        GROUP BY id, name,city,country,json,geom,sources,"sensorType","isMobile","isAnalysis"
        ),
        byparameter AS (
            SELECT
                sensors_id as id,
                sensor_nodes_id,
                measurand as parameter,
                units as unit,
                measurands_id as "parameterId",
                value_count as count,
                value_sum / value_count as average,
                first_datetime as "firstUpdated",
                last_datetime as "lastUpdated",
                last_value as "lastValue"
                , is_latest as "isLatest"
                , is_versioned as "isVersioned"
                , version_date as "versionDate"
                , life_cycles_label as "lifeCycle"
                , parent_sensors_id as "parentSensorsId"
                , jsonb_agg(DISTINCT mfr) FILTER (WHERE mfr is not Null) as manufacturers
            FROM
            base
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
        )
        SELECT
            overall.*,
            jsonb_agg((to_jsonb(byparameter) || parameter("parameterId"))-'{sensor_nodes_id}'::text[]) as parameters
        FROM overall
        LEFT JOIN byparameter ON (overall.id=sensor_nodes_id)
        GROUP BY
            overall.id,
            name,
            city,
            country,
            coordinates,
            overall."firstUpdated",
            overall."lastUpdated",
            "sensorType",
            "entity",
            "isMobile",
            "isAnalysis",
            measurements,
            json,
            sources,
            geog,
            sgeom,
            bounds,
            overall.manufacturers
        ORDER BY "lastUpdated" DESC;


CREATE INDEX ON locations_base_v2 ("lastUpdated");
CREATE INDEX ON locations_base_v2 (country);
CREATE INDEX ON locations_base_v2 USING GIST(geog);
CREATE INDEX ON locations_base_v2 USING GIN(parameters);
CREATE INDEX ON locations_base_v2 (id);
CREATE INDEX ON locations_base_v2 (name);

DROP MATERIALIZED VIEW IF EXISTS public.locations;
CREATE MATERIALIZED VIEW locations AS WITH s AS (
SELECT
       sensor_nodes_id as location_id,
       site_name as location,
       sensors_id,
       measurand,
       measurands_id,
       units,
       sn.country,
       ismobile,
       (sn.metadata->>'is_analysis')::bool as is_analysis,
       sn.metadata->>'sensorType' as "sensorType",
       last_datetime,
       st_transform(COALESCE(last_point, sn.geom)::geometry, 3857) as geom,
       minx,
       miny,
       maxx,
       maxy, --st_transform(bounds::geometry, 3857) as bounds,
       value_count,
       last_value
FROM
    sensor_stats
    JOIN sensors using (sensors_id, measurands_id)
    join sensor_nodes sn using(sensor_nodes_id)
    join measurands using (measurands_id)
) SELECT
    location_id,
    location,
    sensors_id,
    measurands_id,
    measurand,
    units,
    country,
    ismobile,
    is_analysis,
    "sensorType",
    max(last_datetime) as "last_datetime",
    last(geom, last_datetime) as geom,
    st_transform(bounds(min(minx), min(miny), max(maxx), max(maxy)), 3857) as bounds,
    sum(value_count) as count,
    last(last_value, last_datetime) as last_value

FROM
    s
WHERE geom IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10;
CREATE INDEX ON locations USING GIST (geom);
CREATE INDEX ON locations USING GIST (bounds) where ismobile;
create index on locations (last_datetime);
create index on locations (location_id);
create index on locations (measurands_id);

create or replace view measurements_analyses AS
SELECT * FROM measurements
UNION ALL
SELECT * FROM analyses;

DROP MATERIALIZED VIEW IF EXISTS  country_stats ;
CREATE MATERIALIZED VIEW country_stats AS
WITH m AS (
    SELECT
        country as code,
        coalesce(name, country) as name,
        count(distinct city) as cities,
        count(distinct sensor_nodes_id) as locations,
        sum(value_count) as count,
        min(first_datetime) as "firstUpdated",
        max(last_datetime) as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sensor_stats
    LEFT JOIN measurands using (measurands_id)
    LEFT JOIN countries cl on (country=iso)
    GROUP BY 1,2
)
SELECT  *, sources_in_country(code) as sources
FROM m;


DROP MATERIALIZED VIEW IF EXISTS  city_stats ;
CREATE MATERIALIZED VIEW city_stats AS
WITH m AS (
    SELECT
        country as code,
        coalesce(name, country) as name,
        city,
        count(distinct sensor_nodes_id) as locations,
        sum(value_count) as count,
        min(first_datetime) as "firstUpdated",
        max(last_datetime) as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sensor_stats
    LEFT JOIN measurands using (measurands_id)
    LEFT JOIN countries cl on (country=iso)
    GROUP BY 1,2,3
)
SELECT *, sources_in_city(code) as sources
FROM m;


/*
DROP MATERIALIZED VIEW IF EXISTS mobile_gen CASCADE;
CREATE MATERIALIZED VIEW mobile_gen AS
SELECT
    sensor_nodes_id,
    st_snaptogrid(pt3857(lon,lat),30) as geom,
    count(*)
FROM
    measurements
    LEFT JOIN sensors USING (sensors_id)
    LEFT JOIN sensor_systems USING (sensor_systems_id)
WHERE
    lat is not null and lon is not null
GROUP BY
    1,2
;
CREATE INDEX ON mobile_gen (sensors_id);
CREATE INDEX ON mobile_gen USING GIST (geom, sensors_id);

CREATE MATERIALIZED VIEW mobile_gen_boxes AS
SELECT
    sensor_nodes_id,
    st_expand(st_extent(geom),20)::geometry as box
FROM
    mobile_gen
GROUP BY 1;
CREATE INDEX ON mobile_gen_boxes (sensors_id);
CREATE INDEX ON mobile_gen_boxes USING GIST (box, sensors_id);
*/




COMMIT;
