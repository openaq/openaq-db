BEGIN;
-- this is just a shim to fix the missing last function issue
-- should explore not using it
CREATE OR REPLACE FUNCTION public.last_time_agg (anyelement, anyelement, timestamptz )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $2;
$$;

DROP AGGREGATE IF EXISTS public.last (anyelement, timestamptz) CASCADE;
CREATE AGGREGATE public.last(anyelement, timestamptz) (
        sfunc    = public.last_time_agg,
        stype = anyelement
);


--CREATE OR REPLACE FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any")
--RETURNS internal
--AS '@MODULE_PATHNAME@', 'ts_last_sfunc'
--LANGUAGE C IMMUTABLE PARALLEL SAFE;

create table if not exists analyses_summary as
SELECT sensors_id
, min(datetime) as first_datetime
, max(datetime) as last_datetime
, last(value,datetime) as last_value
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
    || COALESCE(o.metadata, '{}'::jsonb)
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
FROM
groups_view_pre LEFT JOIN
groups_sources_classify USING (groups_id)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

CREATE UNIQUE INDEX ON groups_view (groups_id, measurands_id);
ANALYZE groups_view;

DROP MATERIALIZED VIEW IF EXISTS sensor_stats;
CREATE MATERIALIZED VIEW sensor_stats AS
WITH
    analyses AS
    (SELECT
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
    ),
    sensorsdata as (
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
    FROM
        rollups
    LEFT JOIN groups_view USING (groups_id, measurands_id)
    WHERE
        rollup='total' and groups_view.type='node'
    ),
    out as (
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
    (sensor_nodes.metadata->>'is_analysis')::boolean as is_analysis,
    source_name as "sourceName",
    sensor_nodes.metadata->'attribution' as attribution,
    jsonb_build_object('unit', 'seconds', 'value', sensors.metadata->'data_averaging_period_seconds') as "averagingPeriod"
FROM
    sensor_nodes
    LEFT JOIN sensor_systems USING (sensor_nodes_id)
    LEFT JOIN sensors USING (sensor_systems_id)
    LEFT JOIN measurands USING (measurands_id)
;
CREATE UNIQUE INDEX ON measurements_fastapi_base (sensors_id);
CREATE INDEX ON measurements_fastapi_base  (sensor_nodes_id);
CREATE INDEX ON measurements_fastapi_base  (site_name);
CREATE INDEX ON measurements_fastapi_base  (measurand);
CREATE INDEX ON measurements_fastapi_base  (country);
CREATE INDEX ON measurements_fastapi_base  (city);
CREATE INDEX ON measurements_fastapi_base USING GIST (geog);




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
            FROM
            sensor_nodes
            JOIN sensor_nodes_json USING (sensor_nodes_id)
            JOIN sensor_systems USING(sensor_nodes_id)
            JOIN sensors USING (sensor_systems_id)
            JOIN sensor_stats using (sensors_id)
            JOIN measurands on (measurands.measurands_id = sensors.measurands_id)
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
               -- 'longitude', st_x(coalesce((last(last_point, last_datetime))::geometry, geom)),
               -- 'latitude', st_y(coalesce((last(last_point, last_datetime))::geometry, geom))
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
        group by id, name,city,country,json,geom,sources,"sensorType","isMobile","isAnalysis"
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
                last_value as "lastValue",
                jsonb_agg(DISTINCT mfr) FILTER (WHERE mfr is not Null) as manufacturers
            FROM
            base
            GROUP BY 1,2,3,4,5,6,7,8,9,10
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
CREATE MATERIALIZED VIEW locations AS
WITH s AS (
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

DROP VIEW IF EXISTS sensor_nodes_check;
CREATE OR REPLACE VIEW sensor_nodes_check AS
SELECT sn.sensor_nodes_id
, sn.site_name
, sn.source_name
, sn.source_id
, sn.origin
, COALESCE(p.label, 'Not found') as provider
, st_astext(COALESCE(l.geom_latest, sn.geom)) as location
, sy.sensor_systems_id
, sn.added_on
, s.sensors_id
, COALESCE(m.measurand, 'Not found') as parameter
, l.datetime_first as datetime_first
, l.datetime_last as datetime_last
, CASE WHEN l.datetime_first < l.datetime_last THEN
    ROUND(l.value_count::numeric/(EXTRACT(EPOCH FROM l.datetime_last - l.datetime_first)/s.data_logging_period_seconds)::numeric * 100)
  ELSE NULL END as percent_complete
, s.added_on as sensor_added_on
, sn.added_on as node_added_on
FROM sensor_nodes sn
LEFT JOIN providers p ON (sn.providers_id = p.providers_id)
LEFT JOIN sensor_systems sy USING (sensor_nodes_id)
LEFT JOIN sensors s USING (sensor_systems_id)
LEFT JOIN measurands m USING (measurands_id)
LEFT JOIN sensors_rollup l USING (sensors_id);

DROP VIEW IF EXISTS sensor_nodes_summary;
CREATE OR REPLACE VIEW sensor_nodes_summary AS
SELECT sn.sensor_nodes_id
, sn.site_name
, sn.source_name
, sn.source_id
, sn.origin
, sn.ismobile
, geom IS NOT NULL as has_coordinates
, COUNT(sy.sensor_systems_id) as systems_count
, COUNT(s.sensors_id) as sensors_count
, COUNT(DISTINCT s.measurands_id) as parameters_count
, array_agg(json_build_object(
    'sensors_id', s.sensors_id
    , 'measurand', m.measurand
)) as sensors_list
FROM sensor_nodes sn
LEFT JOIN sensor_systems sy USING (sensor_nodes_id)
LEFT JOIN sensors s USING (sensor_systems_id)
LEFT JOIN measurands m USING (measurands_id)
GROUP BY sn.sensor_nodes_id
, sn.site_name
, sn.source_name
, sn.source_id
, sn.origin
, sn.ismobile;



DROP FUNCTION IF EXISTS split_ingest_id(text);
DROP FUNCTION IF EXISTS split_ingest_id(text, int);
DROP FUNCTION IF EXISTS check_ingest_id(text);

CREATE OR REPLACE FUNCTION split_ingest_id(iid text) RETURNS text[] AS $$
WITH arr AS (
SELECT iid as ingest_id
, string_to_array(iid,'-') as iid)
  SELECT ARRAY[
     iid[1]
    -- deals with case where source_id (from client) has a dash in it
    , CASE WHEN array_length(iid, 1) < 3 THEN 'N/A'
           ELSE array_to_string(iid[2:(array_length(iid, 1)-1)], '-')
           END
    , iid[array_length(iid, 1)]
  ]
  FROM arr;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION split_ingest_id(iid text, pos int) RETURNS text AS $$
SELECT (split_ingest_id(iid))[pos];
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION check_ingest_id(iid text) RETURNS TABLE(
  ingest_id text
, source_name text
, source_id text
, parameter text
, measurands_id int
, measurand text
, units text
) AS $$
WITH arr AS (
SELECT iid as ingest_id
, split_ingest_id(iid) as iid
), sen AS (
  SELECT ingest_id
  , iid[1] as source_name
  , iid[2] as source_id
  , iid[3] as parameter
  FROM arr)
SELECT s.*
, m.measurands_id
, m.measurand
, m.units
FROM sen s
LEFT JOIN measurands_map_view mp ON (mp.key = s.parameter)
LEFT JOIN measurands m ON (mp.measurands_id = m.measurands_id);
$$ LANGUAGE SQL;

CREATE OR REPLACE VIEW measurements_ingest_format_view AS
SELECT sensor_nodes_id as id
, source_name||'-'||s.source_id||'-'|| measurand as ingest_id
, v.value
, to_json(v.datetime)#>>'{}' as datetime
, v.lon
, v.lat
FROM measurements v
JOIN sensors s USING (sensors_id)
JOIN measurands USING (measurands_id)
JOIN sensor_systems USING (sensor_systems_id)
JOIN sensor_nodes USING (sensor_nodes_id);


SELECT sensor_nodes_id as id
, source_name||'/'||s.sensors_id||'/'|| measurand as ingest_id
, v.value
, to_json(v.datetime)#>>'{}' as datetime
, COALESCE(v.lon, st_x(geom))
, COALESCE(v.lat, st_y(geom))
, sn.metadata->>'timezone'
FROM measurements v
JOIN sensors s USING (sensors_id)
JOIN measurands m USING (measurands_id)
JOIN sensor_systems ss USING (sensor_systems_id)
JOIN sensor_nodes sn USING (sensor_nodes_id)
LIMIT 10;




CREATE OR REPLACE VIEW analyses_ingest_format_view AS
SELECT sensor_nodes_id as id
, source_name||'-'||s.source_id||'-'|| measurand as ingest_id
, v.value
, to_json(v.datetime)#>>'{}' as datetime
, v.lon
, v.lat
FROM analyses v
JOIN sensors s USING (sensors_id)
JOIN measurands USING (measurands_id)
JOIN sensor_systems USING (sensor_systems_id)
JOIN sensor_nodes USING (sensor_nodes_id);

DROP VIEW IF EXISTS public.active_locks;
CREATE OR REPLACE VIEW public.active_locks AS
SELECT t.schemaname,
    t.relname,
    l.locktype,
    l.page,
    l.virtualtransaction,
    l.pid,
    l.mode,
    l.granted,
    substr(query, 0, 40) as query,
    age(now(), a.query_start) as age
   FROM pg_locks l
   JOIN pg_stat_all_tables t ON l.relation = t.relid
   JOIN pg_stat_activity a ON (l.pid = a.pid)
  WHERE t.schemaname <> 'pg_toast'::name
  AND t.schemaname <> 'pg_catalog'::name
  ORDER BY t.schemaname, t.relname;


SELECT l.pid
       , COUNT(1)
   FROM pg_locks l
   JOIN pg_stat_all_tables t ON l.relation = t.relid
   JOIN pg_stat_activity a ON (l.pid = a.pid)
  WHERE t.schemaname <> 'pg_toast'::name
  AND t.schemaname <> 'pg_catalog'::name
  GROUP BY 1;



CREATE FUNCTION table_row_estimator(table_name text) RETURNS bigint
   LANGUAGE plpgsql AS
$$
DECLARE
   plan jsonb;
BEGIN
   EXECUTE 'EXPLAIN (FORMAT JSON) SELECT * FROM ' || table_name INTO plan;
   RETURN (plan->0->'Plan'->>'Plan Rows')::bigint;
END;
$$;

CREATE OR REPLACE VIEW active_ingestions AS
SELECT pid
, COUNT(1) as n
, MAX(age(now(), query_start)) as age
FROM pg_stat_activity
WHERE query~*'ingest'
AND pg_backend_pid() != pid
GROUP BY 1;

CREATE OR REPLACE FUNCTION cancel_ingestions(age interval DEFAULT '4h') RETURNS TABLE(
 pid int
 , locks bigint
 , age interval
 , canceled bigint
) AS $$
 SELECT pid
, COUNT(1) as locks
, MAX(age(now(), query_start)) as process_age
, COUNT(pg_cancel_backend(pid)) as canceled
FROM pg_stat_activity
WHERE query~*'ingest'
AND pg_backend_pid() != pid
AND age(now(), query_start) > age
GROUP BY 1;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION cancel_processes(pattern text, age interval DEFAULT '4h') RETURNS TABLE(
 pid int
 , locks bigint
 , age interval
 , canceled bigint
) AS $$
 SELECT pid
, COUNT(1) as locks
, MAX(age(now(), query_start)) as process_age
, COUNT(pg_cancel_backend(pid)) as canceled
FROM pg_stat_activity
WHERE query~*pattern
AND pg_backend_pid() != pid
AND age(now(), query_start) > age
GROUP BY 1;
$$ LANGUAGE SQL;


CREATE OR REPLACE VIEW fetchlogs_pending AS
SELECT date_trunc('hour', init_datetime) as added_on
, MIN(loaded_datetime) as loaded_min
, MAX(loaded_datetime) as loaded_max
, MAX(age(now(), init_datetime)) as oldest
, SUM(jobs) as jobs
, COUNT(1) as n
, array_agg(fetchlogs_id) as fetchlogs
, array_agg(DISTINCT batch_uuid) as batches
FROM fetchlogs
WHERE completed_datetime IS NULL
AND NOT has_error
GROUP BY 1
ORDER BY 1 DESC;

DROP VIEW IF EXISTS fetchlogs_recent_summary;
CREATE OR REPLACE VIEW fetchlogs_recent_summary AS
SELECT date_trunc('hour', init_datetime) as added_on
, MIN(age(completed_datetime, init_datetime)) as ingest_time_min
, MAX(age(completed_datetime, init_datetime)) as ingest_time_max
, AVG(age(completed_datetime, init_datetime)) as ingest_time_avg
, MIN(age(completed_datetime, loaded_datetime)) as load_time_min
, MAX(age(completed_datetime, loaded_datetime)) as load_time_max
, AVG(age(completed_datetime, loaded_datetime)) as load_time_avg
, COUNT(1) as files_added
, SUM((completed_datetime IS NULL)::int) as files_pending
, ROUND(AVG(jobs)) as jobs_avg
, SUM((has_error)::int) as errors
, SUM(records) as records
, SUM(inserted) as inserted
FROM fetchlogs
WHERE init_datetime > now() - '24h'::interval
GROUP BY 1
ORDER BY 1 DESC;

DROP VIEW IF EXISTS fetchlogs_daily_summary;
CREATE OR REPLACE VIEW fetchlogs_daily_summary AS
SELECT init_datetime::date as added_on
, MIN(age(completed_datetime, init_datetime)) as ingest_time_min
, MAX(age(completed_datetime, init_datetime)) as ingest_time_max
, AVG(age(completed_datetime, init_datetime)) as ingest_time_avg
, MIN(age(completed_datetime, loaded_datetime)) as load_time_min
, MAX(age(completed_datetime, loaded_datetime)) as load_time_max
, AVG(age(completed_datetime, loaded_datetime)) as load_time_avg
, COUNT(1) as files_added
, SUM((completed_datetime IS NULL)::int) as files_pending
, ROUND(AVG(jobs)) as jobs_avg
, SUM((has_error)::int) as errors
, SUM(records) as records
, SUM(inserted) as inserted
, SUM(file_size) as total_size
, ROUND(AVG(file_size)) as avg_size
, ROUND(SUM(inserted)::numeric/SUM(records)::numeric * 100, 1) as pct
FROM fetchlogs
WHERE init_datetime::date >= current_date - 14
GROUP BY 1
ORDER BY 1 DESC;


CREATE OR REPLACE VIEW fetchlogs_hourly_summary AS
SELECT date_trunc('hour', init_datetime) as added_on
, MIN(age(completed_datetime, init_datetime)) as ingest_time_min
, MAX(age(completed_datetime, init_datetime)) as ingest_time_max
, AVG(age(completed_datetime, init_datetime)) as ingest_time_avg
, MIN(age(completed_datetime, loaded_datetime)) as load_time_min
, MAX(age(completed_datetime, loaded_datetime)) as load_time_max
, AVG(age(completed_datetime, loaded_datetime)) as load_time_avg
, COUNT(1) as files_added
, SUM((completed_datetime IS NULL)::int) as files_pending
, ROUND(AVG(jobs)) as jobs_avg
, SUM((has_error)::int) as errors
, SUM(records) as records
, SUM(inserted) as inserted
, SUM(file_size) as total_size
, ROUND(AVG(file_size)) as avg_size
, ROUND(SUM(inserted)::numeric/SUM(records)::numeric * 100, 1) as pct
FROM fetchlogs
WHERE init_datetime::date >= current_date - 1
GROUP BY 1
ORDER BY 1 DESC;



DROP VIEW IF EXISTS fetchlogs_recent_issues;
CREATE OR REPLACE VIEW fetchlogs_recent_issues AS
SELECT fetchlogs_id
, init_datetime
, key
, jobs
, age(last_modified, init_datetime) as modified_time
, age(completed_datetime, init_datetime) as ingeset_time
, age(completed_datetime, loaded_datetime) as loaded_time
, has_error
, EXTRACT(EPOCH FROM age(completed_datetime, init_datetime))/3600 as hours_ago
FROM fetchlogs
WHERE completed_datetime > now() - '24h'::interval
AND key !~* 'station'
AND (has_error OR jobs > 1 OR age(completed_datetime, init_datetime) > '1h'::interval);


CREATE OR REPLACE FUNCTION clean_sensor_nodes() RETURNS VOID AS $$
BEGIN
DELETE
FROM sensors
WHERE sensors_id IN (SELECT sensors_id FROM sensor_nodes_check WHERE datetime_first IS NULL);
DELETE
FROM sensor_systems
WHERE sensor_systems_id NOT IN (SELECT sensor_systems_id FROM sensors);
DELETE
FROM sensor_nodes_sources
WHERE sensor_nodes_id NOT IN (SELECT sensor_nodes_id FROM sensor_systems);
DELETE
FROM sensor_nodes
WHERE sensor_nodes_id NOT IN (SELECT sensor_nodes_id FROM sensor_systems);
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS records_inserted(text, timestamptz, text);
CREATE OR REPLACE FUNCTION records_inserted(
   text DEFAULT 'hour'
 , timestamptz DEFAULT current_date - 1
 , text DEFAULT 'realtime'
 ) RETURNS TABLE(
   datetime timestamptz
 , period text
 , pattern text
 , inserted bigint
 , records bigint
 , percentage numeric
 , files bigint
 , min int
 , max int
) AS $$
WITH inserted AS (
 SELECT date_trunc($1, init_datetime) as datetime
 , SUM(inserted) as inserted
 , SUM(records) as records
 , COUNT(1) as files
 , MIN(inserted) as min
 , MAX(inserted) as max
 FROM fetchlogs
 WHERE init_datetime>$2
 AND key~* $3
 GROUP BY 1
 ORDER BY 1)
 SELECT datetime
 , $1 as period
 , $3 as pattern
 , inserted
 , records
 , CASE WHEN records>0 THEN ROUND((inserted::numeric/records::numeric) * 100.0) ELSE 0 END as percentage
 , files
 , min
 , max
 FROM inserted;
$$ LANGUAGE SQL;


CREATE OR REPLACE VIEW providers_check AS
WITH nodes AS (
  SELECT sensor_nodes_id
  , MIN(sl.datetime_first) as datetime_first
  , MAX(sl.datetime_last) as datetime_last -- need to change
  , COUNT(1) as sensors_count
  FROM sensor_systems ss
  JOIN sensors s USING (sensor_systems_id)
  JOIN sensors_rollup sl USING (sensors_id)
  JOIN measurands m USING (measurands_id)
  GROUP BY sensor_nodes_id)
SELECT p.label
, MIN(datetime_first) as datetime_first
, MIN(datetime_last) as datetime_last
, COUNT(1) as sensor_nodes_count
, ROUND(AVG(sensors_count)::numeric, 1) as sensors_avg
, SUM(sensors_count) as sensors_count
FROM nodes n
JOIN sensor_nodes sn ON (sn.sensor_nodes_id = n.sensor_nodes_id)
JOIN providers p ON (sn.providers_id = p.providers_id)
GROUP BY 1
ORDER BY lower(p.label);


-- a convenience view to aid in querying a all lists a user has permissions to
CREATE OR REPLACE VIEW user_lists_view AS
WITH owner_users AS (
    SELECT DISTINCT
        lists_id
        , users_id 
		, 'owner' AS role
    FROM 
        lists 
),
list_users AS (
	SELECT lists_id
	,users_id
	, role::text
	FROM users_lists
	UNION 
	SELECT lists_id
	,users_id
	, role::text
	FROM owner_users
),
user_count AS (
    SELECT lists_id 
    , COUNT(*) AS user_count
	FROM 
	lists
    JOIN 
    	list_users lu USING (lists_id)
	GROUP BY lists_id
)
SELECT 
    l.lists_id
    , l.users_id AS owner_id
    , lu.users_id
	, lu.role
    , l.label
    , l.description
    , visibility
    , uc.count as user_count
    , COUNT(*) as locations_count
FROM    
    lists l
JOIN 
    sensor_nodes_list snl USING (lists_id)
JOIN 
    list_users lu USING (lists_id)
JOIN 
    user_count uc USING (lists_id)
GROUP BY
    1,2,3,4,5,6



SELECT * FROM records_inserted('day', current_date - 8, 'realtime')
UNION ALL
SELECT * FROM records_inserted('day', current_date - 8, 'purple')
UNION ALL
SELECT * FROM records_inserted('day', current_date - 8, 'clarity')
UNION ALL
SELECT * FROM records_inserted('day', current_date - 8, 'senstate');


--SELECT * FROM parse_ingest_id('CMU-Technology Center-pm25');

-- SELECT *
-- FROM sensor_nodes_check
-- WHERE sensor_nodes_id = 23642;

-- SELECT *
-- FROM sensor_nodes_check
-- WHERE sensors_id = 1152;

-- SELECT r->>'ingest_id' as key
-- , COUNT(1) as n
-- FROM rejects
-- GROUP BY 1
-- LIMIT 1000;


COMMIT;
