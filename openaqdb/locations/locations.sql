-- Materialized view used for the locations
-- currently using materialized view because the entire table is updated at once
-- and therefor there may not be much of a speed improvement to handle this
-- manually

-- first/last updated should not be queried directly

-- Sensors rollups will store the summary for the sensors
-- entire lifespan
DROP TABLE IF EXISTS sensors_rollup;
CREATE SEQUENCE IF NOT EXISTS sensors_rollup_sq START 10;
CREATE TABLE IF NOT EXISTS sensors_rollup (
    sensors_id int PRIMARY KEY REFERENCES sensors
  , datetime_first timestamptz -- first recorded measument datetime (@ingest)
  , datetime_last timestamptz -- last recorded measurement time (@ingest)
  , geom_latest geometry -- last recorded point (@ingest)
  , value_latest double precision -- last recorded measurement (@ingest)
  , value_count int NOT NULL -- total count of measurements (@ingest, @rollup)
  , value_avg double precision -- average of all measurements (@ingest, @rollup)
  , value_sd double precision -- sd of all measurements (@ingest, @rollup)
  , value_min double precision -- lowest measurement value (@ingest, @rollup)
  , value_max double precision -- highest value measured (@ingest, @rollup)
  --, value_p05 double precision -- 5th percentile (@rollup)
  --, value_p50 double precision -- median (@rollup)
  --, value_p95 double precision -- 95th percentile (@rollup)
  , added_on timestamptz NOT NULL DEFAULT now() -- first time measurements were added (@ingest)
  , modified_on timestamptz NOT NULL DEFAULT now() -- last time we measurements were added (@ingest)
  --, calculated_on timestamptz -- last time data was rolled up (@rollup)
);



-- Sensors latest will act as a cache for the most recent
-- sensor value, managed by the ingester
CREATE TABLE IF NOT EXISTS sensors_latest (
    sensors_id int PRIMARY KEY NOT NULL REFERENCES sensors
  , datetime timestamptz
  , value double precision NOT NULL
  , lat double precision -- so that nulls dont take up space
  , lon double precision
  , modified_on timestamptz DEFAULT now()
  , fetchlogs_id int -- for debugging issues, no reference constraint
);

DROP VIEW IF EXISTS locations_view CASCADE;
CREATE OR REPLACE VIEW locations_view AS
-----------------------------
WITH nodes_instruments AS (
-----------------------------
  SELECT sn.sensor_nodes_id
  , json_agg(json_build_object(
    'id', i.instruments_id
    , 'name', i.label
    , 'manufacturer', jsonb_build_object(
      'id', i.manufacturer_entities_id
      , 'name', mc.full_name
      )
  )) as instruments
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN instruments i USING (instruments_id)
  JOIN entities mc ON (mc.entities_id = i.manufacturer_entities_id)
  GROUP BY sn.sensor_nodes_id
  -----------------------------
), nodes_sensors AS (
-----------------------------
  SELECT sn.sensor_nodes_id
  , MIN(sl.datetime_first) as datetime_first
  , MAX(sl.datetime_last) as datetime_last -- need to change
  , json_agg(jsonb_build_object(
    'id', s.sensors_id
    , 'name', m.measurand||' '||m.units
    , 'parameter', jsonb_build_object(
        'id', m.measurands_id
        , 'name', m.measurand
        , 'units', m.units
        , 'value_last', sl.value_latest
        , 'datetime_last', sl.datetime_last
        , 'display_name', m.display
        )
    )) as sensors
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN sensors s USING (sensor_systems_id)
  JOIN sensors_rollup sl USING (sensors_id)
  JOIN measurands m USING (measurands_id)
  GROUP BY sensor_nodes_id)
  -----------------------------
SELECT
  l.sensor_nodes_id as id
  , site_name as name
  , l.ismobile
  , t.tzid as timezone
-- the following is a placeholder that should
-- be replaced with something at either the instrument
-- or the provider level
  , COALESCE(l.origin, '') = 'OPENAQ' as ismonitor
  , l.city
  , jsonb_build_object(
      'id', c.countries_id
    , 'code', c.iso
    , 'name', c.name
    ) as country
  , jsonb_build_object(
      'id', oc.entities_id
    , 'name', oc.full_name
    ) as owner
  , jsonb_build_object(
      'id', p.providers_id
    , 'name', p.label
    ) as provider
  , jsonb_build_object(
      'latitude', st_y(l.geom)
    , 'longitude', st_x(l.geom)
   ) as coordinates
  , ni.instruments
  , ns.sensors
  , get_datetime_object(ns.datetime_first, t.tzid) as datetime_first
  , get_datetime_object(ns.datetime_last, t.tzid) as datetime_last
  , l.geom -- exposed for use in spatial queries
  , c.countries_id
FROM sensor_nodes l
JOIN timezones t ON (l.timezones_id = t.gid)
JOIN countries c ON (c.countries_id = l.countries_id)
JOIN entities oc ON (oc.entities_id = 1)
JOIN providers p ON (p.providers_id = l.providers_id)
JOIN nodes_instruments ni USING (sensor_nodes_id)
JOIN nodes_sensors ns USING (sensor_nodes_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS locations_view_cached AS
SELECT *
FROM locations_view;
CREATE INDEX ON locations_view_cached (id);
CREATE INDEX ON locations_view_cached USING GIST (geom);

CREATE OR REPLACE VIEW source_stats AS
WITH nodes AS (
  SELECT sensor_nodes_id
  , p.source_name
  , added_on
  FROM sensor_nodes n
  JOIN providers p ON (n.providers_id = p.providers_id)
), a AS (
  SELECT source_name
  , COUNT(1) as n
  FROM nodes
  WHERE added_on > now() - '1day'::interval
  GROUP BY source_name
), t AS (
  SELECT source_name
  , COUNT(1) as n
  FROM nodes
  GROUP BY source_name
), a2 AS (
  SELECT source_name
  , COUNT(DISTINCT y.sensor_nodes_id) FILTER (WHERE r.modified_on > now() - '1day'::interval) as n
  , MAX(r.modified_on) as last_modified
  FROM sensors_rollup r
  JOIN sensors s ON (r.sensors_id = s.sensors_id)
  JOIN sensor_systems y ON (s.sensor_systems_id = y.sensor_systems_id)
  JOIN nodes n ON (y.sensor_nodes_id = n.sensor_nodes_id)
  GROUP BY source_name)
SELECT t.source_name
, COALESCE(t.n, 0) as nodes_total
, COALESCE(a.n, 0) as nodes_added
, COALESCE(a2.n,0) as nodes_active
, a2.last_modified
FROM t
LEFT JOIN a ON (t.source_name = a.source_name)
LEFT JOIN a2 ON (t.source_name = a2.source_name);
