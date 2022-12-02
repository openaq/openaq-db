-- Materialized view used for the locations
-- currently using materialized view because the entire table is updated at once
-- and therefor there may not be much of a speed improvement to handle this
-- manually

-- first/last updated should not be queried directly

-- Sensors rollups will store the summary for the sensors
-- entire lifespan
CREATE SEQUENCE IF NOT EXISTS sensors_rollup_sq START 10;
CREATE TABLE IF NOT EXISTS sensors_rollup (
  sensor_rollup_id int PRIMARY KEY DEFAULT nextval('sensors_rollup_sq')
  , sensors_id int NOT NULL REFERENCES sensors
  , last_added_on timestamptz
  , first_added_on timestamptz
  , last_datetime timestamptz
  , first_datetime timestamptz
  , value_count int NOT NULL
  , value_avg double precision
  , value_sd double precision
  , value_min double precision
  , value_max double precision
  , value_p05 double precision
  , value_p50 double precision
  , value_p95 double precision
  , modified_on timestamptz
  , UNIQUE(sensors_id)
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
  , array_agg(json_build_object(
    'id', i.instruments_id
    , 'name', i.label
    , 'manufacturer', jsonb_build_object(
      'id', i.manufacturer_contacts_id
      , 'name', mc.full_name
      )
  )) as instruments
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN instruments i USING (instruments_id)
  JOIN contacts mc ON (mc.contacts_id = i.manufacturer_contacts_id)
  GROUP BY sn.sensor_nodes_id
  -----------------------------
), nodes_sensors AS (
-----------------------------
  SELECT sn.sensor_nodes_id
  , MIN(sl.datetime) as datetime_first
  , MAX(sl.datetime) as datetime_last -- need to change
  , array_agg(jsonb_build_object(
    'id', s.sensors_id
    , 'name', m.measurand||' '||m.units
    , 'parameter', jsonb_build_object(
        'id', m.measurands_id
        , 'name', m.measurand
        , 'units', m.units
        )
    )) as sensors
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN sensors s USING (sensor_systems_id)
  JOIN sensors_latest sl USING (sensors_id)
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
  , l.origin = 'OPENAQ' as ismonitor
  , l.city
  , jsonb_build_object(
      'id', null
    , 'code', c.iso
    , 'name', c.name
    ) as country
  , jsonb_build_object(
      'id', oc.contacts_id
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
FROM sensor_nodes l
JOIN timezones t ON (l.timezones_id = t.gid)
JOIN countries c ON (c.iso = l.country)
JOIN contacts oc ON (oc.contacts_id = 1)
JOIN providers p ON (p.providers_id = l.providers_id)
JOIN nodes_instruments ni USING (sensor_nodes_id)
JOIN nodes_sensors ns USING (sensor_nodes_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS locations_view_m AS
SELECT *
FROM locations_view;
CREATE INDEX ON locations_view_m (id);
CREATE INDEX ON locations_view_m USING GIST (geom);
