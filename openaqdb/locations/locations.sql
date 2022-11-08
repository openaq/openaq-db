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
CREATE SEQUENCE IF NOT EXISTS sensors_latest_sq START 10;
CREATE TABLE IF NOT EXISTS sensors_latest (
  sensors_latest_id int PRIMARY KEY DEFAULT nextval('sensors_latest_sq')
  , sensors_id int NOT NULL REFERENCES sensors
  , value double precision NOT NULL
  , lat double precision -- so that nulls dont take up space
  , lon double precision
  , modified_on timestamptz
  , fetchlogs_id int -- for debugging issues, no reference constraint
  , UNIQUE(sensors_id)
);


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
), nodes_parameters AS (
-----------------------------
  SELECT sn.sensor_nodes_id
  , array_agg(jsonb_build_object(
    'id', m.measurands_id
    , 'name', m.measurand
    , 'units', m.units
    )) as parameters
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN sensors s USING (sensor_systems_id)
  JOIN measurands m USING (measurands_id)
  GROUP BY sensor_nodes_id)
  -----------------------------
SELECT
  l.sensor_nodes_id as id
  , site_name as name
  , 'UK' as sensor_type -- placeholder
  , l.ismobile
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
      'latitude', st_y(l.geom)
    , 'longitude', st_x(l.geom)
   ) as coordinates
  , ni.instruments
  , np.parameters
FROM sensor_nodes l
JOIN countries c ON (c.iso = l.country)
JOIN contacts oc ON (oc.contacts_id = 1)
JOIN providers p ON (p.providers_id = l.providers_id)
JOIN nodes_instruments ni USING (sensor_nodes_id)
JOIN nodes_parameters np USING (sensor_nodes_id);
