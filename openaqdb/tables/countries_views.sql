DROP VIEW IF EXISTS countries_view CASCADE;
CREATE OR REPLACE VIEW countries_view AS
-----------------------------------
WITH countries_locations AS (
-----------------------------------
  SELECT countries_id
  , COUNT(1) as locations_count
  , COUNT(DISTINCT providers_id) as providers_count
  FROM sensor_nodes
  GROUP BY countries_id
-----------------------------------
), countries_parameters AS (
-----------------------------------
  SELECT sn.countries_id
  , s.measurands_id
  , MIN(sl.datetime_first) as datetime_first
  , MAX(sl.datetime_last) as datetime_last
  , SUM(value_count) as value_count
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN sensors s USING (sensor_systems_id)
  JOIN sensors_rollup sl USING (sensors_id)
  GROUP BY countries_id, measurands_id
-----------------------------------
), countries_rollup AS (
-----------------------------------
  SELECT cp.countries_id
  , MIN(cp.datetime_first) as datetime_first
  , MAX(cp.datetime_last) as datetime_last
  , SUM(value_count) as measurements_count
  , array_agg(jsonb_build_object(
        'id', m.measurands_id
        , 'name', m.measurand
        , 'units', m.units
        )
    ) as parameters
  FROM countries_parameters cp
  JOIN measurands m USING (measurands_id)
  JOIN countries_locations l USING (countries_id)
  GROUP BY cp.countries_id)
-----------------------------------
  SELECT countries_id as id
  , name
  , iso as code
  , datetime_first
  , datetime_last
  , measurements_count
  , locations_count
  , providers_count
  , parameters
  --, geojson(geom) as geojson
  FROM countries
  JOIN countries_rollup USING (countries_id)
  JOIN countries_locations USING (countries_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS countries_view_cached AS
SELECT *
FROM countries_view;
CREATE INDEX ON countries_view_cached (id);
