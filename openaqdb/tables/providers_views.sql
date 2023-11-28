DROP VIEW IF EXISTS providers_view CASCADE;
CREATE OR REPLACE VIEW providers_view AS
-----------------------------------
WITH providers_locations AS (
-----------------------------------
  SELECT providers_id
  , COUNT(1) as locations_count
  , COUNT(DISTINCT countries_id) as countries_count
  , st_extent(geom) as extent
  FROM sensor_nodes
  GROUP BY providers_id
-----------------------------------
), providers_parameters AS (
-----------------------------------
  SELECT sn.providers_id
  , s.measurands_id
  , MIN(sl.datetime_first) as datetime_first
  , MAX(sl.datetime_last) as datetime_last
  , SUM(value_count) as value_count
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN sensors s USING (sensor_systems_id)
  JOIN sensors_rollup sl USING (sensors_id)
  GROUP BY providers_id
  , measurands_id
-----------------------------------
), providers_rollup AS (
-----------------------------------
  SELECT cp.providers_id
  , MIN(cp.datetime_first) as datetime_first
  , MAX(cp.datetime_last) as datetime_last
  , SUM(value_count) as measurements_count
  , array_agg(jsonb_build_object(
        'id', m.measurands_id
        , 'name', m.measurand
        , 'units', m.units
        )
    ) as parameters
  FROM providers_parameters cp
  JOIN measurands m USING (measurands_id)
  JOIN providers_locations l USING (providers_id)
  GROUP BY cp.providers_id)
-----------------------------------
  SELECT providers_id as id
  , label as name
  , description
  , source_name
  , export_prefix
  , datetime_first
  , datetime_last
  , added_on as datetime_added
  , measurements_count
  , locations_count
  , countries_count
  , parameters
  , jsonb_build_object(
    'id', p.owner_entities_id
    , 'name', e.full_name
   ) as owner_entity
  , license
  , extent
  FROM providers p
  JOIN providers_rollup USING (providers_id)
  JOIN providers_locations USING (providers_id)
  JOIN entities e ON (p.owner_entities_id = e.entities_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS providers_view_cached AS
SELECT *
FROM providers_view;
CREATE INDEX ON providers_view_cached (id);
