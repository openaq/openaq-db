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
	WHERE is_public
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
  WHERE sn.is_public AND s.is_public
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
	, array_agg(DISTINCT m.measurands_id) AS parameter_ids
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
  , pr.datetime_first
  , pr.datetime_last
  , added_on as datetime_added
  , pr.measurements_count
  , pl.locations_count
  , pl.countries_count
  , pr.parameters
  , pr.parameter_ids
  , jsonb_build_object(
    'id', p.owner_entities_id
    , 'name', e.full_name
   ) as owner_entity
  , license
  , extent
  FROM providers p
  JOIN providers_rollup pr USING (providers_id)
  JOIN providers_locations pl USING (providers_id)
  JOIN entities e ON (p.owner_entities_id = e.entities_id)
  WHERE p.is_public
  ORDER BY lower(p.label) ASC;


CREATE MATERIALIZED VIEW IF NOT EXISTS providers_view_cached AS
SELECT *
FROM providers_view;
CREATE INDEX ON providers_view_cached (id);
