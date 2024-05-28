CREATE OR REPLACE FUNCTION get_countries_id(g geography)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT countries_id from countries WHERE st_intersects(g, geog) LIMIT 1;
$$;
CREATE OR REPLACE FUNCTION get_countries_id(g geometry)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT countries_id from countries WHERE st_intersects(g::geography, geog) LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION country(g geography)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT iso from countries WHERE st_intersects(g, geog) LIMIT 1;
$$;
CREATE OR REPLACE FUNCTION country(g geometry)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT iso from countries WHERE st_intersects(g::geography, geog) LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION countries(nodes int[])
RETURNS text[] AS
$$
WITH t AS (
        SELECT DISTINCT country
        FROM sensor_nodes WHERE
        sensor_nodes_id = ANY(nodes)
) SELECT array_agg(country) FROM t;
$$ LANGUAGE SQL;

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
	WHERE sn.is_public AND s.is_public
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
	, array_agg(DISTINCT m.measurands_id) AS parameter_ids
  FROM countries_parameters cp
  JOIN measurands m USING (measurands_id)
  JOIN countries_locations l USING (countries_id)
  GROUP BY cp.countries_id)
-----------------------------------
  SELECT countries_id as id
  , name
  , iso as code
  , cr.datetime_first
  , cr.datetime_last
  , cr.measurements_count
  , cl.locations_count
  , cl.providers_count
  , cr.parameters
  , cr.parameter_ids
  FROM countries
  JOIN countries_rollup cr USING (countries_id)
  JOIN countries_locations cl USING (countries_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS countries_view_cached AS
SELECT *
FROM countries_view;
CREATE INDEX ON countries_view_cached (id);
