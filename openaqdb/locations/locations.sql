-- Materialized view used for the locations
-- currently using materialized view because the entire table is updated at once
-- and therefor there may not be much of a speed improvement to handle this
-- manually

-- first/last updated should not be queried directly

--DROP VIEW IF EXISTS locations_view CASCADE;

 CREATE OR REPLACE VIEW provider_licenses_view AS
	SELECT p.providers_id
	, json_agg(json_build_object(
	  'id', p.licenses_id
	, 'url', COALESCE(p.url, l.url)
	, 'description', COALESCE(p.notes, l.description)
	, 'date_from', lower(active_period)
	, 'date_to', upper(active_period)
	)) as licenses
	FROM providers_licenses p
	JOIN licenses l ON (l.licenses_id = p.licenses_id)
	GROUP BY providers_id;


CREATE OR REPLACE VIEW locations_view AS
-----------------------------
WITH nodes_instruments AS (
-----------------------------
  SELECT sn.sensor_nodes_id
  , bool_or(i.is_monitor) as is_monitor
  , json_agg(json_build_object(
    'id', i.instruments_id
    , 'name', i.label
    , 'manufacturer', jsonb_build_object(
      'id', i.manufacturer_entities_id
      , 'name', mc.full_name
      )
  )) as instruments
  , array_agg(DISTINCT mc.full_name) as manufacturers
  , array_agg(DISTINCT i.manufacturer_entities_id) as manufacturer_ids
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
    , array_agg(DISTINCT m.measurand) as parameters
    , array_agg(DISTINCT m.measurands_id) as parameter_ids
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
  , ni.is_monitor as ismonitor
  , l.city
  , jsonb_build_object(
      'id', c.countries_id
    , 'code', c.iso
    , 'name', c.name
    ) as country
  , jsonb_build_object(
      'id', oc.entities_id
    , 'name', oc.full_name
    , 'type', oc.entity_type
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
  , l.geom::geography as geog
  , c.countries_id
  , ns.parameters
  , ns.parameter_ids
  , oc.entity_type::text~*'research' as is_analysis
  , ni.manufacturers
  , ni.manufacturer_ids
	, pl.licenses
	, l.providers_id
FROM sensor_nodes l
JOIN timezones t ON (l.timezones_id = t.timezones_id)
JOIN countries c ON (c.countries_id = l.countries_id)
JOIN entities oc ON (oc.entities_id = l.owner_entities_id)
JOIN providers p ON (p.providers_id = l.providers_id)
JOIN nodes_instruments ni USING (sensor_nodes_id)
JOIN nodes_sensors ns USING (sensor_nodes_id)
LEFT JOIN provider_licenses_view pl ON (pl.providers_id = l.providers_id)
WHERE l.is_public;

DROP MATERIALIZED VIEW IF EXISTS locations_view_cached CASCADE;
CREATE MATERIALIZED VIEW locations_view_cached AS
SELECT *
FROM locations_view;
CREATE INDEX ON locations_view_cached (id);
-- old version of the index, might not be needed anymore
CREATE INDEX ON locations_view_cached USING GIST (geom);
-- newer version, required if we are going to use geographies in the st_dwithin
CREATE INDEX ON locations_view_cached USING GIST (geog);
CREATE INDEX ON locations_view_cached((datetime_last->>'utc') DESC NULLS LAST);
CREATE INDEX locations_view_cached_is_analysis ON locations_view_cached(is_analysis);
CREATE INDEX locations_view_cached_entity_onwer ON locations_view_cached((owner->>'type'));


--DROP VIEW IF EXISTS locations_manufacturers;
CREATE OR REPLACE VIEW locations_manufacturers AS
WITH locations AS (
 SELECT sn.sensor_nodes_id as id
  , jsonb_build_object(
     'modelName', i.label
     , 'manufacturerName', mc.full_name
  ) as manufacturer
  FROM sensor_nodes sn
  JOIN sensor_systems ss ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
  JOIN instruments i ON (ss.instruments_id = i.instruments_id)
  JOIN entities mc ON (mc.entities_id = i.manufacturer_entities_id)
	WHERE sn.is_public
  GROUP BY 1,2)
  SELECT id
  , jsonb_agg(manufacturer) as manufacturers
  FROM locations
  GROUP BY 1
  ;


DROP MATERIALIZED VIEW IF EXISTS locations_manufacturers_cached CASCADE;
CREATE MATERIALIZED VIEW locations_manufacturers_cached AS
SELECT *
FROM locations_manufacturers
ORDER BY id;
CREATE INDEX IF NOT EXISTS locations_manufacturers_id_idx
ON locations_manufacturers_cached (id);


CREATE OR REPLACE VIEW locations_latest_measurements AS
  SELECT sn.sensor_nodes_id as id
  , jsonb_agg(jsonb_build_object(
      'parameter', m.measurand
    , 'id', m.measurands_id
    , 'parameterId', m.measurands_id
    , 'unit', m.units
    , 'value', sl.value_latest
    , 'lastUpdated', sl.datetime_last
    , 'sourceName', sn.source_name
    , 'displayName', m.measurand||' '||COALESCE(m.units, 'n/a')
    , 'count', sl.value_count
    , 'average', sl.value_avg
    , 'lastValue', sl.value_latest
    , 'firstUpdated', sl.datetime_first
    , 'averagingPeriod', jsonb_build_object(
       'value', s.data_averaging_period_seconds
     , 'unit', 'seconds'
    ))) as measurements
   , jsonb_agg(jsonb_build_object(
        'id', m.measurands_id
       , 'parameter', m.measurand||' '||m.units
    ,   'count', sl.value_count
    )) as counts
    , array_agg(m.measurand) as parameters
    , SUM(sl.value_count) as total_count
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN sensors s USING (sensor_systems_id)
  JOIN sensors_rollup sl USING (sensors_id)
  JOIN measurands m USING (measurands_id)
  WHERE sn.is_public
  GROUP BY sensor_nodes_id;

DROP MATERIALIZED VIEW IF EXISTS locations_latest_measurements_cached CASCADE;
CREATE MATERIALIZED VIEW locations_latest_measurements_cached AS
SELECT *
FROM locations_latest_measurements
ORDER BY id;
CREATE INDEX IF NOT EXISTS locations_latest_measurements_id_idx ON locations_latest_measurements_cached (id);


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
