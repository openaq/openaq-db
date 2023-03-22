--BEGIN;

DROP TABLE IF EXISTS sensors_migrate
, sensor_systems_migrate
, sensor_nodes_migrate
, sensor_nodes_import
, sensor_nodes_map
, sensors_map;

\timing on

SET statement_timeout TO '8h';


DROP TABLE IF EXISTS sensor_nodes_tracking;
WITH history AS (
 SELECT sensor_nodes_id
 , created
 FROM sensor_nodes_history
 --LIMIT 10000
)
SELECT sensor_nodes_id
 , COUNT(*) as n
 , MIN(created) as added_on
 , MAX(created) as modified_on
 INTO sensor_nodes_tracking
 FROM history
 GROUP BY 1;

-- SELECT sensors_id
-- , COUNT(*) as n
-- , MIN(created) as added_on
-- , MAX(created) as modified_on
-- INTO sensors_tracking
-- FROM sensors_history
-- GROUP BY 1;

CREATE TABLE IF NOT EXISTS sensor_nodes_migrate (
    sensor_nodes_id int primary key,
    timezones_id int REFERENCES timezones(gid),
    ismobile boolean,
    geom geometry,
    site_name text,
    source_name text NOT NULL,
    source_id text NOT NULL,
    origin text,
    metadata jsonb,
    added_on timestamptz,
    modified_on timestamptz,
    modified_times int,
    UNIQUE(source_name, source_id)
);

CREATE TABLE IF NOT EXISTS sensor_systems_migrate (
    sensor_systems_id int primary key,
    sensor_nodes_id int not null REFERENCES sensor_nodes_migrate,
    source_id text,
    metadata jsonb,
    UNIQUE(source_id)
);

CREATE TABLE IF NOT EXISTS sensors_migrate (
    sensors_id int primary key,
    sensor_systems_id int NOT NULL REFERENCES sensor_systems_migrate,
    measurands_id int not null REFERENCES measurands,
    source_id text,
    data_averaging_period_seconds int,
    data_logging_period_seconds int,
    metadata jsonb,
    added_on timestamptz,
    modified_on timestamptz,
    modified_times int,
    UNIQUE(sensor_systems_id, measurands_id)
);

CREATE TABLE IF NOT EXISTS sensor_nodes_import (
    sensor_nodes_id int UNIQUE
  , source_id text
  , source_name text
  , n bigint
  , geom_var double precision
  , nodes int[]
  , UNIQUE(source_name, source_id)
);

CREATE TABLE IF NOT EXISTS sensor_nodes_map (
    sensor_nodes_id int
  , sensor_systems_id int
  , old_sensor_nodes_id int
  , UNIQUE(old_sensor_nodes_id)
);

CREATE TABLE IF NOT EXISTS sensors_map (
    sensors_id int
  , old_sensors_id int
  , sensor_nodes_id int
  , old_sensor_nodes_id int
  , UNIQUE(old_sensors_id)
);

CREATE OR REPLACE VIEW sensors_import AS
SELECT s.sensors_id
, ss.sensor_nodes_id
, m.measurands_id
, s.source_id as sensors_source_id
, m.measurand
, m.units
,  CASE
     WHEN s.metadata->>'data_averaging_period_seconds' IS NOT NULL THEN (s.metadata->>'data_averaging_period_seconds')::int
     WHEN origin ~* 'SENSTATE' THEN 60
     WHEN origin ~* 'CLARITY' THEN 90
     WHEN origin ~* 'PURPLE' THEN 120
     WHEN origin ~* 'HABITAT' THEN 1
     WHEN origin ~* 'CMU' THEN 3600
     ELSE 3600
   END as data_averaging_period_seconds
,  CASE
     WHEN origin ~* 'SENSTATE' THEN 60
     WHEN origin ~* 'CLARITY' THEN 180
     WHEN origin ~* 'PURPLE' THEN 120
     WHEN origin ~* 'HABITAT' THEN 1
     WHEN origin ~* 'CMU' THEN 3600
     ELSE 3600
   END as data_logging_period_seconds
, COALESCE(sn.source_id, regexp_replace(site_name, E'[|\\n\\r\\u2028]+', '', 'g' ), format('%s/%s', st_y(sn.geom), st_x(sn.geom))) as source_id
, sn.source_name
, regexp_replace(site_name, E'[|\\n\\r\\u2028]+', '', 'g' ) as site_name
, st_x(sn.geom) as lon
, st_y(sn.geom) as lat
, sn.metadata->>'timezone' as timezone
, sn.origin
, sn.ismobile
, added_on
, modified_on
, st.n as modified_times
FROM public.sensors s
JOIN public.measurands m USING(measurands_id)
JOIN public.sensor_systems ss USING (sensor_systems_id)
JOIN public.sensor_nodes sn USING (sensor_nodes_id)
LEFT JOIN public.sensors_tracking st ON (st.sensors_id = s.sensors_id);


-- group and add most of the locations
TRUNCATE TABLE sensor_nodes_import;
INSERT INTO sensor_nodes_import (
         source_id
       , source_name
       , sensor_nodes_id
       , n
       , geom_var
       , nodes
       )
SELECT source_id
, source_name
, MIN(sensor_nodes_id)
, COUNT(DISTINCT sensor_nodes_id)
, ROUND(stddev(lon)::numeric + stddev(lat)::numeric, 3)
, array_agg(DISTINCT sensor_nodes_id)
FROM sensors_import
WHERE NOT (source_name = 'Turkiye' AND site_name = 'TR') -- these are old imports
AND NOT (source_name = 'AirNow' AND site_name = 'National Housing Aut') -- these should not be grouped
GROUP BY 1,2;

-- Now add those air now locations
INSERT INTO sensor_nodes_import (
         source_id
       , source_name
       , sensor_nodes_id
       , n
       , geom_var
       , nodes
       )
SELECT source_id||'@'||lat
, source_name
, MIN(sensor_nodes_id)
, COUNT(DISTINCT sensor_nodes_id)
, ROUND(stddev(lon)::numeric + stddev(lat)::numeric, 3)
, array_agg(DISTINCT sensor_nodes_id)
FROM sensors_import
WHERE (source_name = 'AirNow' AND site_name = 'National Housing Aut') -- these should not be grouped
GROUP BY 1,2,lat,lon
ON CONFLICT DO NOTHING;

-- now we can use that list to add the sensors

INSERT INTO sensor_nodes_migrate (
  sensor_nodes_id
, source_name
, source_id
, site_name
, geom
, timezones_id
, origin
, ismobile
, metadata
, added_on
, modified_on
, modified_times
) OVERRIDING SYSTEM VALUE
WITH nodes AS (
     SELECT sensor_nodes_id
     , source_name
     , source_id
     , site_name
     , lat
     , lon
     , timezone
     , origin
     FROM sensors_import
     GROUP BY 1,2,3,4,5,6,7,8)
SELECT n.sensor_nodes_id
, n.source_name
, s.source_id
, n.site_name
, st_setsrid(st_makepoint(lon,lat), 4326)
, t.gid
, n.origin
, CASE WHEN lon IS NULL THEN true ELSE false END
, json_build_object('imported', 'production')
, st.added_on
, st.modified_on
, st.n
FROM nodes n
JOIN sensor_nodes_import s USING (sensor_nodes_id)
JOIN sensor_nodes_tracking st USING (sensor_nodes_id)
LEFT JOIN timezones t ON (n.timezone = t.tzid)
--LIMIT 10;
ON CONFLICT DO NOTHING;

-- create an instrument for each origin
-- INSERT INTO instruments (label, description, manufacturer_entities_id)
-- SELECT origin
-- , 'Added from imported sensor nodes'
-- , 1
-- FROM sensor_nodes
-- GROUP BY 1, 2
-- ON CONFLICT DO NOTHING;

-- Now create a new system for each of these
-- INSERT INTO sensor_systems_migrate (
--   sensor_nodes_id
-- , source_id
-- , instruments_id
-- , metadata)
-- SELECT sensor_nodes_id
-- , source_id
-- , instruments_
-- FROM sensor_nodes n
-- ON CONFLICT DO NOTHING;

-- about 30 duplicates
WITH systems AS (
  SELECT sensor_nodes_id
  , MIN(sensor_systems_id) as sensor_systems_id
  , COUNT(1) as n
  FROM sensor_systems y
  JOIN sensor_nodes_migrate n USING (sensor_nodes_id)
  GROUP BY 1)
INSERT INTO sensor_systems_migrate (
  sensor_systems_id
, sensor_nodes_id
, source_id
, metadata)
SELECT s.sensor_systems_id
, s.sensor_nodes_id
, source_id
, json_build_object('imported', 'production')
FROM systems s
JOIN sensor_systems USING (sensor_systems_id)
ON CONFLICT DO NOTHING;

-- create a map to use later
INSERT INTO sensor_nodes_map (
  sensor_nodes_id
, sensor_systems_id
, old_sensor_nodes_id)
SELECT sensor_nodes_id
, sensor_systems_id
, UNNEST(nodes)
FROM sensor_nodes_import
JOIN sensor_systems USING (sensor_nodes_id)
ON CONFLICT DO NOTHING;


-- now add the sensors
INSERT INTO sensors_migrate (
  sensors_id
, measurands_id
, sensor_systems_id
, source_id
, data_averaging_period_seconds
, data_logging_period_seconds
, metadata
, added_on
, modified_on
, modified_times
) OVERRIDING SYSTEM VALUE
WITH reduced AS (
  SELECT sy.sensor_systems_id
  , m.measurands_id
  , MAX(s.sensors_id) as sensors_id
  FROM sensors_import s
  JOIN sensor_systems_migrate sy ON (sy.sensor_nodes_id = s.sensor_nodes_id)
  JOIN measurands m ON (s.measurand = m.measurand AND s.units = m.units)
  GROUP BY 1,2
)
SELECT i.sensors_id
, r.measurands_id
, r.sensor_systems_id
, i.sensors_source_id
, i.data_averaging_period_seconds
, i.data_logging_period_seconds
, json_build_object('imported', 'production')
, i.added_on
, i.modified_on
, i.modified_times
FROM reduced r
JOIN sensors_import i ON (r.sensors_id = i.sensors_id)
--WHERE sensor_systems_id = 6654;
ON CONFLICT DO NOTHING;

-- check the nodes map
-- turkiye should be missing (~71)
SELECT i.sensor_nodes_id
, i.source_name
, i.source_id
, m.sensor_nodes_id
FROM sensors_import i
LEFT JOIN sensor_nodes_map m ON (i.sensor_nodes_id = m.old_sensor_nodes_id)
WHERE m.sensor_nodes_id IS NULL;

-- Now to make a sensors map for measurements import
INSERT INTO sensors_map (
 sensors_id
 , old_sensors_id
 , sensor_nodes_id
 , old_sensor_nodes_id
)
SELECT s.sensors_id
, i.sensors_id as old_sensors_id
, sm.sensor_nodes_id
, sm.old_sensor_nodes_id
-- , i.sensors_source_id
-- , i.source_name
-- , i.source_id
-- , i.measurand
-- , i.units
FROM sensors_import i
JOIN measurands m ON (i.measurand = m.measurand AND i.units = m.units)
JOIN sensor_nodes_map sm ON (sm.old_sensor_nodes_id = i.sensor_nodes_id)
JOIN sensor_systems_migrate sy ON (sy.sensor_nodes_id = sm.sensor_nodes_id)
JOIN sensors_migrate s ON (sy.sensor_systems_id = s.sensor_systems_id AND m.measurands_id = s.measurands_id)
--LEFT JOIN sensors s ON (s.sensors_id = i.sensors_id)
--WHERE sm.sensor_nodes_id != sm.old_sensor_nodes_id
--WHERE i.sensors_id = 852469
;

-- sensor summary
-- 7374
SELECT COUNT(DISTINCT sensors_id) as imported_sensors
, COUNT(DISTINCT old_sensors_id) as mapped_sensors
, COUNT(1) as records
, COUNT(DISTINCT old_sensors_id) - COUNT(DISTINCT sensors_id) as diff
FROM sensors_map;

-- double check
-- this should be zero
SELECT sensor_systems_id
, measurands_id
, COUNT(1) as n
FROM sensors_migrate
GROUP BY 1,2
HAVING COUNT(1) > 1
LIMIT 10;


\echo 'sensor_nodes_import'
SELECT COUNT(1) FROM sensor_nodes_import;
\echo 'sensor_nodes_migrate'
SELECT COUNT(1) FROM sensor_nodes_migrate;
\echo 'sensor_systems_migrate'
SELECT COUNT(1) FROM sensor_systems_migrate;
\echo 'sensors_migrate'
SELECT COUNT(1) FROM sensors_migrate;
\echo 'sensor_nodes_map'
SELECT COUNT(1) FROM sensor_nodes_map;
\echo 'sensors_map'
SELECT COUNT(1) FROM sensors_map;
\echo 'sensors_import'
SELECT COUNT(1) FROM sensors_import;


WITH reduced AS (
  SELECT sy.sensor_systems_id
  , m.measurands_id
  , MAX(s.sensors_id) as sensors_id
  FROM sensors_import s
  JOIN sensor_systems_migrate sy ON (sy.sensor_nodes_id = s.sensor_nodes_id)
  JOIN measurands m ON (s.measurand = m.measurand AND s.units = m.units)
  GROUP BY 1,2
)
SELECT COUNT(1)
FROM reduced;

-- these data should be able to be imported directly

-- CREATE OR REPLACE VIEW measurements_migrate AS
-- SELECT m.sensors_id
-- , i.datetime
-- , MAX(i.value) as value
-- , i.lat
-- , i.lon
-- FROM public.measurements i
-- JOIN public.sensors_map m ON (i.sensors_id = m.old_sensors_id)
-- GROUP BY 1,2,4,5;


-- INSERT INTO measurements (sensors_id, datetime, value, lat, lon)
-- SELECT m.sensors_id
-- , i.datetime
-- , i.value
-- , i.lat
-- , i.lon
-- FROM public.measurements_import i
-- JOIN public.sensors_map m ON (i.sensors_id = m.old_sensors_id)
-- ON CONFLICT DO NOTHING;




-- -- after inserting manually we need to update the sequences
-- SELECT setval(
--  pg_get_serial_sequence('sensor_nodes', 'sensor_nodes_id'),
--  (SELECT MAX(sensor_nodes_id) FROM public.sensor_nodes)
-- );

-- SELECT setval(
--  pg_get_serial_sequence('sensor_systems', 'sensor_systems_id'),
--  (SELECT MAX(sensor_systems_id) FROM public.sensor_systems)
-- );

-- SELECT setval(
--  pg_get_serial_sequence('sensors', 'sensors_id'),
--  (SELECT MAX(sensors_id) FROM public.sensors)
-- );


--COMMIT;
