CREATE SCHEMA IF NOT EXISTS testing;
SET search_path = testing, public;

--SET TIMEZONE TO 'US/Pacific';
SET TIMEZONE TO 'UTC';

-- A function to make it easier to generate sets of fake data
CREATE OR REPLACE FUNCTION generate_fake_data(
       stations int    -- The number of locations to create
       , origin text   --
       , averaging interval = '1hour'
       , period interval = '7days'
       ) RETURNS int AS $$
DECLARE
n int;
BEGIN
-- Create some test stations
INSERT INTO sensor_nodes (site_name, source_name, source_id, origin, metadata)
WITH stations AS (
SELECT generate_series(1,stations,1) as id)
SELECT 'Station #'||id||' (tz:'||((MOD(id, 24)+1)-12)::text||')'
, origin||'-'||id
, origin
, origin
, jsonb_build_object(
   'utc_offset', to_char((MOD(id, 24)+1)-12, 'S00')||':00:00',
   'testing', true
   )
FROM stations
ON CONFLICT (source_name, source_id) DO UPDATE
SET metadata = EXCLUDED.metadata;
-- add the sensor systems
INSERT INTO sensor_systems (sensor_nodes_id, source_id)
SELECT sensor_nodes_id, source_id
FROM sensor_nodes sn
WHERE sn.origin = $2
ON CONFLICT DO NOTHING;
-- Now add a sensor for each station / measurand id
INSERT INTO sensors (sensor_systems_id, measurands_id, source_id)
SELECT s.sensor_systems_id
, m.measurands_id
, n.source_id||'-'||m.measurand
FROM sensor_systems s
JOIN sensor_nodes n ON (s.sensor_nodes_id = n.sensor_nodes_id)
JOIN measurands m ON (TRUE AND m.is_core)
WHERE n.origin = $2
ON CONFLICT DO NOTHING;
-- Add some test data
INSERT INTO measurements (datetime, sensors_id, value)
WITH dates AS (
SELECT generate_series(current_date - period::interval, current_date, averaging::interval) as datetime)
SELECT datetime
, sensors_id
, measurands_id as value -- keep this simple for testing purposes
FROM dates d
JOIN sensors s ON (TRUE)
JOIN sensor_systems ss USING (sensor_systems_id)
JOIN sensor_nodes sn USING (sensor_nodes_id)
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET value = EXCLUDED.value;
-- now count up how many we created to return that
SELECT COUNT(1) INTO n FROM public.measurements;
RETURN n;
END;
$$ LANGUAGE plpgsql;

-- function for creating the canary data
-- the canary data is used to test how the timezones are affecting dates
CREATE OR REPLACE FUNCTION generate_canary_data(
        origin text   --
       , averaging interval = '1hour'
       , period interval = '7days'
       ) RETURNS int AS $$
DECLARE
mid int;
n int;
BEGIN
-- now for this one we want to use the `date canary` sensor
-- create a new measurand
INSERT INTO measurands (measurand, units, display, description, is_core) VALUES
('DOM', 'Day', 'Day of month', 'Local day of month for measurement to use for testing', false)
ON CONFLICT (measurand, units) DO UPDATE
SET description = EXCLUDED.description
RETURNING measurands_id INTO mid;
-- Now create a sensor for each node
INSERT INTO sensors (sensor_systems_id, measurands_id, source_id)
SELECT s.sensor_systems_id
, m.measurands_id
, n.source_id||'-'||m.measurand
FROM sensor_systems s
JOIN sensor_nodes n ON (s.sensor_nodes_id = n.sensor_nodes_id)
JOIN measurands m ON (m.measurands_id = mid)
WHERE n.origin = $1
ON CONFLICT DO NOTHING;
-- and now create measurements for just those sensors
-- and use the local day of the month as the value
INSERT INTO measurements (datetime, sensors_id, value)
WITH dates AS (
     SELECT generate_series(current_date - period, current_date + averaging, averaging)::timestamp as datetime
), datestz AS (
   SELECT datetime::text||(sn.metadata->>'utc_offset') as datetimetz
   , datetime
   , (sn.metadata->>'utc_offset') as utc_offset
   , s.sensors_id
FROM dates d
JOIN sensors s ON (TRUE)
JOIN sensor_systems ss USING (sensor_systems_id)
JOIN sensor_nodes sn USING (sensor_nodes_id)
WHERE s.measurands_id = mid)
SELECT datetimetz::timestamptz as utc_datetime
--, datetimetz
--, datetime as local_datetime
--, utc_offset
, sensors_id
, date_part('day', datetime - '1sec'::interval) as value
FROM datestz
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET value = EXCLUDED.value;
-- get the number of entries
SELECT COUNT(1) INTO n
FROM public.measurements m
JOIN sensors s ON (s.sensors_id = m.sensors_id)
WHERE s.measurands_id = mid;
RETURN n;
END;
$$ LANGUAGE plpgsql;

-- version data function to create versions of the existing data
CREATE OR REPLACE FUNCTION generate_version_data(
       sensors int    -- The number of locations to create
       , origin text   --
       , averaging interval = '1hour'
       , period interval = '7days'
       ) RETURNS int AS $$
DECLARE
mid int;
n int;
BEGIN
-- now take some of those and make some versions
-- make sure we add the version to the metadata
INSERT INTO sensors (sensor_systems_id, source_id, measurands_id, metadata)
WITH v as (
SELECT '2021-12-01'::date as version_date
, 2 as df
), s as (
SELECT sensor_systems_id, sensors_id, measurands_id, source_id
FROM sensors
WHERE source_id ~* $2
LIMIT $1)
SELECT sensor_systems_id
, source_id||'-'||v.version_date::text as source_id
, measurands_id
, jsonb_build_object('parent_sensors_id', sensors_id, 'version_date', v.version_date, 'df', v.df)
FROM s, v
ON CONFLICT (sensor_systems_id, measurands_id, source_id) DO UPDATE
SET metadata = EXCLUDED.metadata;
-- Now add some adjusted data
INSERT INTO measurements (sensors_id, datetime, value)
SELECT s.sensors_id
, m.datetime
, m.value * (s.metadata->>'df')::int
FROM sensors s
JOIN measurements m ON ((s.metadata->>'parent_sensors_id')::int = m.sensors_id)
WHERE metadata->>'parent_sensors_id' IS NOT NULL
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET value = EXCLUDED.value;
-- Now add the verions
INSERT INTO versions (sensors_id, parent_sensors_id, version_date, life_cycles_id, readme)
SELECT s.sensors_id
, (s.metadata->>'parent_sensors_id')::int as parent_sensors_id
, (s.metadata->>'version_date')::date as version_date
, 3 as life_cycles_id
, 'Added as part of testing'
FROM sensors s
WHERE metadata->>'parent_sensors_id' IS NOT NULL
ON CONFLICT DO NOTHING;
RETURN 1;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS remove_testing_data();
CREATE OR REPLACE FUNCTION remove_testing_data() RETURNS int AS $$
DECLARE
n int;
BEGIN
WITH deleted AS (
 DELETE
 FROM measurements m
 WHERE sensors_id IN (
   SELECT sensors_id
   FROM sensors s
   JOIN sensor_systems ss ON (ss.sensor_systems_id = s.sensor_systems_id)
   JOIN sensor_nodes sn ON (sn.sensor_nodes_id = ss.sensor_nodes_id)
   WHERE (sn.metadata->'testing')::boolean = true
 ) RETURNING 1) SELECT COUNT(1) INTO n FROM deleted;
DELETE
FROM rollups
WHERE sensors_id IN (
  SELECT sensors_id
  FROM sensors s
  JOIN sensor_systems ss ON (ss.sensor_systems_id = s.sensor_systems_id)
  JOIN sensor_nodes sn ON (sn.sensor_nodes_id = ss.sensor_nodes_id)
  WHERE (sn.metadata->'testing')::boolean = true
);
DELETE
FROM sensors
WHERE sensors_id IN (
  SELECT sensors_id
  FROM sensors s
  JOIN sensor_systems ss ON (ss.sensor_systems_id = s.sensor_systems_id)
  JOIN sensor_nodes sn ON (sn.sensor_nodes_id = ss.sensor_nodes_id)
  WHERE (sn.metadata->'testing')::boolean = true
);
DELETE
FROM sensor_systems
WHERE sensor_systems_id IN (
  SELECT sensor_systems_id
  FROM sensor_systems ss
  JOIN sensor_nodes sn ON (sn.sensor_nodes_id = ss.sensor_nodes_id)
  WHERE (sn.metadata->'testing')::boolean = true
);
DELETE
FROM sensor_nodes_sources
WHERE sensor_nodes_id IN (
  SELECT sensor_nodes_id
  FROM sensor_nodes sn
  WHERE (sn.metadata->'testing')::boolean = true
);
DELETE
FROM sensor_nodes
WHERE (metadata->'testing')::boolean = true;
DELETE
FROM measurands
WHERE measurand = 'DOM';
RETURN n;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE VIEW canary_rollup_days AS
SELECT g.subtitle
, rollup
, first_datetime
, last_datetime
, value_count
, value_sum/value_count as value_avg
, last_value
FROM rollups r
JOIN groups g ON (r.groups_id = g.groups_id)
WHERE rollup = 'day'
AND g.type = 'node'
AND measurands_id IN (
  SELECT measurands_id
  FROM measurands
  WHERE measurand = 'DOM')
ORDER BY first_datetime;

CREATE OR REPLACE VIEW canary_days_utc AS
WITH meas AS (
SELECT sn.site_name
, m.datetime - '1sec'::interval as datetime
, m.value
, sn.metadata->>'utc_offset' as utc_offset
FROM measurements m
JOIN sensors s ON (s.sensors_id = m.sensors_id)
JOIN sensor_systems ss ON (ss.sensor_systems_id = s.sensor_systems_id)
JOIN sensor_nodes sn ON (sn.sensor_nodes_id = ss.sensor_nodes_id)
WHERE measurands_id IN (
  SELECT measurands_id
  FROM measurands
  WHERE measurand = 'DOM'))
SELECT site_name
, date_trunc('day', datetime) as day
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, COUNT(value) as value_count
, AVG(value) as value_avg
FROM meas
GROUP BY date_trunc('day', datetime)
, site_name
, utc_offset
ORDER BY utc_offset;

CREATE OR REPLACE VIEW canary_days_local AS
WITH meas AS (
SELECT sn.site_name
, (m.datetime - '1sec'::interval + (sn.metadata->>'utc_offset')::interval)::timestamp as datetime
--, (m.datetime AT TIME ZONE (sn.metadata->>'utc_offset')) - '1sec'::interval as datetime
, m.value
, sn.metadata->>'utc_offset' as utc_offset
FROM measurements m
JOIN sensors s ON (s.sensors_id = m.sensors_id)
JOIN sensor_systems ss ON (ss.sensor_systems_id = s.sensor_systems_id)
JOIN sensor_nodes sn ON (sn.sensor_nodes_id = ss.sensor_nodes_id)
WHERE measurands_id IN (
  SELECT measurands_id
  FROM measurands
  WHERE measurand = 'DOM'))
SELECT site_name
, date_trunc('day', datetime) as day
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, COUNT(value) as value_count
, AVG(value) as value_avg
FROM meas
GROUP BY date_trunc('day', datetime)
, site_name
, utc_offset
ORDER BY utc_offset
, date_trunc('day', datetime);

CREATE OR REPLACE VIEW canary_days_compare AS
SELECT sn.site_name
, m.datetime
, (m.datetime AT TIME ZONE (sn.metadata->>'utc_offset')) as local_datetime_offset
, (m.datetime AT TIME ZONE 'US/Pacific') as local_datetime_tz
, (m.datetime + (sn.metadata->>'utc_offset')::interval)::timestamp as local_datetime_interval
, m.value
, sn.metadata->>'utc_offset' as utc_offset
FROM measurements m
JOIN sensors s ON (s.sensors_id = m.sensors_id)
JOIN sensor_systems ss ON (ss.sensor_systems_id = s.sensor_systems_id)
JOIN sensor_nodes sn ON (sn.sensor_nodes_id = ss.sensor_nodes_id)
WHERE measurands_id IN (
  SELECT measurands_id
  FROM measurands
  WHERE measurand = 'DOM');
