BEGIN;
SET search_path = public;

-- A method to adjust the time for query purposes
-- right now the indexing on the measurements table is based off of the
-- timestamp and does not handle the local time or date very well
-- and so this is a work around
CREATE OR REPLACE FUNCTION utc_offset(tz text) RETURNS interval AS $$
SELECT timezone(tz, now()) - timezone('UTC', now());
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- A view to pull the data from. This can be modified as needed
-- but will need to be structured the same way
-- the current setup
DROP VIEW IF EXISTS measurement_data_export;
CREATE OR REPLACE VIEW measurement_data_export AS
SELECT s.sensors_id
, sn.sensor_nodes_id
, sn.site_name||'-'||ss.sensor_systems_id as location
, p.measurands_id
, CASE WHEN sn.ismobile
    THEN 'mobile'
    ELSE COALESCE(LOWER(sn.country), 'no-country')
    END as country
, sn.ismobile
, s.source_id as sensor
-- utc time for use in query
, datetime
, utc_offset(sn.metadata->>'timezone') as utc_offset
-- local time with tz for exporting
, format_timestamp(m.datetime, sn.metadata->>'timezone') as datetime_str
, p.measurand
, p.units
, m.value
, CASE WHEN sn.ismobile
    THEN lon
    ELSE st_x(geom)
    END as lon
, CASE WHEN sn.ismobile
    THEN lon
    ELSE st_y(geom)
    END as lat
FROM measurements m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN measurands p ON (s.measurands_id = p.measurands_id)
JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
WHERE sn.metadata->'timezone' IS NOT NULL
-- once we have versioning we can uncomment this line
--AND s.sensors_id NOT IN (SELECT sensors_id FROM versions)
;


-- A function to query the database and then create or reset
-- the export logs. This takes a long time to run and therefor
-- should not be run that often. If it needs to be run more often
-- we may want to make it use one of the rollup tables to generate it
-- ** NOTES **
-- Turning on/off the logs does not speed this up much at all
-- dropping and adding the indexes doesnt either
-- inserting over the existing data with `on conflict` clause
-- is also just about as fast as truncating and inserting
-- this is all based on the current openaq database setup
DROP FUNCTION IF EXISTS reset_export_logs();
CREATE OR REPLACE FUNCTION reset_export_logs() RETURNS TABLE(
 sensor_nodes_id int
 , first_day date
 , last_day date
 , days int
 , records int
 , measurands int
 ) AS $$
WITH inserts AS (
  INSERT INTO public.open_data_export_logs (sensor_nodes_id, day, records, measurands)
  SELECT m.sensor_nodes_id
  , (m.datetime - '1sec'::interval)::date as day
  , COUNT(m.value) as records
  , COUNT(DISTINCT m.measurands_id) as measurands
  FROM public.measurement_data_export m
  GROUP BY m.sensor_nodes_id, (m.datetime-'1sec'::interval)::date
  ON CONFLICT(sensor_nodes_id, day) DO UPDATE
  SET modified_on = now(), exported_on = null, queued_on = null
  RETURNING sensor_nodes_id, day, records, measurands)
  SELECT sensor_nodes_id
  , MIN(day) as first_day
  , MAX(day) as last_day
  , COUNT(day)::int as days
  , SUM(records)::int as records
  , MAX(measurands)::int as measurands
  FROM inserts
  GROUP BY sensor_nodes_id;
$$ LANGUAGE SQL;


-- A list of modiified location days without paying attention to the timzone
DROP VIEW IF EXISTS modified_location_days;
CREATE OR REPLACE VIEW modified_location_days AS
SELECT l.sensor_nodes_id
, day
, records
, measurands
, modified_on
--, queued_on
--, exported_on
, age(day + '1day'::interval, (now() AT TIME ZONE (sn.metadata->>'timezone')::text)) as wait_interval
FROM public.open_data_export_logs l
JOIN public.sensor_nodes sn ON (l.sensor_nodes_id = sn.sensor_nodes_id)
WHERE (queued_on IS NULL OR modified_on > queued_on);


-- The view that is used in the pending function
-- this allows us to look at the pending list without updating it
CREATE OR REPLACE VIEW pending_location_days AS
SELECT l.sensor_nodes_id
, day
, records
, measurands
, modified_on
, queued_on
, exported_on
, utc_offset(sn.metadata->>'timezone') as utc_offset
FROM public.open_data_export_logs l
JOIN public.sensor_nodes sn ON (l.sensor_nodes_id = sn.sensor_nodes_id)
WHERE (queued_on IS NULL OR modified_on > queued_on)
-- wait until the day is done in that timezone to export data
AND day < (now() AT TIME ZONE (sn.metadata->>'timezone')::text)::date;


-- A function to use to get a list of days that need to be exported
-- the method will also mark the entries as queued so we dont fetch
-- them again under a different process
-- Also includes a rate limiter so we dont attempt to much at once
DROP FUNCTION IF EXISTS get_pending(int);
CREATE OR REPLACE FUNCTION get_pending(lmt int = 100) RETURNS TABLE(
   sensor_nodes_id int
 , day date
 , records int
 , measurands int
 , modified_on timestamptz
 , queued_on timestamptz
 , exported_on timestamptz
 , utc_offset interval
 ) AS $$
WITH pending AS (
  SELECT *
  FROM pending_location_days
  LIMIT lmt)
UPDATE public.open_data_export_logs
SET queued_on = now()
FROM pending
WHERE pending.day = open_data_export_logs.day
AND pending.sensor_nodes_id = open_data_export_logs.sensor_nodes_id
RETURNING pending.*;
$$ LANGUAGE SQL;

-- used to make the entry as finished
CREATE OR REPLACE FUNCTION update_export_log_exported(dy date, id int, n int) RETURNS interval AS $$
UPDATE public.open_data_export_logs
SET exported_on = now()
, records = n
WHERE day = dy AND sensor_nodes_id = id
RETURNING exported_on - queued_on;
$$ LANGUAGE SQL;

-- used by the ingester to mark the record as having been modified
-- which will set it up to be exported
CREATE OR REPLACE FUNCTION update_export_log_modified(dy date, id int) RETURNS interval AS $$
UPDATE public.open_data_export_logs
SET modified_on = now()
WHERE day = dy AND sensor_nodes_id = id
RETURNING exported_on - queued_on;
$$ LANGUAGE SQL;

COMMIT;
