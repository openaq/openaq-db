SET search_path = public;

-- A view to pull the data from. This can be modified as needed
-- but will need to be structured the same way
-- the current setup
DROP VIEW IF EXISTS measurement_data_export;
CREATE OR REPLACE VIEW measurement_data_export AS
SELECT sn.site_name||'-'||ss.sensor_systems_id as location
, sn.sensor_nodes_id
, p.measurands_id
, CASE WHEN sn.ismobile
    THEN 'mobile'
    ELSE COALESCE(LOWER(sn.country), 'no-country')
    END as country
, sn.ismobile
, s.source_id as sensor
, m.datetime
, p.measurand||'-'||p.units as measurand
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
-- once we have versioning we can uncomment this line
--WHERE s.sensors_id NOT IN (SELECT sensors_id FROM versions)
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
  , COUNT(day) as days
  , SUM(records) as records
  , MAX(measurands) as measurands
  FROM inserts
  GROUP BY sensor_nodes_id;
$$ LANGUAGE SQL;

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
 ) AS $$
WITH pending AS (
  SELECT sensor_nodes_id
  , day
  , records
  , measurands
  , modified_on
  , queued_on
  , exported_on
  FROM public.open_data_export_logs
  WHERE queued_on IS NULL
  OR modified_on > queued_on
  LIMIT lmt)
UPDATE public.open_data_export_logs
SET queued_on = now()
FROM pending
WHERE pending.day = open_data_export_logs.day
AND pending.sensor_nodes_id = open_data_export_logs.sensor_nodes_id
RETURNING pending.*;
$$ LANGUAGE SQL;

-- used to make the entry as finished
CREATE OR REPLACE FUNCTION update_export_log_exported(dy date, id int) RETURNS interval AS $$
UPDATE public.open_data_export_logs
SET exported_on = now()
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
