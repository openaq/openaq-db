	-- The purpose of this set of tables and methods
	-- is to keep daily summaries for each sensor along with
	-- a table to track stats for each day. One of the challenges here is to
	-- deal with the timezones across the different sensor nodes.

	-- Notes:
	-- * Each record is a day in that timezone.
	-- * Days are always stored time begining
	-- * The first and last hours are in UTC and stored time ending (like the hourly data)
	-- * Unlike the hourly_data, the daily_stats table is not updated first
	-- and then used as a queue for the filling/updating of the daily_data table.
	-- Instead we calculate the current day every hour, we do the whole day even though
	-- the day will only be complete in 1-2 timezones at a time. And after we run the
	-- daily_data method we update the daily_stats table.
	-- * We also have an update method that will update any day that may have changed
	-- after first being calculated or was never run in the first place.
	-- * This set of methods current runs pretty independently from the hourly_methods


	-- Daily stats will store data for each day, across all sensors
	-- It should only be populated AFTER the daily data is summarized for a given day
	DROP TABLE IF EXISTS daily_stats;
	CREATE TABLE IF NOT EXISTS daily_stats (
   datetime date PRIMARY KEY
 , added_on timestamptz NOT NULL DEFAULT now()
 , modified_on timestamptz 											-- last time the hourly data was modified
 , calculated_count int NOT NULL DEFAULT 0
 , updated_on timestamptz												--
 , calculated_on timestamptz
 , sensor_nodes_count int
 , measurements_count int
 , measurements_raw_count int
 , sensors_count int
 );

-- The daily data will be similar to the hourly data and include
-- summary data for that day in the appropriate timezone
CREATE TABLE IF NOT EXISTS daily_data (
  sensors_id int NOT NULL --REFERENCES sensors ON DELETE CASCADE
, datetime date NOT NULL -- keeping the name datetime makes dynamic queries easier
, first_datetime timestamptz NOT NULL
, last_datetime timestamptz NOT NULL
, value_count int NOT NULL
, value_avg double precision
, value_sd double precision
, value_min double precision
, value_max double precision
, value_p02 double precision
, value_p25 double precision
, value_p50 double precision
, value_p75 double precision
, value_p98 double precision
, value_raw_avg double precision
, value_raw_count double precision
, value_raw_min double precision
, value_raw_max double precision
, error_count int
, error_raw_count int
, updated_on timestamptz -- last time the sensor data was updated
, calculated_on timestamptz-- last time the row rollup was calculated
, calculated_count int DEFAULT 1
, UNIQUE(sensors_id, datetime)
)

--ALTER TABLE daily_data
--  ADD COLUMN error_count int NOT NULL DEFAULT 0
--, ADD COLUMN error_raw_count int NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS daily_data_sensors_id_idx
ON daily_data
USING btree (sensors_id);

CREATE INDEX IF NOT EXISTS daily_data_day_idx
ON daily_data
USING btree (datetime);

-- This can be used to check our method without writing anything
DROP FUNCTION IF EXISTS daily_data_check(sd date, ed date, sids int[]);
CREATE OR REPLACE FUNCTION daily_data_check(sd date, ed date, sids int[]) RETURNS TABLE (
	sensors_id int
	, sensor_nodes_id int
	, datetime date
	, utc_offset interval
	, datetime_min timestamptz
	, daettime_max timestamptz
	, value_count bigint
	, value_raw_count bigint
	, value_avg double precision
	, value_raw_avg double precision
	) AS $$
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_date(m.datetime, t.tzid) as datetime
, utc_offset(t.tzid) as utc_offset
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, COUNT(1) AS value_count
, SUM(value_count) as value_raw_count
, AVG(value_avg) as value_avg
, SUM(value_avg*value_count)/SUM(value_count) as value_raw_avg
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE value_count > 0
AND datetime > as_utc(sd, t.tzid)
AND datetime <= as_utc(ed, t.tzid)
AND m.sensors_id = ANY(sids)
GROUP BY 1,2,3,4
HAVING COUNT(1) > 0;
$$ LANGUAGE SQL;


-- This is the primary method for getting daily data
	-- It will only do one offset at a time which would be 1-2 timezones at a time
	-- this is because doing an entire day in one method is too much data to aggregate efficiently
	-- see the next method for a way to run a whole day
	-- NOTE: this method does not update the stats table
CREATE OR REPLACE FUNCTION calculate_daily_data(dy date DEFAULT current_date - 1, lag int DEFAULT 2) RETURNS TABLE (
	sensor_nodes_count bigint
, sensors_count bigint
, measurements_hourly_count bigint
, measurements_count bigint
) AS $$
SET LOCAL work_mem = '512MB';
WITH sensors_rollup AS (
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_date(m.datetime, t.tzid)  as datetime
, MAX(m.updated_on) as updated_on
, MIN(first_datetime) as first_datetime
, MAX(last_datetime) as last_datetime
, COUNT(1) AS value_count
, AVG(value_avg) as value_avg
, STDDEV(value_avg) as value_sd
, MIN(value_avg) as value_min
, MAX(value_avg) as value_max
, SUM(value_count) as value_raw_count
, SUM(value_avg*value_count)/SUM(value_count) as value_raw_avg
, MIN(value_min) as value_raw_min
, MAX(value_max) as value_raw_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value_avg) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value_avg) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value_avg) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value_avg) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value_avg) as value_p98
, SUM(error_count) as error_raw_count
, SUM((value_avg IS NULL)::int) as error_count
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE value_count > 0
AND datetime > as_utc(dy, t.tzid)
AND datetime <= as_utc(dy + 1, t.tzid)
AND as_local_hour_int(t.tzid) = lag
GROUP BY 1,2,3
HAVING COUNT(1) > 0
	), inserted AS (
INSERT INTO daily_data (
  sensors_id
, datetime
, updated_on
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, calculated_on)
	SELECT sensors_id
, datetime
, updated_on
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, current_timestamp as calculated_on
	FROM sensors_rollup
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET first_datetime = EXCLUDED.first_datetime
, last_datetime = EXCLUDED.last_datetime
, updated_on = EXCLUDED.updated_on
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_raw_avg = EXCLUDED.value_raw_avg
, value_raw_min = EXCLUDED.value_raw_min
, value_raw_max = EXCLUDED.value_raw_max
, value_raw_count = EXCLUDED.value_raw_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, error_raw_count = EXCLUDED.error_raw_count
, calculated_on = EXCLUDED.calculated_on
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_hourly_count
	, SUM(value_raw_count) as measurements_count
	FROM sensors_rollup;
$$ LANGUAGE SQL;



CREATE OR REPLACE FUNCTION calculate_sensor_daily_data(id int, sd date, ed date) RETURNS TABLE (
	sensor_nodes_count bigint
, sensors_count bigint
, measurements_hourly_count bigint
, measurements_count bigint
) AS $$
SET LOCAL work_mem = '512MB';
WITH sensors_rollup AS (
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_date(m.datetime, t.tzid)  as datetime
, MAX(m.updated_on) as updated_on
, MIN(first_datetime) as first_datetime
, MAX(last_datetime) as last_datetime
, COUNT(1) AS value_count
, AVG(value_avg) as value_avg
, STDDEV(value_avg) as value_sd
, MIN(value_avg) as value_min
, MAX(value_avg) as value_max
, SUM(value_count) as value_raw_count
, SUM(value_avg*value_count)/SUM(value_count) as value_raw_avg
, MIN(value_min) as value_raw_min
, MAX(value_max) as value_raw_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value_avg) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value_avg) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value_avg) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value_avg) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value_avg) as value_p98
, SUM(error_count) as error_raw_count
, SUM((value_avg IS NULL)::int) as error_count
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE value_count > 0
AND datetime > as_utc(sd, t.tzid)
AND datetime <= as_utc(ed, t.tzid)
AND m.sensors_id = id
GROUP BY 1,2,3
HAVING COUNT(1) > 0
	), inserted AS (
INSERT INTO daily_data (
  sensors_id
, datetime
, updated_on
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, calculated_on)
	SELECT sensors_id
, datetime
, updated_on
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, current_timestamp as calculated_on
	FROM sensors_rollup
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET first_datetime = EXCLUDED.first_datetime
, last_datetime = EXCLUDED.last_datetime
, updated_on = EXCLUDED.updated_on
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_raw_avg = EXCLUDED.value_raw_avg
, value_raw_min = EXCLUDED.value_raw_min
, value_raw_max = EXCLUDED.value_raw_max
, value_raw_count = EXCLUDED.value_raw_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, error_raw_count = EXCLUDED.error_raw_count
, calculated_on = EXCLUDED.calculated_on
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_hourly_count
	, SUM(value_raw_count) as measurements_count
	FROM sensors_rollup;
$$ LANGUAGE SQL;


	-- this is just a helper method
CREATE OR REPLACE FUNCTION calculate_daily_data(lag int DEFAULT 2) RETURNS TABLE (
	sensor_nodes_count bigint
, sensors_count bigint
, measurements_hourly_count bigint
, measurements_count bigint
) AS $$
SELECT * FROM calculate_daily_data(current_date - 1, lag);
$$ LANGUAGE SQL;

-- This is the method that is used to update the stats table based on the
	-- daily data table
	-- NOTE: running this alone could lead to misleading data. Ideally we
	--  would run this only after we have calculated a FULL days data
	-- NOTE: the method is written so that even a day with no daily data
	-- will get added to the stats table with zeros
CREATE OR REPLACE FUNCTION upsert_daily_stats(dt date) RETURNS json AS $$
	WITH daily_data_summary AS (
	SELECT datetime
	, SUM(value_count) as measurements_count
	, SUM(value_raw_count) as measurements_raw_count
	, COUNT(1) as sensors_count
	, COUNT(DISTINCT sy.sensor_nodes_id) as sensor_nodes_count
	, MAX(calculated_on) as calculated_on
	, MAX(updated_on) as updated_on
	, SUM(calculated_count) as calculated_count
	FROM daily_data d
	JOIN sensors s ON (d.sensors_id = s.sensors_id)
	JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
	WHERE datetime = dt
	GROUP BY 1)
	INSERT INTO daily_stats(
		datetime
	, sensor_nodes_count
	, sensors_count
	, measurements_count
	, measurements_raw_count
	, calculated_on
	, updated_on
	, calculated_count
	, added_on)
	SELECT datetime
	, COALESCE(sensor_nodes_count, 0)
	, COALESCE(sensors_count, 0)
	, COALESCE(measurements_count, 0)
	, COALESCE(measurements_raw_count, 0)
	, calculated_on
	, updated_on
	, COALESCE(calculated_count, 1)
	, now() as added_on
	FROM (SELECT dt as datetime) d
	LEFT JOIN daily_data_summary USING (datetime)
	ON CONFLICT (datetime) DO UPDATE
	SET sensor_nodes_count = EXCLUDED.sensor_nodes_count
	, sensors_count = EXCLUDED.sensors_count
	, measurements_count = EXCLUDED.measurements_count
	, measurements_raw_count = EXCLUDED.measurements_raw_count
	, calculated_on = EXCLUDED.calculated_on
	, calculated_count = EXCLUDED.calculated_count
	, updated_on = EXCLUDED.updated_on
	RETURNING json_build_object(datetime, measurements_raw_count);
$$ LANGUAGE SQL;


-- this is the function that should be run each hour
-- it will
-- CREATE OR REPLACE FUNCTION calculate_daily_data_full(dt date DEFAULT current_date) RETURNS TABLE (
-- 	tz_offset int
-- , sensor_nodes_count bigint
-- , sensors_count bigint
-- , measurements_hourly_count bigint
-- , measurements_count bigint
-- ) AS $$
-- WITH offsets AS (
-- 	SELECT generate_series(0,23,1) as tz_offset
-- 	), calculated AS (
-- 	SELECT tz_offset, f.*
-- 	FROM offsets o, calculate_daily_data(dt, o.tz_offset) f
-- ), stats AS (
-- 	SELECT upsert_daily_stats(dt)
-- ) SELECT * FROM calculated;
-- $$ LANGUAGE SQL;

	-- The following method will calculate a FULL day and then update the stats table
	-- It runs the normal method for each hour in a 24h series which is
	-- considerable faster (<~2m) then running a whole day in one statement (~1.5h)
	-- NOTE: the commented out method above failed to update the stats table correctly for some reason
DROP FUNCTION IF EXISTS calculate_daily_data_full(date);
CREATE OR REPLACE FUNCTION calculate_daily_data_full(dt date DEFAULT current_date) RETURNS json AS $$
	DECLARE
	 o record;
	 obj json;
	BEGIN
  FOR o IN SELECT generate_series(0,23,1) as tz_offset
	LOOP
		PERFORM calculate_daily_data(dt, o.tz_offset);
	END LOOP;
	-- update the stats table
	SELECT upsert_daily_stats(dt) INTO obj;
	RETURN obj;
	END;
$$ LANGUAGE plpgsql;

-- A view to help find days that have not been summarized
CREATE OR REPLACE VIEW missing_daily_summaries AS
  WITH days AS (
	SELECT MIN(datetime) as datetime_min
	, MAX(datetime) as datetime_max
	FROM hourly_data
	), available_days AS (
	SELECT generate_series(
   datetime_min::date
	, datetime_max::date
  , '1day'::interval)::date as datetime
	FROM days)
	SELECT a.datetime
	FROM available_days a
	LEFT JOIN daily_stats s ON (a.datetime = s.datetime)
	WHERE s.datetime IS NULL
	ORDER BY a.datetime ASC;


-- this is a helper method that will calcluated the next available
	-- day that needs to be calculated, working from the last day forward.
DROP FUNCTION IF EXISTS calculate_next_available_day();
CREATE OR REPLACE FUNCTION calculate_next_available_day() RETURNS json AS $$
  WITH days AS (
	SELECT MIN(datetime) as datetime_min
	, MAX(datetime) as datetime_max
	FROM hourly_data
	), available_days AS (
	SELECT generate_series(
   datetime_min::date
	, datetime_max::date
  , '1day'::interval)::date as datetime
	FROM days
	), selected_day AS (
	SELECT a.datetime
	FROM available_days a
	LEFT JOIN daily_stats s ON (a.datetime = s.datetime)
	WHERE s.datetime IS NULL
	ORDER BY a.datetime ASC
	LIMIT 1
	) SELECT calculate_daily_data_full(datetime)
	FROM selected_day;
$$ LANGUAGE SQL;


-- and now a method to take care of updating the days that have changed
CREATE OR REPLACE FUNCTION recalculate_modified_days(lmt int DEFAULT 10) RETURNS int AS $$
DECLARE
	d record;
	days int := 0;
BEGIN
	FOR d IN SELECT datetime
		FROM daily_stats
		WHERE datetime < current_date
		AND modified_on IS NOT NULL
		AND modified_on > calculated_on
		LIMIT lmt
	  LOOP
	    RAISE NOTICE 'Running % of %', d.datetime, lmt;
	 		PERFORM calculate_daily_data_full(d.datetime);
	  	days := days+1;
	END LOOP;
	RETURN days;
END;
$$ LANGUAGE plpgsql;


SELECT *
	FROM daily_stats
	ORDER BY datetime DESC
	LIMIT 3;



SELECT sensor_nodes_count
	, sensors_count
	, measurements_count as hourly_count
	, measurements_raw_count as raw_count
	, calculated_on
	FROM daily_stats
	ORDER BY datetime DESC
	LIMIT 30;

	WITH days AS (
	SELECT datetime
	FROM (VALUES
	  (date '2022-01-01')
	, (date '2022-01-02')
	, (date '2022-01-03')
	, (date '2022-01-08')
	, (date '2022-01-09')
	, (date '2022-01-10')
	, (date '2022-02-03')
	, (date '2022-02-02')
	, (date '2022-02-03')
	, (date '2022-04-02')
	, (date '2022-04-03')
	) a(datetime)
	), first_days_marked AS (
	SELECT datetime
	, CASE WHEN age(datetime, lag(datetime) OVER (ORDER BY datetime)) <= '1day'::interval
		THEN NULL ELSE 1 END as idx
	FROM days
	), groups_identified AS (
	SELECT datetime
	, SUM(idx) OVER (ORDER BY datetime) as grp
	FROM first_days_marked)
	SELECT MIN(datetime) as day_first
	, MAX(datetime) as day_last
	FROM groups_identified
	GROUP BY grp;




 WITH first_days_marked AS (
	SELECT datetime
	, CASE WHEN age(datetime, lag(datetime) OVER (ORDER BY datetime)) <= '1day'::interval
		THEN NULL ELSE 1 END as idx
	FROM daily_data
	WHERE value_count > 0
	AND sensors_id = 2257208
	), groups_identified AS (
	SELECT datetime
	, SUM(idx) OVER (ORDER BY datetime) as grp
	FROM first_days_marked)
	SELECT MIN(datetime) as day_first
	, MAX(datetime) as day_last
	FROM groups_identified
	GROUP BY grp
	ORDER BY grp;
