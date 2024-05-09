
-- Set the day, offset and sensor to check
\set day '''2022-07-24'''
\set hr '''3h'''
\set sensor 391670

-- Each of the following queries pulls data and summarizes it from
-- a different part of the system. They should all match.


-- Start by checking the data in the final rollup table
-- if you need to update that data you can do it with this function
-- it will take up to 2m so I would not do it every time
-- it will also update the entire day

SELECT SUM(measurements_count) as count_total
FROM calculate_daily_data_full(:day::date);

SELECT upsert_daily_stats(:day::date);

\echo ###########################################################
\echo Summary data pulled from the daily data table
\echo ###########################################################

SELECT sensors_id
	, day
	, first_datetime
	, last_datetime
	, value_count
	, value_avg
	, value_raw_count
	, value_raw_avg
	FROM daily_data
	WHERE day = :day::date
	AND sensors_id = :sensor;

\echo ###########################################################
\echo Summary data pulled from the hourly data
\echo ###########################################################

WITH meas AS (
SELECT m.sensors_id
	, t.tzid as timezone
	, datetime
	, first_datetime
	, last_datetime
	, value_count
	, value_avg
	, as_utc(:day::timestamptz, t.tzid) as from_utc
	FROM hourly_data m
	JOIN sensors s ON (m.sensors_id = s.sensors_id)
	JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
	JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
	JOIN timezones t ON (sn.timezones_id = t.timezones_id)
	WHERE m.sensors_id = :sensor
AND datetime > :day::timestamptz - :hr::interval
AND datetime <= (:day::date + 1)::timestamptz - :hr::interval
	ORDER BY datetime)
SELECT *
	, AVG(value_avg) OVER (PARTITION BY sensors_id) as avg_total
	, SUM(value_count) OVER (PARTITION BY sensors_id) as count_total
	FROM meas;

\echo ###########################################################
\echo Summary data pulled directly from measurements
\echo ###########################################################

WITH meas AS (
SELECT sensors_id
  , date_trunc('hour', datetime - '1sec'::interval) + '1hour'::interval as datetime
	, MIN(datetime)
	, MAX(datetime)
	, AVG(value) as avg_value
	, COUNT(1) as n
	, MIN(value)
	, MAX(value)
	, MAX(added_on)
FROM measurements
WHERE sensors_id = :sensor
AND datetime > :day::timestamptz - :hr::interval
AND datetime <= (:day::date + 1)::timestamptz - :hr::interval
	GROUP BY 1, 2
	ORDER BY 2)
SELECT *
	, AVG(avg_value) OVER (PARTITION BY sensors_id) as avg_total
	, SUM(n) OVER (PARTITION BY sensors_id) as count_total
	FROM meas;
