DROP TABLE IF EXISTS daily_stats;

	-- Start by pulling from the hourly data which days need to be updated
	-- and/or, when we calculate the

	DROP TABLE IF EXISTS daily_stats;
	CREATE TABLE IF NOT EXISTS daily_stats (
   day date PRIMARY KEY
 , added_on timestamptz NOT NULL DEFAULT now()
 , modified_on timestamptz
 , calculated_count int NOT NULL DEFAULT 0
 , updated_on timestamptz
 , calculated_on timestamptz
 , sensor_nodes_count int
 , measurements_count int
 , measurements_raw_count int
 , sensors_count int
 );


-- initialize

  WITH days AS (
	SELECT MIN(datetime) as datetime_min
	, MAX(datetime) as datetime_max
	FROM hourly_data)
	INSERT INTO daily_stats (day)
	SELECT generate_series(
   datetime_min::date
	, datetime_max::date
  , '1day'::interval)::date
	FROM days
	ON CONFLICT DO NOTHING;



CREATE TABLE IF NOT EXISTS daily_data (
  sensors_id int NOT NULL --REFERENCES sensors ON DELETE CASCADE
, day date NOT NULL
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
, updated_on timestamptz -- last time the sensor data was updated
, calculated_on timestamptz-- last time the row rollup was calculated
, calculated_count int DEFAULT 1
, UNIQUE(sensors_id, day)
)

CREATE INDEX IF NOT EXISTS daily_data_sensors_id_idx
ON daily_data
USING btree (sensors_id);

CREATE INDEX IF NOT EXISTS daily_data_day_idx
ON daily_data
USING btree (day);


DROP FUNCTION IF EXISTS daily_data_check(sd date, ed date, sids int[]);
CREATE OR REPLACE FUNCTION daily_data_check(sd date, ed date, sids int[]) RETURNS TABLE (
	sensors_id int
	, sensor_nodes_id int
	, day date
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
, as_date(m.datetime, t.tzid)  as day
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
JOIN timezones t ON (sn.timezones_id = t.gid)
WHERE value_count > 0
AND datetime > as_utc(sd, t.tzid)
AND datetime <= as_utc(ed, t.tzid)
AND m.sensors_id = ANY(sids)
GROUP BY 1,2,3,4
HAVING COUNT(1) > 0;
$$ LANGUAGE SQL;




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
, as_date(m.datetime, t.tzid)  as day
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
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.gid)
WHERE value_count > 0
AND datetime > as_utc(dy, t.tzid)
AND datetime <= as_utc(dy + 1, t.tzid)
AND as_local_hour_int(t.tzid) = lag
GROUP BY 1,2,3
HAVING COUNT(1) > 0
	), inserted AS (
INSERT INTO daily_data (
  sensors_id
, day
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
, calculated_on)
	SELECT sensors_id
, day
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
, current_timestamp as calculated_on
	FROM sensors_rollup
ON CONFLICT (sensors_id, day) DO UPDATE
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
, calculated_on = EXCLUDED.calculated_on
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_hourly_count
	, SUM(value_raw_count) as measurements_count
	FROM sensors_rollup;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION calculate_daily_data(lag int DEFAULT 2) RETURNS TABLE (
	sensor_nodes_count bigint
, sensors_count bigint
, measurements_hourly_count bigint
, measurements_count bigint
) AS $$
SELECT * FROM calculate_daily_data(current_date - 1, lag);
$$ LANGUAGE SQL;





SELECT *
	FROM calculate_daily_data_full('2022-07-24');


	CREATE OR REPLACE FUNCTION upsert_daily_stats(dt date) RETURNS json AS $$
WITH daily_data_summary AS (
	SELECT day
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
	WHERE day = dt
	GROUP BY 1)
	INSERT INTO daily_stats(
		day
	, sensor_nodes_count
	, sensors_count
	, measurements_count
	, measurements_raw_count
	, calculated_on
	, updated_on
	, calculated_count
	, added_on)
	SELECT day
	, sensor_nodes_count
	, sensors_count
	, measurements_count
	, measurements_raw_count
	, calculated_on
	, updated_on
	, calculated_count
	, now() as added_on
	FROM daily_data_summary
	ON CONFLICT (day) DO UPDATE
	SET sensor_nodes_count = EXCLUDED.sensor_nodes_count
	, sensors_count = EXCLUDED.sensors_count
	, measurements_count = EXCLUDED.measurements_count
	, measurements_raw_count = EXCLUDED.measurements_raw_count
	, calculated_on = EXCLUDED.calculated_on
	, calculated_count = EXCLUDED.calculated_count
	, updated_on = EXCLUDED.updated_on
	, modified_on = EXCLUDED.added_on
	RETURNING json_build_object(day, measurements_raw_count);
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION calculate_daily_data_full(dt date) RETURNS TABLE (
	tz_offset int
, sensor_nodes_count bigint
, sensors_count bigint
, measurements_hourly_count bigint
, measurements_count bigint
) AS $$
WITH offsets AS (
	SELECT generate_series(0,23,1) as tz_offset
	), calculated AS (
	SELECT tz_offset, f.*
	FROM offsets o, calculate_daily_data(dt, o.tz_offset) f
	), stats AS (
	SELECT upsert_daily_stats(dt)
	;
$$ LANGUAGE SQL;


	SELECT
SELECT *
FROM hourly_data
WHERE sensors_id = 391223
AND  datetime > date_trunc('hour', :st)
AND datetime <= date_trunc('hour', :et);
