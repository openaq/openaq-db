CREATE TABLE IF NOT EXISTS rollups (
    groups_id int REFERENCES groups (groups_id),
    measurands_id int,
    sensors_id int,
    rollup text,
    st timestamptz,
    et timestamptz,
    first_datetime timestamptz,
    last_datetime timestamptz,
    value_count bigint,
    value_sum float,
    last_value float,
    minx float,
    miny float,
    maxx float,
    maxy float,
    last_point geography,
    PRIMARY KEY (groups_id, measurands_id, rollup, et)
);

CREATE INDEX rollups_measurands_id_idx ON rollups USING btree (measurands_id);
CREATE INDEX rollups_rollup_idx ON rollups USING btree (rollup);
CREATE INDEX rollups_sensors_id_idx ON rollups USING btree (sensors_id);
CREATE INDEX rollups_st_idx ON rollups USING btree (st);

-- The following tables, functions and views are to handle
-- tracking coverage for the system. If possibly we may also want to replace
-- the rollups table above (which uses groups) with the hourly_rollups table
-- below. Therefor the table below also includes some extended summary stats
CREATE TABLE IF NOT EXISTS hourly_rollups (
  sensors_id int NOT NULL --REFERENCES sensors ON DELETE CASCADE
, datetime timestamptz NOT NULL
, measurands_id int NOT NULL --REFERENCES measurands -- required for partition
, first_datetime timestamptz NOT NULL
, last_datetime timestamptz NOT NULL
, value_count int NOT NULL
, value_avg double precision
, value_sd double precision
, value_min double precision
, value_max double precision
, value_p05 double precision
, value_p50 double precision
, value_p95 double precision
, updated_on timestamptz -- last time the sensor was updated
, calculated_on timestamptz-- last time the row rollup was calculated
, UNIQUE(sensors_id, measurands_id, datetime)
);

CREATE INDEX hourly_rollups_sensors_id_idx
ON hourly_rollups
USING btree (sensors_id);

CREATE INDEX hourly_rollups_datetime_idx
ON hourly_rollups
USING btree (datetime);

-- Create a hypertable (partitioned) using timescaledb
-- Data will also be partitioned by measurand id to make queries
-- within one measurand faster
SELECT create_hypertable(
  relation => 'hourly_rollups'
, time_column_name => 'datetime'
, partitioning_column => 'measurands_id'
, number_partitions => 10
, chunk_time_interval => '1month'::interval
, if_not_exists => TRUE
);


-- create a table to help us keep track of what days have been updated
-- This table is used to back calculate rollup days. If the rollup service (lambda)
-- is running than adding a day and nulling the calculated on will update that day
CREATE TABLE IF NOT EXISTS daily_stats (
  day date NOT NULL UNIQUE
, sensor_nodes_count bigint NOT NULL
, sensors_count bigint NOT NULL
, hours_count bigint NOT NULL
, measurements_count bigint NOT NULL
, calculated_on timestamp
, initiated_on timestamp
, metadata jsonb
);

-- this is the basic function used to rollup an entire day
CREATE OR REPLACE FUNCTION calculate_rollup_daily_stats(day date) RETURNS bigint AS $$
WITH data AS (
SELECT (datetime - '1sec'::interval)::date as day
, h.sensors_id
, sensor_nodes_id
, value_count
FROM hourly_rollups h
JOIN sensors s ON (h.sensors_id = s.sensors_id)
JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
WHERE datetime > day::timestamp
AND  datetime <= day + '1day'::interval
), inserts AS (
INSERT INTO daily_stats (
  day
, sensor_nodes_count
, sensors_count
, hours_count
, measurements_count
, calculated_on
)
SELECT day
, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
, COUNT(DISTINCT sensors_id) as sensors_count
, COUNT(1) as hours_count
, SUM(value_count) as measurements_count
, current_timestamp
FROM data
GROUP BY day
ON CONFLICT (day) DO UPDATE
SET sensor_nodes_count = EXCLUDED.sensor_nodes_count
, sensors_count = EXCLUDED.sensors_count
, hours_count = EXCLUDED.hours_count
, measurements_count = EXCLUDED.measurements_count
, calculated_on = EXCLUDED.calculated_on
RETURNING measurements_count)
SELECT measurements_count
FROM inserts;
$$ LANGUAGE SQL;

-- Function to rollup a give interval to the hour
-- date_trunc is used to ensure that only hourly data is inserted
-- an hour currently takes about 15-30 seconds to roll up, depending on load
-- we add the hour to the datetime so that its saved as time ending
-- we subtract the second so that a value that is recorded as 2022-01-01 10:00:00
-- and is time ending becomes 2022-01-01 09:59:59, and then trucated to the 9am hour
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(st timestamptz, et timestamptz) RETURNS bigint AS $$
WITH inserted AS (
INSERT INTO hourly_rollups (
  sensors_id
, measurands_id
, datetime
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p05
, value_p50
, value_p95
, calculated_on)
SELECT
  m.sensors_id
, measurands_id
, date_trunc('hour', datetime - '1sec'::interval) + '1hour'::interval as datetime
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, COUNT(1) as value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.05) WITHIN GROUP(ORDER BY value) as value_p05
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY value) as value_p95
, current_timestamp as calculated_on
FROM measurements m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
WHERE datetime > date_trunc('hour', st)
AND datetime <= date_trunc('hour', et)
GROUP BY 1,2,3
HAVING COUNT(1) > 0
ON CONFLICT (sensors_id, measurands_id, datetime) DO UPDATE
SET first_datetime = EXCLUDED.first_datetime
, last_datetime = EXCLUDED.last_datetime
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_p05 = EXCLUDED.value_p05
, value_p50 = EXCLUDED.value_p50
, value_p95 = EXCLUDED.value_p95
, calculated_on = EXCLUDED.calculated_on
RETURNING 1)
SELECT COUNT(1)
FROM inserted;
$$ LANGUAGE SQL;


-- Some helper functions to make things easier
-- Pass the time ending timestamp to calculate one hour
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(et timestamptz) RETURNS bigint AS $$
SELECT calculate_hourly_rollup(et - '1hour'::interval, et);
$$ LANGUAGE SQL;

-- Helper function to record a how day
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(dt date) RETURNS bigint AS $$
SELECT calculate_hourly_rollup(dt::timestamptz, dt + '1day'::interval);
$$ LANGUAGE SQL;

-- A method that includes specifying the sensors_id
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(
  id int
, st timestamptz
, et timestamptz
) RETURNS bigint AS $$
WITH inserted AS (
INSERT INTO hourly_rollups (
  sensors_id
, measurands_id
, datetime
, first_datetime
, last_datetime
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p05
, value_p50
, value_p95
, calculated_on)
SELECT
  m.sensors_id
, measurands_id
, date_trunc('hour', datetime - '1sec'::interval) + '1hour'::interval as datetime
, MIN(datetime) as first_datetime
, MAX(datetime) as last_datetime
, COUNT(1) as value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.05) WITHIN GROUP(ORDER BY value) as value_p05
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY value) as value_p95
, current_timestamp as calculated_on
FROM measurements m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
WHERE m.sensors_id = id
AND datetime > date_trunc('hour', st)
AND datetime <= date_trunc('hour', et)
GROUP BY 1,2,3
HAVING COUNT(1) > 0
ON CONFLICT (sensors_id, measurands_id, datetime) DO UPDATE
SET first_datetime = EXCLUDED.first_datetime
, last_datetime = EXCLUDED.last_datetime
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_p05 = EXCLUDED.value_p05
, value_p50 = EXCLUDED.value_p50
, value_p95 = EXCLUDED.value_p95
, calculated_on = EXCLUDED.calculated_on
RETURNING 1)
SELECT COUNT(1)
FROM inserted;
$$ LANGUAGE SQL;

-- helpers for measurand_id
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(id int, et timestamptz) RETURNS bigint AS $$
SELECT calculate_hourly_rollup(id, et - '1hour'::interval, et);
$$ LANGUAGE SQL;

-- Helper function to record a how day
CREATE OR REPLACE FUNCTION calculate_hourly_rollup(id int, dt date) RETURNS bigint AS $$
SELECT calculate_hourly_rollup(id, dt::timestamptz, dt + '1day'::interval);
$$ LANGUAGE SQL;

-- Simple view for coverage
CREATE OR REPLACE VIEW sensor_hourly_coverage AS
SELECT r.sensors_id
, datetime
, value_count
, (s.metadata->'hourly_frequency')::int as expected
, CASE WHEN value_count >= (s.metadata->'hourly_frequency')::int THEN 100
  ELSE ROUND(value_count/(s.metadata->'hourly_frequency')::int::decimal * 100)
  END as coverage
FROM hourly_rollups r
JOIN sensors s ON (r.sensors_id = s.sensors_id);

-- simple table to add some time tracking
-- this is to monitor the update_rollups process which
-- is creating too many table locks
CREATE TABLE IF NOT EXISTS performance_log (
  process_name text
, start_datetime timestamptz
, end_datetime timestamptz DEFAULT current_timestamp
);

CREATE OR REPLACE VIEW performance_log_view AS
SELECT process_name
, start_datetime
, end_datetime
, age(end_datetime, start_datetime) as process_time
FROM performance_log;

CREATE OR REPLACE FUNCTION log_performance(text, timestamptz) RETURNS timestamptz AS $$
  INSERT INTO performance_log (process_name, start_datetime, end_datetime)
  VALUES (pg_backend_pid()||'-'||$1, $2, current_timestamp)
  RETURNING end_datetime;
$$ LANGUAGE SQL;


-- For adding existing days to the stats table
-- INSERT INTO daily_stats (
--   day
-- , sensor_nodes_count
-- , sensors_count
-- , hours_count
-- , measurements_count
-- )
-- SELECT generate_series(MIN(datetime)::date, MAX(datetime)::date, '1day'::interval)::date
-- , -1 as sensor_nodes_count
-- , -1 as sensors_count
-- , -1 as hours_count
-- , -1 as measurements_count
-- FROM measurements
-- ON CONFLICT DO NOTHING;
