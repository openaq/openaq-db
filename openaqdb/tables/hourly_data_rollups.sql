
-- The following tables, functions and views are to handle
-- tracking coverage for the system. If possibly we may also want to replace
-- the rollups table above (which uses groups) with the hourly_data table
-- below. Therefor the table below also includes some extended summary stats
SET search_path = public;
CREATE SCHEMA IF NOT EXISTS _measurements_internal;

CREATE TABLE IF NOT EXISTS hourly_data (
  sensors_id int NOT NULL --REFERENCES sensors ON DELETE CASCADE
, datetime timestamptz NOT NULL
, measurands_id int NOT NULL --REFERENCES measurands -- required for partition
, datetime_first timestamptz NOT NULL
, datetime_last timestamptz NOT NULL
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
, threshold_values jsonb
, error_count int NOT NULL DEFAULT 0
, updated_on timestamptz -- last time the sensor was updated
, calculated_on timestamptz-- last time the row rollup was calculated
, UNIQUE(sensors_id, measurands_id, datetime)
) PARTITION BY RANGE (datetime);

--ALTER TABLE hourly_data
--ADD COLUMN error_count int NOT NULL DEFAULT 0;


CREATE INDEX IF NOT EXISTS hourly_data_sensors_id_idx
ON hourly_data
USING btree (sensors_id);

CREATE INDEX IF NOT EXISTS hourly_data_datetime_idx
ON hourly_data
USING btree (datetime);

CREATE UNIQUE INDEX IF NOT EXISTS hourly_data_sensors_id_datetime_idx
ON hourly_data
USING btree (sensors_id, datetime);

CREATE INDEX IF NOT EXISTS hourly_data_measurands_id_idx
ON hourly_data
USING btree (measurands_id);

CREATE INDEX IF NOT EXISTS hourly_data_measurands_id_datetime_idx
ON hourly_data
USING btree (measurands_id, datetime);

-- not really used but here just in case we need it
CREATE OR REPLACE FUNCTION create_hourly_data_partition(sd date, ed date) RETURNS text AS $$
DECLARE
table_name text := 'hourly_data_'||to_char(sd, 'YYYYMMDD')||||to_char(ed, '_YYYYMMDD');
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS _measurements_internal.%s
          PARTITION OF hourly_data
          FOR VALUES
          FROM (''%s'')
          TO (''%s'');',
          table_name,
          sd,
          ed
          );
   RETURN table_name;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_hourly_data_partition(dt date) RETURNS text AS $$
DECLARE
_table_schema text := '_measurements_internal';
_table_name text := 'hourly_data_'||to_char(dt, 'YYYYMM');
sd date := date_trunc('month', dt);
ed date := date_trunc('month', dt + '1month'::interval);
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS %s.%s
          PARTITION OF hourly_data
          FOR VALUES
          FROM (''%s'')
          TO (''%s'');',
          _table_schema,
          _table_name,
          sd,
          ed
          );
   -- register that table
   INSERT INTO data_table_partitions (
   data_tables_id
   , table_schema
   , table_name
   , start_date
   , end_date)
   SELECT data_tables_id
   , _table_schema
   , _table_name
   , sd
   , ed
   FROM data_tables
   WHERE table_schema = 'public'
   AND table_name = 'hourly_data'
   ON CONFLICT DO NOTHING;
   RETURN _table_name;
END;
$$ LANGUAGE plpgsql;


INSERT INTO data_tables (data_tables_id, table_schema, table_name) VALUES
(2, 'public', 'hourly_data');


WITH dates AS (
SELECT generate_series('2016-01-01'::date, date_trunc('month', current_date + '1month'::interval), '1month'::interval) as dt)
SELECT create_hourly_data_partition(dt::date)
FROM dates;


 -- store it in local time
CREATE TABLE IF NOT EXISTS hourly_data_queue (
   datetime timestamptz NOT NULL
 , tz_offset interval NOT NULL
 , added_on timestamptz NOT NULL DEFAULT now()
 , queued_on timestamptz
 , modified_on timestamptz 											-- last time the hourly data was modified
 , modified_count int NOT NULL DEFAULT 0
 , calculated_on timestamptz
 , calculated_count int NOT NULL DEFAULT 0
 , calculated_seconds double precision
 , sensor_nodes_count int
 , sensors_count int
 , measurements_count int
 , UNIQUE(datetime, tz_offset)
 );



CREATE OR REPLACE FUNCTION reset_hourly_data_queue(
  st timestamptz DEFAULT '-infinity'
  , et timestamptz DEFAULT 'infinity'
  ) RETURNS bigint AS $$
WITH first_and_last AS (
SELECT utc_offset(tz.tzid) as tz_offset
, MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
FROM measurements m
  JOIN sensors s ON (m.sensors_id = s.sensors_id)
  JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
  JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
  JOIN timezones tz ON (sn.timezones_id = tz.timezones_id)
WHERE datetime >= st
AND datetime <= et
  GROUP BY 1
 ), datetimes AS (
  SELECT tz_offset
  , generate_series(
     as_utc_hour(datetime_first, tz_offset)
   , as_utc_hour(datetime_last, tz_offset)
   , '1hour'::interval) as datetime
  FROM first_and_last
) , inserts AS (
 INSERT INTO hourly_data_queue (datetime, tz_offset, modified_on)
  SELECT datetime
  , tz_offset
  , now()
  FROM datetimes
--WHERE has_hourly_measurement(datetime, tz_offset)
  ORDER BY tz_offset, datetime
ON CONFLICT (tz_offset, datetime) DO UPDATE
SET modified_on = GREATEST(EXCLUDED.modified_on, hourly_data_queue.modified_on)
RETURNING 1)
SELECT COUNT(1) FROM inserts;
$$ LANGUAGE SQL;



 CREATE TABLE IF NOT EXISTS hourly_stats (
   datetime timestamp PRIMARY KEY
 , added_on timestamptz NOT NULL DEFAULT now()
 , modified_on timestamptz 											-- last time the hourly data was modified
 , calculated_count int NOT NULL DEFAULT 0
 , updated_on timestamptz												--
 , calculated_on timestamptz
 , sensor_nodes_count int
 , measurements_count int
 , sensors_count int
 );




CREATE OR REPLACE FUNCTION fetch_hourly_data_jobs2(n int DEFAULT 1, min_hour timestamptz DEFAULT NULL, max_hour timestamptz DEFAULT NULL) RETURNS TABLE(
    datetime timestamptz
  , tz_offset interval
  ) AS $$
          SELECT q.datetime
          , q.tz_offset
          FROM hourly_data_queue q
          -- Its either not been calculated or its been modified
          WHERE q.datetime >= COALESCE(min_hour, '-infinity'::date)
          AND q.datetime <= COALESCE(max_hour, now() - '1hour'::interval)
          AND (q.calculated_on IS NULL)-- OR (q.modified_on IS NULL OR q.modified_on > q.calculated_on))
          -- either its never been or it was resently modified but not queued
          --AND (q.queued_on IS NULL -- has not been queued
          --OR (
          --   q.queued_on < now() - '1h'::interval -- a set amount of time has passed AND
          --   AND (
          --     q.queued_on < q.modified_on  -- its been changed since being queued
          --     OR calculated_on IS NULL     -- it was never calculated
          --   )
         -- )
          --)
          ORDER BY q.datetime, q.tz_offset
          LIMIT n;
$$ LANGUAGE sql;



CREATE OR REPLACE FUNCTION fetch_hourly_data_jobs(n int DEFAULT 1, min_hour timestamptz DEFAULT NULL, max_hour timestamptz DEFAULT NULL) RETURNS TABLE(
    datetime timestamptz
  , tz_offset interval
  , queued_on timestamptz
  ) AS $$
  BEGIN
        RETURN QUERY
        UPDATE hourly_data_queue
        SET queued_on = CURRENT_TIMESTAMP
        , calculated_count = calculated_count + 1
        FROM (
          SELECT q.datetime
          , q.tz_offset
          FROM hourly_data_queue q
          -- Its either not been calculated or its been modified
          WHERE q.datetime >= COALESCE(min_hour, '-infinity'::date)
          AND q.datetime <= COALESCE(max_hour, date_trunc('hour', now()))
          AND (q.calculated_on IS NULL OR q.modified_on > q.calculated_on)
          -- either its never been or it was resently modified but not queued
          AND (q.queued_on IS NULL -- has not been queued
          OR (
              q.queued_on < now() - '1h'::interval -- a set amount of time has passed AND
              AND (
                q.queued_on < q.modified_on  -- its been changed since being queued
                OR calculated_on IS NULL     -- it was never calculated
              )
          ))
          ORDER BY q.datetime, q.tz_offset
          LIMIT n
          FOR UPDATE SKIP LOCKED
        ) as d
        WHERE d.datetime = hourly_data_queue.datetime
        AND d.tz_offset = hourly_data_queue.tz_offset
        RETURNING hourly_data_queue.datetime
        , hourly_data_queue.tz_offset
        , hourly_data_queue.queued_on;
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_hourly_data_queue(hr timestamptz, _tz_offset interval) RETURNS bigint AS $$
 WITH hourly_inserts AS (
  INSERT INTO hourly_data_queue (datetime, tz_offset) VALUES
  (date_trunc('hour', hr + _tz_offset + '-1s'::interval)
  , _tz_offset)
  ON CONFLICT (datetime, tz_offset) DO UPDATE
  SET modified_on = now()
  , modified_count = hourly_data_queue.modified_count + 1
  RETURNING datetime, tz_offset
  ) SELECT COUNT(*)
  FROM hourly_inserts;
  $$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION hourly_data_updated_event(hr timestamptz, _tz_offset interval) RETURNS boolean AS $$
  SELECT 't'::boolean;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION calculate_hourly_data(hr timestamptz DEFAULT now() - '1hour'::interval)
  RETURNS TABLE (
	  sensors_id int
  , measurands_id int
  , sensor_nodes_id int
  , datetime timestamptz
  , updated_on timestamptz
  , datetime_first timestamptz
  , datetime_last timestamptz
  , value_count bigint
  , value_avg double precision
  , value_sd double precision
  , value_min double precision
  , value_max double precision
  , value_p02 double precision
  , value_p25 double precision
  , value_p50 double precision
  , value_p75 double precision
  , value_p98 double precision
  , error_count bigint
  ) AS $$
SELECT
  m.sensors_id
, s.measurands_id
, sn.sensor_nodes_id
, as_utc_hour(m.datetime + '1h'::interval, t.tzid) as datetime -- this will make sure weird offsets are kept
, MAX(m.added_on) as updated_on
, MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
, COUNT(1) AS value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value) as value_p98
, SUM((value IS NULL)::int) as error_count
FROM measurements m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE datetime > hr - '1hour'::interval
AND datetime <= hr
--AND utc_offset_hours(hr, t.tzid) = tz_offset
GROUP BY 1,2,3,4
HAVING COUNT(1) > 0;
  $$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION calculate_hourly_data(hr timestamptz DEFAULT now() - '1hour'::interval, _tz_offset interval DEFAULT '0s')
  RETURNS TABLE (
	  sensors_id int
  , measurands_id int
  , sensor_nodes_id int
  , datetime timestamptz
  , updated_on timestamptz
  , datetime_first timestamptz
  , datetime_last timestamptz
  , value_count bigint
  , value_avg double precision
  , value_sd double precision
  , value_min double precision
  , value_max double precision
  , value_p02 double precision
  , value_p25 double precision
  , value_p50 double precision
  , value_p75 double precision
  , value_p98 double precision
  , error_count bigint
  ) AS $$
SELECT
  m.sensors_id
, s.measurands_id
, sn.sensor_nodes_id
-- save as utc hour ending (interval makes it time ending)
, as_utc_hour(m.datetime + '1h'::interval, t.tzid)  as datetime
, MAX(m.added_on) as updated_on
, MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
, COUNT(1) AS value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value) as value_p98
, SUM((value IS NULL)::int) as error_count
FROM measurements m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
-- We want to track everything in the hourly_data_queue and that will be in utc time
-- and then we will need to convert to utc_hour for
WHERE datetime > hr - '1hour'::interval
AND datetime <= hr
AND utc_offset(hr, t.tzid) = _tz_offset
GROUP BY 1,2,3,4
HAVING COUNT(1) > 0;
  $$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION insert_hourly_data(hr timestamptz DEFAULT now() - '1hour'::interval, _tz_offset interval DEFAULT '0s')
  RETURNS TABLE (
	   sensor_nodes_count bigint
   , sensors_count bigint
   , measurements_count bigint
  ) AS $$
SET LOCAL work_mem = '512MB';
WITH data_rollup AS (
  SELECT *
  FROM calculate_hourly_data(hr, _tz_offset)
), data_inserted AS (
INSERT INTO hourly_data (
  sensors_id
, measurands_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, calculated_on)
	SELECT sensors_id
, measurands_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, current_timestamp as calculated_on
	FROM data_rollup
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, updated_on = EXCLUDED.updated_on
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, calculated_on = EXCLUDED.calculated_on
  RETURNING sensors_id, value_count
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_count
	FROM data_rollup;
$$ LANGUAGE SQL;



CREATE OR REPLACE FUNCTION update_hourly_data(hr timestamptz DEFAULT now() - '1hour'::interval, _tz_offset interval DEFAULT '0s') RETURNS bigint AS $$
DECLARE
nw timestamptz := clock_timestamp();
mc bigint;
BEGIN
WITH inserted AS (
  SELECT sensor_nodes_count
  , sensors_count
  , measurements_count
  FROM insert_hourly_data(hr, _tz_offset))
  INSERT INTO hourly_data_queue (
    datetime
  , tz_offset
  , calculated_on
  , calculated_count
  , sensor_nodes_count
  , sensors_count
  , measurements_count
  , calculated_seconds
  )
  SELECT hr
  , _tz_offset
  , now()
  , 1
  , sensor_nodes_count
  , sensors_count
  , measurements_count
  , EXTRACT(EPOCH FROM clock_timestamp() - nw)
  FROM inserted i
  ON CONFLICT (datetime, tz_offset) DO UPDATE
  SET calculated_on = EXCLUDED.calculated_on
  , calculated_count = hourly_data_queue.calculated_count + 1
  , measurements_count = EXCLUDED.measurements_count
  , sensors_count = EXCLUDED.sensors_count
  , sensor_nodes_count = EXCLUDED.sensor_nodes_count
  , calculated_seconds = EXCLUDED.calculated_seconds
  RETURNING measurements_count INTO mc;
  PERFORM hourly_data_updated_event(hr, _tz_offset);
  RETURN mc;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE update_hourly_data(n int DEFAULT 5, min_hour timestamptz DEFAULT NULL, max_hour timestamptz DEFAULT NULL) AS $$
DECLARE
  rw record;
BEGIN
FOR rw IN (
    SELECT datetime
    , tz_offset
     FROM fetch_hourly_data_jobs(n, min_hour, max_hour))
LOOP
  RAISE NOTICE 'updating hour: % - %', rw.datetime, rw.tz_offset;
  PERFORM update_hourly_data(rw.datetime, rw.tz_offset);
  COMMIT;
END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE VIEW "public"."sensor_hourly_coverage" AS
  SELECT r.sensors_id,
    r.datetime,
    r.value_count,
    (s.metadata -> 'hourly_frequency'::text)::integer AS expected,
        CASE
            WHEN r.value_count >= (s.metadata -> 'hourly_frequency'::text)::integer THEN 100::numeric
            ELSE round(r.value_count::numeric / (s.metadata -> 'hourly_frequency'::text)::integer::numeric * 100::numeric)
        END AS coverage
   FROM hourly_data r
     JOIN sensors s ON r.sensors_id = s.sensors_id;
