
CREATE TABLE IF NOT EXISTS annual_data (
  sensors_id int NOT NULL REFERENCES sensors ON DELETE CASCADE
, datetime date NOT NULL -- time be
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
);

CREATE INDEX IF NOT EXISTS annual_data_sensors_id_idx
ON annual_data
USING btree (sensors_id);

CREATE INDEX IF NOT EXISTS annual_data_day_idx
ON annual_data
USING btree (datetime);


CREATE TABLE IF NOT EXISTS annual_data_queue (
   datetime date NOT NULL
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
 , measurements_raw_count int
 , UNIQUE(datetime, tz_offset)
 );


 CREATE TABLE IF NOT EXISTS annual_stats (
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


CREATE OR REPLACE FUNCTION fetch_annual_data_jobs(n int DEFAULT 1, min_day date DEFAULT NULL, max_day date DEFAULT NULL) RETURNS TABLE(
    datetime date
  , tz_offset interval
  , queued_on timestamptz
  ) AS $$
  BEGIN
        RETURN QUERY
        UPDATE annual_data_queue
        SET queued_on = CURRENT_TIMESTAMP
        , calculated_count = calculated_count + 1
        FROM (
          SELECT q.datetime
          , q.tz_offset
          FROM annual_data_queue q
          -- Its either not been calculated or its been modified
          WHERE q.datetime >= COALESCE(min_day, '-infinity'::date)
          AND q.datetime <= COALESCE(max_day, current_date - '1year'::interval)
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
        WHERE d.datetime = annual_data_queue.datetime
        AND d.tz_offset = annual_data_queue.tz_offset
        RETURNING annual_data_queue.datetime
        , annual_data_queue.tz_offset
        , annual_data_queue.queued_on;
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_annual_data_queue(dt date, _tz_offset interval) RETURNS bigint AS $$
 WITH annual_inserts AS (
  INSERT INTO annual_data_queue (datetime, tz_offset) VALUES
  (date_trunc('year', dt + _tz_offset + '-1s'::interval), _tz_offset)
  ON CONFLICT (datetime, tz_offset) DO UPDATE
  SET modified_on = now()
  , modified_count = annual_data_queue.modified_count + 1
  RETURNING datetime, tz_offset
  ) SELECT COUNT(*)
  FROM annual_inserts;
  $$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION daily_data_updated_event(dy date, _tz_offset interval) RETURNS boolean AS $$
 SELECT update_annual_data_queue(dy, _tz_offset)>0;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION calculate_annual_data(dy date DEFAULT current_date - 1, _tz_offset interval DEFAULT '0s')
  RETURNS TABLE (
	  sensors_id int
  , sensor_nodes_id int
  , datetime date
  , updated_on timestamptz
  , datetime_first timestamptz
  , datetime_last timestamptz
  , value_count bigint
  , value_avg double precision
  , value_sd double precision
  , value_min double precision
  , value_max double precision
  , value_raw_count bigint
  , value_raw_avg double precision
  , value_raw_min double precision
  , value_raw_max double precision
  , value_p02 double precision
  , value_p25 double precision
  , value_p50 double precision
  , value_p75 double precision
  , value_p98 double precision
  , error_raw_count bigint
  , error_count bigint
  ) AS $$
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_year(m.datetime, t.tzid)  as datetime
, MAX(m.updated_on) as updated_on
, MIN(datetime_first) as datetime_first
, MAX(datetime_last) as datetime_last
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
AND datetime > as_utc(date_trunc('year', dy), t.tzid)
AND datetime <= as_utc(date_trunc('year', dy + '1year'::interval), t.tzid)
AND utc_offset(dy, t.tzid) = _tz_offset
GROUP BY 1,2,3
HAVING COUNT(1) > 0;
  $$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION insert_annual_data_by_offset(dy date DEFAULT current_date - 1, _tz_offset interval DEFAULT '0s')
  RETURNS TABLE (
	   sensor_nodes_count bigint
   , sensors_count bigint
   , measurements_hourly_count bigint
   , measurements_count bigint
  ) AS $$
SET LOCAL work_mem = '512MB';
WITH data_rollup AS (
  SELECT *
  FROM calculate_annual_data(dy, _tz_offset)
), data_inserted AS (
INSERT INTO annual_data (
  sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
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
, datetime_first
, datetime_last
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
	FROM data_rollup
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
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
  RETURNING sensors_id, value_count, value_raw_count
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_hourly_count
	, SUM(value_raw_count) as measurements_count
	FROM data_rollup;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION annual_data_updated_event(dy date, _tz_offset interval) RETURNS boolean AS $$
 SELECT 't'::boolean;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION update_annual_data(dy date DEFAULT current_date - 1, _tz_offset interval DEFAULT '0s') RETURNS bigint AS $$
DECLARE
nw timestamptz := clock_timestamp();
mc bigint;
BEGIN
WITH inserted AS (
  SELECT sensor_nodes_count
  , sensors_count
  , measurements_hourly_count
  , measurements_count
  FROM insert_annual_data_by_offset(dy, _tz_offset))
  INSERT INTO annual_data_queue (
    datetime
  , tz_offset
  , calculated_on
  , calculated_count
  , sensor_nodes_count
  , sensors_count
  , measurements_count
  , measurements_raw_count
  , calculated_seconds
  )
  SELECT dy
  , _tz_offset
  , now()
  , 1
  , sensor_nodes_count
  , sensors_count
  , measurements_hourly_count
  , measurements_count
  , EXTRACT(EPOCH FROM clock_timestamp() - nw)
  FROM inserted
  ON CONFLICT (datetime, tz_offset) DO UPDATE
  SET calculated_on = EXCLUDED.calculated_on
  , calculated_count = annual_data_queue.calculated_count + 1
  , measurements_count = EXCLUDED.measurements_count
  , sensors_count = EXCLUDED.sensors_count
  , sensor_nodes_count = EXCLUDED.sensor_nodes_count
  , calculated_seconds = EXCLUDED.calculated_seconds
  RETURNING measurements_count INTO mc;
  PERFORM annual_data_updated_event(dy, _tz_offset);
  RETURN mc;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE update_annual_data(n int DEFAULT 5, min_day date DEFAULT NULL, max_day date DEFAULT NULL) AS $$
DECLARE
  rw record;
BEGIN
FOR rw IN (
    SELECT datetime
    , tz_offset
     FROM fetch_annual_data_jobs(n, min_day, max_day))
LOOP
  RAISE NOTICE 'updating year: % - %', rw.datetime, rw.tz_offset;
  PERFORM update_annual_data(rw.datetime, rw.tz_offset);
  COMMIT;
END LOOP;
END;
$$ LANGUAGE plpgsql;
