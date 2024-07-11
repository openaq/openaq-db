SET search_path = public;
-- A simple table to manage open data exports
-- includes some extra information just for qa/qc
CREATE TABLE IF NOT EXISTS open_data_export_logs (
   sensor_nodes_id int NOT NULL REFERENCES sensor_nodes ON DELETE CASCADE
  , day date NOT NULL
--  , utc_offset interval NOT NULL         -- how many hours from the server time (utc?)
  , records int NOT NULL                   -- how many entries do we have for this location/date
  , measurands int NOT NULL                -- how many unique measurands exist
  , added_on timestamptz DEFAULT now()     -- when was this date first added
  , modified_on timestamptz DEFAULT now()  -- when was this date last modidified
  , queued_on timestamptz                  -- when did we last queue up a change
  , exported_on timestamptz                -- and when did we last finish exporting
  , has_error boolean DEFAULT 'f'
  , key text -- file location
  , version int -- file schema ??
	, checked_on timestamptz
  , metadata json
  , UNIQUE(sensor_nodes_id, day)
);



DROP TABLE IF EXISTS exported_measurements_log;
CREATE TABLE IF NOT EXISTS exported_measurements_log (
    datetime_range tstzrange NOT NULL UNIQUE
  , export_path text UNIQUE                -- in the form s3://bucket/key
  , records_count bigint                   -- how many entries do we have for this location/date
  , updated_on timestamptz                 -- when was data in this period last updated
  , added_on timestamptz DEFAULT now()     -- when was this record first added
  , modified_on timestamptz                -- when was this record last modified
  , queued_on timestamptz                  -- when did we last queue up an export
  , exported_on timestamptz                -- and when did we last finish exporting
  , size_kb real
  , export_time interval
  , EXCLUDE USING gist(datetime_range with &&)
);



DROP TABLE IF EXISTS exported_public_measurements_log;
CREATE TABLE IF NOT EXISTS exported_public_measurements_log (
    datetime_range tstzrange NOT NULL UNIQUE
  , export_path text UNIQUE                -- in the form s3://bucket/key
  , records_count bigint                   -- how many entries do we have for this location/date
  , updated_on timestamptz                 -- when was data in this period last updated
  , added_on timestamptz DEFAULT now()     -- when was this record first added
  , modified_on timestamptz                -- when was this record last modified
  , queued_on timestamptz                  -- when did we last queue up an export
  , exported_on timestamptz                -- and when did we last finish exporting
  , size_kb real
  , export_time interval
  , imported_on timestamptz
  , import_time interval
  , EXCLUDE USING gist(datetime_range with &&)
);


-- CREATE INDEX IF NOT EXISTS export_logs_exported_on_idx
-- ON open_data_export_logs USING btree (exported_on);

-- CREATE INDEX IF NOT EXISTS export_logs_queued_on_idx
-- ON open_data_export_logs USING btree (queued_on);

-- CREATE INDEX IF NOT EXISTS export_logs_modified_on_idx
-- ON open_data_export_logs USING btree (modified_on);

-- CREATE INDEX IF NOT EXISTS export_logs_day_idx
-- ON open_data_export_logs USING btree (day);

-- CREATE INDEX IF NOT EXISTS export_logs_exported_on_is_null
-- ON open_data_export_logs (day) WHERE exported_on IS NULL;

-- CREATE INDEX IF NOT EXISTS export_logs_queued_on_is_null
-- ON open_data_export_logs (day) WHERE queued_on IS NULL;

-- CREATE INDEX IF NOT EXISTS export_logs_nodes_exported_on_is_null
-- ON open_data_export_logs (sensor_nodes_id) WHERE exported_on IS NULL;

-- CREATE INDEX IF NOT EXISTS export_logs_nodes_queued_on_is_null
-- ON open_data_export_logs (sensor_nodes_id) WHERE queued_on IS NULL;


-- DROP INDEX IF EXISTS export_logs_exported_on_idx
-- , export_logs_queued_on_idx
-- , export_logs_modified_on_idx
-- , export_logs_day_idx
-- , export_logs_exported_on_is_null
-- , export_logs_queued_on_is_null
-- , export_logs_nodes_exported_on_is_null
-- , export_logs_nodes_queued_on_is_null
-- ;


CREATE TABLE IF NOT EXISTS export_stats (
    stats_interval interval NOT NULL PRIMARY KEY -- only one right now
  , days_modified bigint
  , days_exported bigint
  , days_added bigint
  , days_pending bigint
  , calculated_on timestamptz DEFAULT now()
);

CREATE OR REPLACE FUNCTION calculate_export_stats(ci interval) RETURNS timestamptz AS $$
WITH m AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE modified_on > added_on
  AND modified_on > now() - ci
), a AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE added_on > now() - ci
), e AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE exported_on > now() - ci
), p AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE exported_on IS NULL
  AND has_error = FALSE
)
INSERT INTO export_stats (
    stats_interval
  , days_modified
  , days_added
  , days_exported
  , days_pending
  , calculated_on)
SELECT ci as stats_interval
, m.n as days_modified
, a.n as days_added
, e.n as days_exported
, p.n as days_pending
, now() as calculated_on
FROM m,a,e,p
ON CONFLICT (stats_interval) DO UPDATE
SET days_modified = EXCLUDED.days_modified
, days_added = EXCLUDED.days_added
, days_exported = EXCLUDED.days_exported
, days_pending = EXCLUDED.days_pending
, calculated_on = EXCLUDED.calculated_on
RETURNING calculated_on;
$$ LANGUAGE SQL;

--SELECT calculate_export_stats('1day');

CREATE SEQUENCE IF NOT EXISTS providers_sq START 10;
CREATE TABLE IF NOT EXISTS providers (
  providers_id int PRIMARY KEY DEFAULT nextval('providers_sq')
  , label text NOT NULL UNIQUE
  , description text
  , is_public boolean DEFAULT true
  -- relates to the sensor_nodes table
  -- in the future we should link the providers_id directly to sensor_nodes
  , source_name text NOT NULL --REFERENCES sensor_nodes(source_name)
  -- the text to use as the root folder in the export method
  , export_prefix text NOT NULL
  , license text
  , metadata jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS providers_source_name_idx ON providers(source_name);

CREATE OR REPLACE FUNCTION get_providers_id(p text)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT providers_id
FROM providers
WHERE source_name = p
LIMIT 1;
$$;

	CREATE TABLE IF NOT EXISTS providers_stats (
	providers_id int NOT NULL REFERENCES providers
, day date NOT NULL
, sensor_nodes_count int
, sensors_count int
, measurements_count int
, offset_min interval
, offset_avg interval
, offset_max interval
, datetime_min timestamptz
, datetime_max timestamptz
, added_on_min timestamptz
, added_on_max timestamptz
, UNIQUE(providers_id, day)
);

\timing on

SELECT datetime::date as day
  , MAX(added_on)
FROM measurements
WHERE datetime > '2023-04-01'
  AND datetime <= '2023-05-01'
  GROUP BY datetime::date;




CREATE OR REPLACE FUNCTION get_latest_measurement_added(rng tstzrange) RETURNS timestamptz AS $$
  SELECT MAX(added_on)
  FROM measurements
  WHERE datetime > lower(rng)
  AND datetime <= upper(rng);
$$ LANGUAGE SQL;


SELECT datetime_range
  , lower(datetime_range) as datetime_first
  , upper(datetime_range) as datetime_last
  , get_latest_measurement_added(datetime_range)
  FROM exported_measurements_log
  ORDER BY upper(datetime_range) DESC
  LIMIT 100;



DO $$DECLARE r record;
BEGIN
    FOR r IN SELECT *
             FROM exported_measurements_log
             WHERE updated_on IS NULL
             ORDER BY upper(datetime_range) DESC
             LIMIT 4000
    LOOP
        RAISE NOTICE 'Running %', lower(r.datetime_range);
        UPDATE exported_measurements_log
        SET updated_on = get_latest_measurement_added(r.datetime_range)
        WHERE datetime_range = r.datetime_range;
    END LOOP;
END$$;




  SELECT *
  INTO exported_measurements_log_temp
  FROM exported_measurements_log
  WHERE updated_on > exported_on;



  DELETE
  FROM exported_measurements_log
  WHERE updated_on > exported_on;




  -- Now I need to insert all the new days
WITH max_day AS (
SELECT MAX(upper(datetime_range))::date as day
  FROM exported_measurements_log)


  INSERT INTO exported_measurements_log (datetime_range)
  WITH ranges AS (
  SELECT tstzrange(dy, dy + '12h'::interval) as dt_range
  FROM generate_series('2021-04-01'::date, '2024-03-12'::date, '12h'::interval) as dy
  ) SELECT dt_range
  FROM ranges r
  LEFT JOIN exported_measurements_log m ON (m.datetime_range && r.dt_range)
  WHERE m.datetime_range IS NULL
  ON CONFLICT DO NOTHING
  ;

  INSERT INTO exported_measurements_log (datetime_range)
  SELECT tstzrange(dy, dy + '1day'::interval) as dt_range
  FROM generate_series('2024-03-12'::date, current_date, '24h'::interval) as dy
  ON CONFLICT DO NOTHING
  ;



 --  SELECT datetime_range, records_count, added_on, exported_on, updated_on
--   FROM exported_measurements_log
--   WHERE exported_on IS NULL
--   OR exported_on < updated_on
--   ORDER BY datetime_range ASC
--   LIMIT 10;


-- SELECT tstzrange(dy, dy + '1day'::interval) as dt_range
--   FROM generate_series('2021-04-01'::date, '2024-03-12'::date, '12h'::interval) as dy;



-- SELECT COUNT(1)
-- FROM exported_measurements_log
-- WHERE exported_on IS NULL;



--   UPDATE exported_public_measurements_log
--   SET queued_on = Null
--   WHERE queued_on < (now() - '20m'::interval)
--   AND exported_on IS NULL;


-- SELECT datetime_range
--   , records_count
--   , export_time
--   , exported_on
--   , age(COALESCE(exported_on, now()), queued_on) as age
--  FROM exported_measurements_log
--   WHERE queued_on > now() - '40m'::interval
--   ORDER BY queued_on DESC;


-- WITH queued AS (
--   SELECT datetime_range
--   FROM exported_measurements_log
--   WHERE (exported_on IS NULL AND queued_on IS NULL)
--   OR exported_on < updated_on
--   ORDER BY lower(datetime_range)
--   LIMIT 10)
-- UPDATE exported_measurements_log l
--   SET queued_on = now()
--   FROM queued q
--   WHERE q.datetime_range = l.datetime_range
-- RETURNING jsonb_build_object('method', 'dump',
--   'args', jsonb_build_object(
--     'datetime_first', lower(l.datetime_range)
--   , 'datetime_last', upper(l.datetime_range)
--   )) as params;



  -- add the export days to the public table
  --DELETE FROM exported_public_measurements_log;
  INSERT INTO exported_public_measurements_log (datetime_range)
  SELECT tstzrange(dy, dy + '3day'::interval) as dt_range
  FROM generate_series('2016-01-01'::date, current_date, '3d'::interval) as dy
  ON CONFLICT DO NOTHING
  ;



  SELECT COUNT(1) as n
  , MIN(import_time)
  , AVG(import_time)
  , MAX(import_time)
  FROM exported_public_measurements_log
  WHERE imported_on IS NOT NULL;


--  SELECT MIN(export_time)
--   , AVG(export_time)
--   , MAX(export_time)
--   , MAX(upper(datetime_range))
--   , SUM((exported_on IS NOT NULL)::int) as exported
--   , SUM((queued_on IS NOT NULL AND exported_on IS NULL)::int) as pending
--   , SUM((queued_on IS NOT NULL AND exported_on IS NULL AND age(now(),queued_on)>'15m'::interval)::int) as expired
--   FROM exported_public_measurements_log
--   WHERE (queued_on IS NOT NULL OR exported_on IS NOT NULL)
--   AND queued_on > now() - '140m'::interval
--   ;


--   SELECT COUNT(*)
--   , MIN(lower(datetime_range))
--   , MAX(upper(datetime_range))
--    FROM exported_public_measurements_log
--   WHERE exported_on IS NULL;
