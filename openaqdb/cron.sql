-- SELECT cron.schedule_in_database(
--  'run-updates-old'
--  , '*/10 * * * *'
--  , $$CALL run_updates(null, jsonb_build_object('end', current_date, 'start', current_date - 1))$$
--  , 'openaq'
--  );

-- -- turn this off by default
-- UPDATE cron.job
-- SET active=false
-- WHERE jobname = 'run-updates-old';

-- Job to keep rollups up to date
-- right now its taking about 20s per hour
SELECT cron.schedule_in_database(
  'rollup-hourly-data'
  , '*/10 * * * *'
  , $$CALL update_hourly_data(1000)$$
  , 'openaq'
  );

SELECT cron.schedule_in_database(
  'rollup-daily-data'
  , '*/20 * * * *'
  , $$CALL update_daily_data(500)$$
  , 'openaq'
  );

SELECT cron.schedule_in_database(
  'rollup-annual-data'
  , '0 * * * *'
  , $$CALL update_annual_data(25)$$
  , 'openaq'
  );

-- at quarter past each hour calculate
-- the latest 10 hours that need updating
SELECT cron.schedule_in_database(
  'rollup-hourly-data-latest'
  , '15 * * * *'
  , $$CALL update_hourly_data_latest(10)$$
  , 'openaq'
  );

-- keep the cached tables up to date
-- last checked this takes about 2s to run
SELECT cron.schedule_in_database(
  'refresh-cached-tables'
  , '*/5 * * * *'
  , $$CALL update_cached_tables()$$
  , 'openaq'
  );

SELECT cron.schedule_in_database(
  'refresh-daily-cached-tables'
  , '0 1 * * *'
  , $$CALL update_daily_cached_tables()$$
  , 'openaq'
  );

SELECT cron.schedule_in_database(
  'calculate-export-stats'
  , '0 * * * *'
  , $$SELECT calculate_export_stats('1hour'::interval)$$
  , 'openaq'
);

SELECT cron.schedule_in_database(
  'calculate-partition-stats'
  , '0 * * * *'
  , $$SELECT calculate_partition_stats()$$
  , 'openaq'
);

SELECT cron.schedule_in_database(
  'calculate-providers-stats'
  , '0 1 * * *'
  , $$SELECT update_providers_stats()$$
  , 'openaq'
  );

-- just in case we start having failed ingestions
-- we dont want to keep them open
SELECT cron.schedule_in_database(
  'cancel-stalled-ingestions'
  , '*/5 * * * *'
  , $$SELECT cancel_ingestions('30m')$$
  , 'openaq'
);

-- make sure the partitions exist
-- builds the next months partition in advance
SELECT cron.schedule_in_database(
  'create-measurements-partition'
  , '* * 1 * *'
  , $$SELECT create_measurements_partition((current_date + '1month'::interval)::date)$$
  , 'openaq'
);

SELECT cron.schedule_in_database(
  'create-hourly-data-partition'
  , '* * 1 * *'
  , $$SELECT create_hourly_data_partition((current_date + '1month'::interval)::date)$$
  , 'openaq'
);

SELECT cron.schedule_in_database(
  'check-metadata'
  , '0 * * * *'
  , $$CALL check_metadata()$$
  , 'openaq'
);


 CREATE OR REPLACE FUNCTION stop_rollups(bool DEFAULT 't') RETURNS bigint AS $$
  WITH jobs AS (
  UPDATE cron.job
  SET active = NOT $1
  WHERE jobname ~* 'rollup'
  AND active = $1
  RETURNING jobid)
  SELECT COUNT(1) FROM jobs;
 $$ LANGUAGE SQL;


 CREATE OR REPLACE FUNCTION stop_ingesting(bool DEFAULT 't') RETURNS bigint AS $$
  WITH jobs AS (
  UPDATE cron.job
  SET active = NOT $1
  WHERE jobname ~* 'ingest'
  AND active = $1
  RETURNING jobid)
  SELECT COUNT(1) FROM jobs;
 $$ LANGUAGE SQL;


 CREATE OR REPLACE FUNCTION stop_calculating(bool DEFAULT 't') RETURNS bigint AS $$
  WITH jobs AS (
  UPDATE cron.job
  SET active = NOT $1
  WHERE jobname ~* 'calculate'
  AND active = $1
  RETURNING jobid)
  SELECT COUNT(1) FROM jobs;
 $$ LANGUAGE SQL;


 CREATE OR REPLACE FUNCTION stop_refreshing(bool DEFAULT 't') RETURNS bigint AS $$
  WITH jobs AS (
  UPDATE cron.job
  SET active = NOT $1
  WHERE jobname ~* 'refresh'
  AND active = $1
  RETURNING jobid)
  SELECT COUNT(1) FROM jobs;
 $$ LANGUAGE SQL;


 CREATE OR REPLACE FUNCTION stop_everything(bool DEFAULT 't') RETURNS bigint AS $$
  WITH jobs AS (
  UPDATE cron.job
  SET active = NOT $1
  WHERE active = $1
  RETURNING jobid)
  SELECT COUNT(1) FROM jobs;
 $$ LANGUAGE SQL;


  CREATE OR REPLACE VIEW recent_jobs_summary AS
  WITH jobs AS (
	SELECT d.jobid
  , j.active
  , jobname
	, start_time::date as day
	, age(end_time, start_time) as duration
  , (status = 'failed')::int as failed
	FROM cron.job_run_details d
  JOIN cron.job j ON (d.jobid=j.jobid)
	WHERE start_time > current_date - 7
	--AND d.jobid = 2
	)
	SELECT jobid
  , jobname
  , active
	, MIN(duration) as min_duration
	, MAX(duration) as max_duration
	, AVG(duration) as avg_duration
	, COUNT(1) as n
  , SUM(failed) as failed
	FROM jobs
	GROUP BY jobid, jobname, active
	ORDER BY 1,2 DESC;
