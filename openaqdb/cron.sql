SELECT cron.schedule_in_database(
 'run-updates-old'
 , '*/10 * * * *'
 , $$CALL run_updates(null, jsonb_build_object('end', current_date, 'start', current_date - 1))$$
 , 'openaq'
 );

-- turn this off by default
UPDATE cron.job
SET active=false
WHERE jobname = 'run-updates-old';

-- Job to keep rollups up to date
-- right now its taking about 20s per hour
SELECT cron.schedule_in_database(
  'update-hourly-data'
  , '*/10 * * * *'
  , $$CALL update_hourly_data(80)$$
  , 'openaq'
  );

-- every hour on the 1/2 hour
SELECT cron.schedule_in_database(
  'update-daily-data'
  , '30 * * * *'
  , $$SELECT calculate_daily_data_full()$$
  , 'openaq'
  );


-- at quarter past each hour calculate
-- the latest 10 hours that need updating
SELECT cron.schedule_in_database(
  'update-hourly-data-latest'
  , '15 * * * *'
  , $$CALL update_hourly_data_latest(10)$$
  , 'openaq'
  );

-- keep the cached tables up to date
-- last checked this takes about 2s to run
SELECT cron.schedule_in_database(
  'update-cached-tables'
  , '*/5 * * * *'
  , $$CALL update_cached_tables()$$
  , 'openaq'
  );

SELECT cron.schedule_in_database(
  'update-daily-cached-tables'
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
  , $$SELECT create_measurements_partition(current_date + '1month'::interval)$$
  , 'openaq'
);

SELECT cron.schedule_in_database(
  'create-hourly-data-partition'
  , '* * 1 * *'
  , $$SELECT create_hourly_data_partition(current_date + '1month'::interval)$$
  , 'openaq'
);

SELECT cron.schedule_in_database(
  'check-metadata'
  , '0 * * * *'
  , $$CALL check_metadata()$$
  , 'openaq'
);

SELECT cron.schedule_in_database(
  'update-providers-stats'
  , '0 1 * * *'
  , $$SELECT update_providers_stats()$$
  , 'openaq'
  );




WITH jobs AS (
	SELECT jobid
	, start_time::date as day
	, age(end_time, start_time) as duration
	FROM cron.job_run_details
	WHERE start_time > current_date - 14
	AND jobid = 14
	)
	SELECT jobid
	, day
	, MIN(duration)
	, MAX(duration)
	, AVG(duration)
	, COUNT(1)
	FROM jobs
	GROUP BY jobid, day
	ORDER BY 1,2
	LIMIT 30;
