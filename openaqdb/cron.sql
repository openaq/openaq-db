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
