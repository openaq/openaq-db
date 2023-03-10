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
  'update-rollups'
  , '*/10 * * * *'
  , $$CALL update_rollups(20)$$
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

-- just in case we start having failed ingestions
-- we dont want to keep them open
SELECT cron.schedule_in_database(
  'cancel-stalled-ingestions'
  , '*/5 * * * *'
  , $$SELECT cancel_ingestions('30m')$$
  , 'openaq'
);
