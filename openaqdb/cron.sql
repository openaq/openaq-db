SELECT cron.schedule_in_database('run-updates', '*/10 * * * *', $$CALL run_updates(null, jsonb_build_object('end', current_date, 'start', current_date - 1))$$, 'openaq');

-- Job to keep rollups up to date
-- run every hour on the quarter hour
SELECT cron.schedule_in_database('update-rollups', '15 * * * *', $$CALL update_rollups(150)$$, 'openaq');


SELECT cron.schedule_in_database('update-cached-tables', '*/5 * * * *', $$CALL update_cached_tables()$$, 'openaq');
