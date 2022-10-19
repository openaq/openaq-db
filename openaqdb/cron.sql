SELECT cron.schedule_in_database('run-updates', '*/10 * * * *', $$CALL run_updates(null, jsonb_build_object('end', current_date, 'start', current_date - 1))$$, 'openaq');
