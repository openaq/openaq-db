#!/bin/bash
cd $( dirname "${BASH_SOURCE[0]}")

export PGDATABASE=$DATABASE_DB
export PGHOST=localhost

psql --single-transaction -f "mock.sql"
psql -c "CALL intialize_sensors_rollup()" \
     -c "SELECT reset_hourly_stats()" \
     -c "CALL update_hourly_data(10000)" \
     -c "CALL update_daily_data(5000)" \
     -c "CALL update_annual_data(5000)" \
     -c "CALL update_cached_tables()" \
     -c "SELECT datetime, AVG(value_avg) as value_avg, AVG(value_min) as value_min, AVG(value_max) as value_max, AVG(value_count) as value_count_avg, MAX(value_count) as value_count_min, MIN(value_count) as value_count_max FROM daily_data GROUP BY 1 ORDER BY 1;" \
     -c "SELECT datetime, AVG(value_avg) as value_avg, AVG(value_min) as value_min, AVG(value_max) as value_max, AVG(value_count) as value_count_avg, MAX(value_count) as value_count_min, MIN(value_count) as value_count_max FROM annual_data GROUP BY 1 ORDER BY 1;"
