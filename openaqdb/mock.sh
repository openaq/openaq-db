#!/bin/bash
cd $( dirname "${BASH_SOURCE[0]}")

export PGDATABASE=$DATABASE_DB
export PGHOST=localhost

psql --single-transaction -f "mock.sql"
psql -c "CALL intialize_sensors_rollup()" \
     -c "SELECT reset_hourly_stats()" \
     -c "CALL update_hourly_data(1000)" \
     -c "CALL update_cached_tables()" \
