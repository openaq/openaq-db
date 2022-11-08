#!/bin/bash
# cd $( dirname "${BASH_SOURCE[0]}")
# pg_ctl -D $PGDATA -o "-c listen_addresses='*' -p 5432" -m fast -w restart
# sleep 3

createdb $DATABASE_DB
export PGDATABASE=$DATABASE_DB
export PGHOST=localhost

psql --single-transaction -v ON_ERROR_STOP=1<<EOSQL
ALTER USER postgres WITH PASSWORD '${DATABASE_POSTGRES_PASSWORD}';
CREATE ROLE ${DATABASE_READ_USER} WITH LOGIN PASSWORD '${DATABASE_READ_PASSWORD}';
CREATE ROLE ${DATABASE_WRITE_USER} WITH LOGIN PASSWORD '${DATABASE_WRITE_PASSWORD}';

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES to public;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE ON SEQUENCES to public;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES to ${DATABASE_WRITE_USER};

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES to ${DATABASE_WRITE_USER};

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON FUNCTIONS to ${DATABASE_WRITE_USER};
EOSQL

set -e

psql --single-transaction -v ON_ERROR_STOP=1 -f init.sql

psql --single-transaction -v ON_ERROR_STOP=1 -f lookups/measurands.sql
gunzip -c lookups/countries.tsv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy countries from stdin"
gunzip -c lookups/timezones.tsv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy timezones from stdin"
gunzip -c lookups/sources_from_openaq.tsv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy sources_from_openaq from stdin"
