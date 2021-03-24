#!/bin/bash
cd $( dirname "${BASH_SOURCE[0]}")
pg_ctl -D $PGDATA -o "-c listen_addresses='*' -p 5432" -m fast -w restart

sleep 3

createdb openaq
export PGDATABASE=openaq
export PGHOST=localhost

psql --single-transaction -v ON_ERROR_STOP=1<<EOSQL
CREATE ROLE apiuser WITH LOGIN PASSWORD '${OPENAQ_APIUSER_PW}';
CREATE ROLE rwuser WITH LOGIN PASSWORD '${OPENAQ_RWUSER_PW}';

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES to public;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE ON SEQUENCES to public;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES to rwuser;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES to rwuser;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON FUNCTIONS to rwuser;
EOSQL

set -e

psql --single-transaction -v ON_ERROR_STOP=1 -f init.sql

gunzip -c lookups/countries.tsv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy countries from stdin"
gunzip -c lookups/timezones.tsv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy timezones from stdin"
gunzip -c lookups/sources_from_openaq.tsv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy sources_from_openaq from stdin"
