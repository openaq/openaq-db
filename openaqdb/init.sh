#!/bin/bash
source .env
createdb openaq
export PGDATABASE=openaq
psql <<"EOSQL"
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

psql -f init.sql

gunzip -c lookups/countries.tsv.gz | psql -c "copy countries from stdin"
gunzip -c lookups/timezones.tsv.gz | psql -c "copy timezones from stdin"
gunzip -c lookups/sources_from_openaq.tsv.gz | psql -c "copy sources_from_openaq from stdin"
