#!/bin/bash
cd $( dirname "${BASH_SOURCE[0]}")
# must restart the service or the install fails
pg_ctl -D $PGDATA -o "-c listen_addresses='*' -p 5432" -m fast -w restart
sleep 3
echo "Installing ${DATABASE_DB}/${DATABASE_READ_USER} at localhost"

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

psql --single-transaction \
     -v ON_ERROR_STOP=1 \
     -v DATABASE_WRITE_USER="${DATABASE_WRITE_USER}" \
     -v DATABASE_READ_USER="${DATABASE_READ_USER}" \
     -f init.sql

psql --single-transaction -v ON_ERROR_STOP=1 -f lookups/measurands.sql
psql --single-transaction -v ON_ERROR_STOP=1 -f lookups/measurands_map.sql
psql --single-transaction -v ON_ERROR_STOP=1 -f lookups/thresholds.sql
psql --single-transaction -v ON_ERROR_STOP=1 -f lookups/thresholds.sql
gunzip -c lookups/countries.csv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy countries (iso, iso_a3, name, geom) from stdin DELIMITER ',' CSV HEADER"
gunzip -c lookups/timezones.tsv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy timezones from stdin"
gunzip -c lookups/providers_data.csv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "COPY providers (providers_id,label,description,source_name,export_prefix,license,metadata,owner_entities_id) FROM stdin DELIMITER ',' CSV HEADER"
gunzip -c lookups/sources_from_openaq.tsv.gz | psql --single-transaction -v ON_ERROR_STOP=1 -c "copy sources_from_openaq from stdin"

psql --single-transaction -v ON_ERROR_STOP=1 -f lookups/licenses.sql
psql --single-transaction -v ON_ERROR_STOP=1 -f lookups/providers_licenses.sql

echo 'installed'
