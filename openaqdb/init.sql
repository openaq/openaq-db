
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS "unaccent"; -- used in slugify
--END;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES to public;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE ON SEQUENCES to public;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES to :DATABASE_WRITE_USER;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES to :DATABASE_WRITE_USER;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON FUNCTIONS to :DATABASE_WRITE_USER;


--BEGIN;
-- General lookup tables
\i tables/countries.sql
\i tables/timezones.sql
--COMMIT;

--BEGIN;
-- Lookup tables for legacy OPENAQ
\i tables/sources_from_openaq_v1.sql
--COMMIT;

--BEGIN;
-- Tables for LCS Data Model
\i tables/fetchlogs.sql
\i tables/sensor_nodes.sql
\i tables/sensor_systems.sql
\i tables/measurands.sql
\i tables/sensors.sql
\i tables/versions.sql
\i tables/measurements.sql
\i tables/sources.sql
\i tables/origins.sql
\i tables/groups.sql
\i tables/readmes.sql
\i tables/exports.sql
\i tables/rejects.sql
\i tables/users.sql
\i tables/flags.sql
\i tables/metadata.sql
\i idempotent/util_functions.sql

--\i tables/rollups.sql
\i tables/sensors_rollup.sql
\i tables/hourly_data_rollups.sql
\i tables/thresholds.sql
\i tables/performance_log.sql
----
\i tables/deployments.sql
\i tables/licenses.sql
\i locations/locations.sql
\i tables/timezones_views.sql
\i tables/countries_views.sql
\i tables/providers_views.sql
\i tables/parameters_views.sql
\i tables/daily_data_rollups.sql
\i tables/annual_data_rollups.sql
\i tables/lists.sql
\i tables/measurements_view.sql

-- Load immutable views/functions
-- file contains begin/commit
\i refresh_idempotent.sql

INSERT INTO fetchlogs (key, last_modified) VALUES
  ('uploaded/measures/houston/61509.csv.gz', now())
, ('realtime-gzipped/2022-10-04/1664912239.ndjson.gz', now())
ON CONFLICT DO NOTHING
;

-- Add the houston mobile locations
INSERT INTO fetchlogs (key, last_modified) VALUES
 ('uploaded/measures/houston/61509.csv.gz', now())
ON CONFLICT DO NOTHING
;

WITH keys as (
  SELECT format('uploaded/measures/houston/61507_%s.csv.gz', to_char(generate_series(1,83) - 1, 'fm00'))
)
INSERT INTO fetchlogs (key, last_modified)
SELECT *, now()
FROM keys;


WITH keys as (
  SELECT format('uploaded/measures/houston/61508_%s.csv.gz', to_char(generate_series(1,239) - 1, 'fm000'))
)
INSERT INTO fetchlogs (key, last_modified)
SELECT *, now()
FROM keys;
