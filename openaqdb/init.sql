BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;
--CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS btree_gist;
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
\i tables/measurements.sql
\i tables/sources.sql
\i tables/origins.sql
\i tables/groups.sql
\i tables/rollups.sql
\i tables/readmes.sql
\i tables/exports.sql
\i tables/rejects.sql
\i tables/users.sql
\i tables/metadata.sql
\i locations/locations.sql
COMMIT;

-- Load immutable views/functions
-- file contains begin/commit
\i refresh_idempotent.sql

INSERT INTO fetchlogs (key) VALUES
  ('lcs-etl-pipeline/measures/purpleair/1664911958-z2atn.csv.gz')
, ('realtime-gzipped/2022-10-04/1664912239.ndjson.gz')
;
