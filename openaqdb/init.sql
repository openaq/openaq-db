BEGIN;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS btree_gist;
END;

BEGIN;
-- General lookup tables
\i tables/countries.sql
\i tables/timezones.sql
COMMIT;

BEGIN;
-- Lookup tables for legacy OPENAQ
\i tables/sources_from_openaq_v1.sql
COMMIT;

BEGIN;
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
\i tables/rejects.sql
\i tables/versions.sql
COMMIT;

BEGIN;
-- Load immutable views/functions
\i refresh_idempotent.sql
COMMIT;
