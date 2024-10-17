-- flagging is a way that we can provide more detail about the data we collect
-- flagging data could be imported automatically via ingestion or
-- from user audit methods

-- suggesting the type instead of the table for a few reasons
  -- 1) I dont expect these to ever change
  -- 2) dont need descriptions and and could just use the type directly instead of having a `invalidates` field
  -- 3) I want to create special functions for each type and dont want to depend on a flag_levels_id
CREATE TYPE flag_level AS ENUM(
    'INFO'
  , 'WARNING'
  , 'ERROR'
  );

-- info, warning, error
CREATE SEQUENCE IF NOT EXISTS flag_types_sq START 10;
CREATE TABLE IF NOT EXISTS flag_types (
  flag_types_id int PRIMARY KEY DEFAULT nextval('flag_types_sq')
  , flag_level flag_level NOT NULL
  , label text NOT NULL
  , description text
  , ingest_id text NOT NULL UNIQUE
);


INSERT INTO flag_types (flag_types_id, flag_level, label, description, ingest_id) VALUES
  (1, 'INFO', 'Read me', 'important information to pass on to the user', 'info')
, (2, 'WARNING', 'Settings updated', 'Suggests to the user that the data may have issues', 'settings_changed')
, (3, 'ERROR', 'Error', 'Unknown error', 'error')
, (4, 'ERROR', 'Limits exceeded', 'The lower or upper limits of the instrument were exceeded', 'exceedance')
ON CONFLICT (flag_types_id) DO UPDATE
SET label = EXCLUDED.label
, flag_level = EXCLUDED.flag_level
, description = EXCLUDED.description
, ingest_id = EXCLUDED.ingest_id;


-- Measurements will be flagged at the station level
-- with the option to specifiy which sensors the flag affects
-- Other options are to do the flagging at the sensor level but that could
-- be laborious for errors like `power out` and others that affect everything
-- Or we could just do the station level but that has issues when we want to
-- invalidate just one part of the station (temp but not wind speed) and
-- leave the rest of the measurements intact
-- as for storing the parameters we could do the array method, which is easy ui
-- but lacks constraints/checks
-- or the child table which has checks and constraints but more involved ui
-- choosing array with a trigger to check the parameter ids
CREATE SEQUENCE IF NOT EXISTS flags_sq START 10;
CREATE TABLE IF NOT EXISTS flags (
  flags_id int PRIMARY KEY DEFAULT nextval('flags_sq')
  , flag_types_id int NOT NULL REFERENCES flag_types
  , sensor_nodes_id int NOT NULL REFERENCES sensor_nodes
  , period tstzrange NOT NULL
  , sensors_ids int[] --NOT NULL DEFAULT '{}'::int[]
  , note text
  , added_on timestamptz DEFAULT now()
  , modified_on timestamptz
);

CREATE INDEX flags_period_idx ON flags USING GiST (period);


-- CREATE OR REPLACE FUNCTION check_flags() RETURNS TRIGGER AS $$
-- DECLARE
--   measurands_check boolean;
-- BEGIN
--   IF NEW.measurands_id IS NOT NULL THEN
--     -- -- check against current ids
--     SELECT NEW.measurands_id <@ array_agg(measurands_id)
--     INTO measurands_check
--     FROM measurands;
--     IF NOT measurands_check THEN
--         RAISE EXCEPTION 'Measurand not found';
--     END IF;
--   END IF;
--   RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;


-- DROP TRIGGER IF EXISTS check_flags_tgr ON flags;
-- CREATE TRIGGER check_flags_tgr
-- BEFORE INSERT OR UPDATE ON flags
-- FOR EACH ROW EXECUTE PROCEDURE check_flags();


-- A few functions that will let check if a sensor/node + period has a flag
CREATE OR REPLACE FUNCTION sensor_node_flags_exist(int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  WHERE f.sensor_nodes_id = $1
  AND tstzrange(LEAST($2, $2 + $3), GREATEST($2, $2 + $3), '[]') && f.period)
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sensor_flags_exist(int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN sensor_systems sy ON (f.sensor_nodes_id = sy.sensor_nodes_id)
  JOIN sensors s ON (sy.sensor_systems_id = s.sensor_systems_id)
  WHERE s.sensors_id = $1
  AND (sensors_ids IS NULL OR ARRAY[$1] @> sensors_ids)
  AND tstzrange(LEAST($2, $2 + $3), GREATEST($2, $2 + $3), '[]') && f.period)
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION flags_exist(int, int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN sensor_systems sy ON (f.sensor_nodes_id = sy.sensor_nodes_id)
  JOIN sensors s ON (sy.sensor_systems_id = s.sensor_systems_id)
  WHERE f.sensor_nodes_id = $1
  AND ((sensors_ids IS NULL AND $2 IS NULL) OR ARRAY[$2] @> sensors_ids)
  AND tstzrange(LEAST($3, $3 + $4), GREATEST($3, $3 + $4), '[]') && f.period)
  $$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION sensor_node_info_flags_exist(int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  WHERE f.sensor_nodes_id = $1
  AND ft.flag_level = 'INFO'::flag_level
  AND tstzrange(LEAST($2, $2 + $3), GREATEST($2, $2 + $3), '[]') && f.period)
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sensor_info_flags_exist(int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  JOIN sensor_systems sy ON (f.sensor_nodes_id = sy.sensor_nodes_id)
  JOIN sensors s ON (sy.sensor_systems_id = s.sensor_systems_id)
  WHERE s.sensors_id = $1
  AND ft.flag_level = 'INFO'::flag_level
  AND (sensors_ids IS NULL OR ARRAY[$1] @> sensors_ids)
  AND tstzrange(LEAST($2, $2 + $3), GREATEST($2, $2 + $3), '[]') && f.period)
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION info_flags_exist(int, int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  JOIN sensor_systems sy ON (f.sensor_nodes_id = sy.sensor_nodes_id)
  JOIN sensors s ON (sy.sensor_systems_id = s.sensor_systems_id)
  WHERE f.sensor_nodes_id = $1
  AND ft.flag_level = 'INFO'::flag_level
  AND ((sensors_ids IS NULL AND $2 IS NULL) OR ARRAY[$2] @> sensors_ids)
  AND tstzrange(LEAST($3, $3 + $4), GREATEST($3, $3 + $4), '[]') && f.period)
  $$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION sensor_node_warning_flags_exist(int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  WHERE f.sensor_nodes_id = $1
  AND ft.flag_level = 'WARNING'::flag_level
  AND tstzrange(LEAST($2, $2 + $3), GREATEST($2, $2 + $3), '[]') && f.period)
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sensor_warning_flags_exist(int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  JOIN sensor_systems sy ON (f.sensor_nodes_id = sy.sensor_nodes_id)
  JOIN sensors s ON (sy.sensor_systems_id = s.sensor_systems_id)
  WHERE s.sensors_id = $1
  AND ft.flag_level = 'WARNING'::flag_level
  AND (sensors_ids IS NULL OR ARRAY[$1] @> sensors_ids)
  AND tstzrange(LEAST($2, $2 + $3), GREATEST($2, $2 + $3), '[]') && f.period)
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION warning_flags_exist(int, int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  JOIN sensor_systems sy ON (f.sensor_nodes_id = sy.sensor_nodes_id)
  JOIN sensors s ON (sy.sensor_systems_id = s.sensor_systems_id)
  WHERE f.sensor_nodes_id = $1
  AND ft.flag_level = 'WARNING'::flag_level
  AND ((sensors_ids IS NULL AND $2 IS NULL) OR ARRAY[$2] @> sensors_ids)
  AND tstzrange(LEAST($3, $3 + $4), GREATEST($3, $3 + $4), '[]') && f.period)
  $$ LANGUAGE SQL;

  CREATE OR REPLACE FUNCTION sensor_node_error_flags_exist(int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  WHERE f.sensor_nodes_id = $1
  AND ft.flag_level = 'ERROR'::flag_level
  AND tstzrange(LEAST($2, $2 + $3), GREATEST($2, $2 + $3), '[]') && f.period)
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sensor_error_flags_exist(int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  JOIN sensor_systems sy ON (f.sensor_nodes_id = sy.sensor_nodes_id)
  JOIN sensors s ON (sy.sensor_systems_id = s.sensor_systems_id)
  WHERE s.sensors_id = $1
  AND ft.flag_level = 'ERROR'::flag_level
  AND (sensors_ids IS NULL OR ARRAY[$1] @> sensors_ids)
  AND tstzrange(LEAST($2, $2 + $3), GREATEST($2, $2 + $3), '[]') && f.period)
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION error_flags_exist(int, int, timestamptz, interval DEFAULT '-1h') RETURNS bool AS $$
SELECT EXISTS(SELECT 1
  FROM flags f
  JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
  JOIN sensor_systems sy ON (f.sensor_nodes_id = sy.sensor_nodes_id)
  JOIN sensors s ON (sy.sensor_systems_id = s.sensor_systems_id)
  WHERE f.sensor_nodes_id = $1
  AND ft.flag_level = 'ERROR'::flag_level
  AND ((sensors_ids IS NULL AND $2 IS NULL) OR ARRAY[$2] @> sensors_ids)
  AND tstzrange(LEAST($3, $3 + $4), GREATEST($3, $3 + $4), '[]') && f.period)
  $$ LANGUAGE SQL;



  SELECT t.*
  -- Flags exist over hour (defaults to hourly)
  , flags_exist(sensor_nodes_id, sensors_id, datetime)
  , sensor_node_flags_exist(sensor_nodes_id, datetime)
  , sensor_flags_exist(sensors_id, datetime)
  -- INFO flags exist
  , info_flags_exist(sensor_nodes_id, sensors_id, datetime)
  , sensor_node_info_flags_exist(sensor_nodes_id, datetime)
  , sensor_info_flags_exist(sensors_id, datetime)
  -- WARNNING flags exist
  , warning_flags_exist(sensor_nodes_id, sensors_id, datetime)
  , sensor_node_warning_flags_exist(sensor_nodes_id, datetime)
  , sensor_warning_flags_exist(sensors_id, datetime)
  -- ERROR flags exists
  , error_flags_exist(sensor_nodes_id, sensors_id, datetime)
  , sensor_node_error_flags_exist(sensor_nodes_id, datetime)
  , sensor_error_flags_exist(sensors_id, datetime)
  -- Daily flags exist
  , flags_exist(sensor_nodes_id, sensors_id, datetime, '1d'::interval)
  FROM (VALUES
  (1, 1, '2024-01-01'::timestamptz)
  , (1, NULL, '2024-01-01'::timestamptz)
  , (2, NULL, '2024-01-03'::timestamptz)
  , (2, 3, '2024-01-03'::timestamptz)
  , (2, 4, '2024-01-03'::timestamptz)
  )as t(sensor_nodes_id, sensors_id, datetime);
