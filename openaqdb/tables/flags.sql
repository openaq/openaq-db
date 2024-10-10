-- flagging is a way that we can provide more detail about the data we collect
-- flagging data could be imported automatically via ingestion or
-- from user audit methods
CREATE SEQUENCE IF NOT EXISTS flag_levels_sq START 10;
CREATE TABLE IF NOT EXISTS flag_levels (
  flag_levels_id int PRIMARY KEY DEFAULT nextval('flag_levels_sq')
  , label text NOT NULL UNIQUE
  , description text
  , invalidates boolean DEFAULT false
);

INSERT INTO flag_levels (flag_levels_id, label, description, invalidates) VALUES
  (1, 'INFO', 'Used to provide information', false)
, (2, 'WARNING', 'Suggests to the user that the data may have issues', false)
, (3, 'ERROR', 'Specifies that the data is erroneous in some way', true)
ON CONFLICT (flag_levels_id) DO UPDATE
SET label = EXCLUDED.label
, description = EXCLUDED.description
, invalidates = EXCLUDED.invalidates;


-- info, warning, error
CREATE SEQUENCE IF NOT EXISTS flags_sq START 10;
CREATE TABLE IF NOT EXISTS flags (
  flags_id int PRIMARY KEY DEFAULT nextval('flags_sq')
  , flag_levels_id int NOT NULL REFERENCES flag_levels
  , label text NOT NULL
  , description text
  , ingest_id text NOT NULL UNIQUE
);

INSERT INTO flags (flags_id, flag_levels_id, label, description, ingest_id) VALUES
  (1, 1, 'Read me', 'important information to pass on to the user', 'info')
, (2, 2, 'Settings updated', 'Suggests to the user that the data may have issues', 'settings_changed')
, (3, 3, 'Limits exceeded', 'The lower or upper limits of the instrument were exceeded', 'exceedance')
ON CONFLICT (flags_id) DO UPDATE
SET label = EXCLUDED.label
, flag_levels_id = EXCLUDED.flag_levels_id
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
CREATE SEQUENCE IF NOT EXISTS flagged_measurements_sq START 10;
CREATE TABLE IF NOT EXISTS flagged_measurements (
  flagged_measurements_id int PRIMARY KEY DEFAULT nextval('flagged_measurements_sq')
  , flags_id int NOT NULL REFERENCES flags
  , sensor_nodes_id int NOT NULL REFERENCES sensor_nodes
  , period tstzrange NOT NULL
  , sensors_ids int[] --NOT NULL DEFAULT '{}'::int[]
  , note text
);

CREATE INDEX flagged_measurements_period_idx ON flagged_measurements USING GiST (period);


-- CREATE OR REPLACE FUNCTION check_flagged_measurements() RETURNS TRIGGER AS $$
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


-- DROP TRIGGER IF EXISTS check_flagged_measurements_tgr ON flagged_measurements;
-- CREATE TRIGGER check_flagged_measurements_tgr
-- BEFORE INSERT OR UPDATE ON flagged_measurements
-- FOR EACH ROW EXECUTE PROCEDURE check_flagged_measurements();
