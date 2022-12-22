-- sensors nodes updates
-- pull

-- Sensors table updates
INSERT INTO users (users_id, email_address, ip_address) VALUES
(1, 'admin@openaq.org', '127.0.0.1')
ON CONFLICT DO NOTHING;


DO $$
BEGIN
  ALTER TABLE sensors
  ADD COLUMN data_averaging_period_seconds int
  , ADD COLUMN data_logging_period_seconds int;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'sensors alter error';
    --END;
END$$;

-- not going to work on hypertables
-- DO $$
-- BEGIN
--   ALTER TABLE measurements
--   ADD COLUMN added_on timestamptz DEFAULT now();
-- EXCEPTION WHEN OTHERS THEN
--     RAISE NOTICE 'measurements alter error';
--     --END;
-- END$$;


DO $$
BEGIN
  ALTER TABLE sensor_nodes
  ADD COLUMN added_on timestamptz DEFAULT now(),
  ADD COLUMN modified_on timestamptz,
  ADD COLUMN timezones_id int REFERENCES timezones(gid);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'sensors alter error';
    --END;
END$$;

DO $$
BEGIN
  ALTER TABLE sensor_systems
  ADD COLUMN added_on timestamptz DEFAULT now(),
  ADD COLUMN modified_on timestamptz;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'sensors alter error';
    --END;
END$$;

DO $$
BEGIN
  ALTER TABLE sensors
  ADD COLUMN added_on timestamptz DEFAULT now(),
  ADD COLUMN modified_on timestamptz;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'sensors alter error';
    --END;
END$$;



COMMENT ON COLUMN sensors.data_averaging_period_seconds IS
'The number of seconds that is averaged in each measurement';

COMMENT ON COLUMN sensors.data_logging_period_seconds IS
'How often we should expect a measurement';


-- Tables
-- lookup table for the sensor types
CREATE SEQUENCE IF NOT EXISTS sensor_types_sq START 10;
CREATE TABLE IF NOT EXISTS sensor_types (
  sensor_types_id int PRIMARY KEY DEFAULT nextval('sensor_types_sq')
  , label text NOT NULL UNIQUE
  , is_reference bool DEFAULT false
);
COMMENT ON TABLE sensor_types IS
'Used to classify the different sensors';
COMMENT ON COLUMN sensor_types.is_reference IS
'Is the sensor considered to be a reference or regulatory grade sensor';


CREATE SEQUENCE IF NOT EXISTS entities_sq START 10;
CREATE TABLE IF NOT EXISTS entities (
  entities_id int PRIMARY KEY DEFAULT nextval('entities_sq')
  , label text NOT NULL UNIQUE
);
COMMENT ON TABLE entities IS
'';


-- units
CREATE SEQUENCE IF NOT EXISTS units_sq START 10;
CREATE TABLE IF NOT EXISTS units (
  units_id int PRIMARY KEY DEFAULT nextval('units_sq')
  , display_short text NOT NULL UNIQUE
  , display_long text NOT NULL UNIQUE
);

-- this will take some thought in how and where its done
-- for example, it might be easiest to do at the application level
-- in which case we just reference the method here
-- or maybe at the databse level and we reference the db function
CREATE SEQUENCE IF NOT EXISTS unit_conversions_sq START 10;
CREATE TABLE IF NOT EXISTS unit_conversions (
  unit_conversions_id int PRIMARY KEY DEFAULT nextval('unit_conversions_sq')
  , from_units_id int NOT NULL REFERENCES units(units_id) ON DELETE CASCADE
  , to_units_id int NOT NULL REFERENCES units(units_id) ON DELETE CASCADE
  , conversion_function text NOT NULL
  , UNIQUE(from_units_id, to_units_id)
);

-- instruments
-- a list of all possible instruments
CREATE SEQUENCE IF NOT EXISTS instruments_sq START 10;
CREATE TABLE IF NOT EXISTS instruments (
  instruments_id int PRIMARY KEY DEFAULT nextval('instruments_sq')
  , manufacturer_entities_id int NOT NULL REFERENCES entities
  , label text NOT NULL UNIQUE
  , description text
);

-- models
CREATE SEQUENCE IF NOT EXISTS models_sq START 10;
CREATE TABLE IF NOT EXISTS models (
  models_id int PRIMARY KEY DEFAULT nextval('models_sq')
  , instruments_id int NOT NULL REFERENCES instruments
  , label text NOT NULL
  , UNIQUE(instruments_id, label)
);


-- instrument model sensors
-- the data channels that are associated with a given instrument model
CREATE SEQUENCE IF NOT EXISTS models_sensors_sq START 10;
CREATE TABLE IF NOT EXISTS models_sensors (
  models_sensors_id int PRIMARY KEY DEFAULT nextval('models_sensors_sq')
  , models_id int NOT NULL REFERENCES models
  , sensor_types_id int NOT NULL REFERENCES sensor_types
  , measurands_id int NOT NULL REFERENCES measurands
  , units_id int NOT NULL REFERENCES units
  , UNIQUE(models_id, measurands_id)
);

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
);

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
  , measurands_id int[] NOT NULL DEFAULT '{}'::int[]
  , description text
);



CREATE OR REPLACE FUNCTION check_flagged_measurements() RETURNS TRIGGER AS $$
DECLARE
  measurands_check boolean;
BEGIN
  IF NEW.measurands_id IS NOT NULL THEN
    -- -- check against current ids
    SELECT NEW.measurands_id <@ array_agg(measurands_id)
    INTO measurands_check
    FROM measurands;
    IF NOT measurands_check THEN
        RAISE EXCEPTION 'Measurand not found';
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS check_flagged_measurements_tgr ON flagged_measurements;
CREATE TRIGGER check_flagged_measurements_tgr
BEFORE INSERT OR UPDATE ON flagged_measurements
FOR EACH ROW EXECUTE PROCEDURE check_flagged_measurements();

-----------
-- VIEWS --
-----------
-- manufacturers
-- a list of all possible manufactures of instruments
-- a view that pulls from instruments
CREATE OR REPLACE VIEW manufacturers AS
SELECT entities_id
, full_name
, m.instruments_count
FROM entities c
JOIN (SELECT manufacturer_entities_id
     , COUNT(1) as instruments_count
     -- could add instrument list here
     FROM instruments
     GROUP BY manufacturer_entities_id) as m
     ON (m.manufacturer_entities_id = c.entities_id);

-- use the instrument model sensors to get a list
-- of all possible sensor units

INSERT INTO entities (entities_id
, full_name
, entity_type) VALUES
(1, 'OpenAQ admin', 'Person'::entity_type)
ON CONFLICT DO NOTHING;

INSERT INTO instruments (instruments_id
, label
, description
, manufacturer_entities_id) VALUES
(1, 'N/A', 'Instrument is not available', 1)
ON CONFLICT DO NOTHING;


DO $$
BEGIN
  ALTER TABLE providers
  ADD COLUMN owner_entities_id int REFERENCES entities DEFAULT 1;
EXCEPTION WHEN OTHERS THEN
   RAISE NOTICE 'providers alter error';
END$$;

INSERT INTO providers (providers_id
, label
, description
, source_name
, export_prefix
, owner_entities_id) VALUES
(1, 'N/A', 'Provider is not available', 'na', 'na_', 1);

DO $$
BEGIN
  ALTER TABLE sensor_nodes
  ADD COLUMN providers_id int REFERENCES providers DEFAULT 1
  , ADD COLUMN countries_id int REFERENCES countries DEFAULT 1
  , ADD COLUMN owner_entities_id int REFERENCES entities DEFAULT 1;
EXCEPTION WHEN OTHERS THEN
   RAISE NOTICE 'sensor nodes alter error';
END$$;

DO $$
BEGIN
  ALTER TABLE sensor_systems
  ADD COLUMN instruments_id int REFERENCES instruments DEFAULT 1
  , ADD COLUMN deployed_by int REFERENCES entities DEFAULT 1
  , ADD COLUMN deployed_on date NOT NULL DEFAULT current_date;
EXCEPTION WHEN OTHERS THEN
   RAISE NOTICE 'sensor systems alter error';
END$$;
