SET search_path = public;
-- A simple table to manage open data exports
-- includes some extra information just for qa/qc
CREATE SEQUENCE IF NOT EXISTS open_data_export_logs_sq START 10;
CREATE TABLE IF NOT EXISTS open_data_export_logs (
  open_data_export_logs_id int PRIMARY KEY DEFAULT nextval('open_data_export_logs_sq')
  , sensor_nodes_id int NOT NULL REFERENCES sensor_nodes ON DELETE CASCADE
  , day date NOT NULL
--  , utc_offset interval NOT NULL            -- how many hours from the server time (utc?)
  , records int NOT NULL                   -- how many entries do we have for this location/date
  , measurands int NOT NULL                -- how many unique measurands exist
  , modified_on timestamptz DEFAULT now()  -- when was this date last modidified
  , queued_on timestamptz                  -- when did we last queue up a change
  , exported_on timestamptz                -- and when did we last finish exporting
  , metadata json
  , UNIQUE(sensor_nodes_id, day)
);


CREATE SEQUENCE IF NOT EXISTS providers_sq START 10;
CREATE TABLE IF NOT EXISTS providers (
  providers_id int PRIMARY KEY DEFAULT nextval('providers_sq')
  , label text NOT NULL UNIQUE
  , description text
  -- relates to the sensor_nodes table
  -- in the future we should link the providers_id directly to sensor_nodes
  , source_name text NOT NULL --REFERENCES sensor_nodes(source_name)
  -- the text to use as the root folder in the export method
  , export_prefix text NOT NULL
  , metadata jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS providers_source_name_idx ON providers(source_name);
