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
