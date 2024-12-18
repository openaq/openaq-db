
DROP TABLE IF EXISTS sensor_statuses CASCADE;
DROP TABLE IF EXISTS versions CASCADE;


-- Create a table to hold the life cycle values
CREATE TABLE IF NOT EXISTS sensor_statuses (
  sensor_statuses_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY
  , label text NOT NULL UNIQUE
  , short_code text NOT NULL UNIQUE -- used for ingest matching
  , sort_order int NOT NULL
  , readme text
);

-- List from the CAC science team
-- The descriptions come from CAC science team but are not formal descriptions
INSERT INTO sensor_statuses (
  label
, short_code
, sort_order
, readme
) VALUES
  ('Unreviewed', 'u', 1, 'Human readable measurement value straight from the instrument')
, ('Reviewed', 'r', 2, 'Data has been reviewed in some way and erroneous values have been removed')
, ('Modified', 'm', 3, 'Data has been reviewed and some data has been modified in some way, filling missing data, etc.')
ON CONFLICT(label) DO UPDATE
SET sort_order = EXCLUDED.sort_order
, readme = EXCLUDED.readme;

ALTER TABLE sensors
   ADD COLUMN sensor_statuses_id int NOT NULL DEFAULT 1 REFERENCES sensor_statuses;



-- Versions table will be used
CREATE TABLE IF NOT EXISTS versions (
     versions_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY
     , sensors_id int NOT NULL REFERENCES sensors ON DELETE CASCADE
     , parent_sensors_id int NOT NULL REFERENCES sensors ON DELETE CASCADE
     , version_date date NOT NULL DEFAULT current_date
     , readme text
     , metadata jsonb
     , added_on timestamp DEFAULT now()
      -- A sensor can only have one parent and therefor must be unique
     , UNIQUE(sensors_id)
      -- And we should limit to one version per parent + date
     , UNIQUE(parent_sensors_id, version_date)
     );



-- Create a query that will put all of this together
DROP VIEW IF EXISTS version_ranks CASCADE;
CREATE OR REPLACE VIEW version_ranks AS
SELECT versions_id
, v.parent_sensors_id
, v.sensors_id
, lc.sensor_statuses_id
, v.version_date
, lc.sort_order
, lc.label
, row_number() OVER (
  PARTITION BY parent_sensors_id
  ORDER BY v.version_date DESC, lc.sort_order DESC
) as version_rank
FROM versions v
JOIN sensors s ON (v.sensors_id = s.sensors_id)
JOIN sensor_statuses lc ON (s.sensor_statuses_id = lc.sensor_statuses_id);


CREATE OR REPLACE VIEW versions_view AS
SELECT r.versions_id
, r.sensors_id
, r.parent_sensors_id
, r.sensor_statuses_id
, s.source_id as sensor
, p.source_id as parent_sensor
, r.version_date
, r.label as life_cycle
, r.version_rank
, sy.sensor_nodes_id
FROM version_ranks r
JOIN sensors s ON (s.sensors_id = r.sensors_id)
JOIN sensors p ON (p.sensors_id = r.parent_sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id);


CREATE OR REPLACE VIEW stale_versions AS
SELECT sensors_id
FROM version_ranks
WHERE version_rank > 1
UNION ALL
SELECT parent_sensors_id
FROM versions;
