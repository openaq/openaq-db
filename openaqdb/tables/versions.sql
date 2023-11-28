
DROP TABLE IF EXISTS life_cycles CASCADE;
DROP TABLE IF EXISTS versions CASCADE;

ALTER TABLE sensors
	DROP CONSTRAINT sensors_sensor_systems_id_measurands_id_key;

-- Create a table to hold the life cycle values
CREATE TABLE IF NOT EXISTS life_cycles (
  life_cycles_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY
  , label text NOT NULL UNIQUE
  , short_code text NOT NULL UNIQUE -- used for ingest matching
  , sort_order int NOT NULL
  , readme text
);

-- List from the CAC science team
-- The descriptions come from CAC science team but are not formal descriptions
INSERT INTO life_cycles (
  label
, short_code
, sort_order
, readme
) VALUES
  ('Unverified', 'u', 1, 'human readable measurement value but not validated by anyone; pre-QA/QC data')
, ('Verified', 'q', 2, 'validated (post-QA/QC) data')
, ('Derived', 'd', 3, 'data having been thru some processing but not quite the end result; intermediate results')
, ('Cleansed', 'c', 4, 'post-QA/QC plus enriched by correcting bad data, filling missing data, etc.')
, ('Analysis result', 'a', 5, 'final result of an analysis')
ON CONFLICT(label) DO UPDATE
SET sort_order = EXCLUDED.sort_order
, readme = EXCLUDED.readme;


-- Versions table will be used
CREATE TABLE IF NOT EXISTS versions (
     versions_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY
     , sensors_id int NOT NULL REFERENCES sensors ON DELETE CASCADE
     , parent_sensors_id int NOT NULL REFERENCES sensors ON DELETE CASCADE
     , life_cycles_id int NOT NULL REFERENCES life_cycles
     , version_date date NOT NULL DEFAULT current_date
     , readme text
     , metadata jsonb
     , added_on timestamp DEFAULT now()
     , UNIQUE(sensors_id, life_cycles_id, version_date)
     );

-- update the reject table to include a default time
-- this needs to be done as the postgres user
DO $$
BEGIN
  ALTER TABLE rejects
  ALTER COLUMN t SET DEFAULT now();
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Failed to set default';
END
$$;

DO $$
BEGIN
  ALTER TABLE rejects
  ADD COLUMN reason text;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Failed to add reason, could exist';
END
$$;


-- Create a query that will put all of this together
DROP VIEW IF EXISTS version_ranks CASCADE;
CREATE OR REPLACE VIEW version_ranks AS
SELECT versions_id
, v.parent_sensors_id
, v.sensors_id
, lc.life_cycles_id
, v.version_date
, lc.sort_order
, lc.label
, row_number() OVER (
  PARTITION BY parent_sensors_id
  ORDER BY v.version_date DESC, lc.sort_order DESC
) as version_rank
FROM versions v
JOIN life_cycles lc USING (life_cycles_id);

CREATE OR REPLACE VIEW versions_view AS
SELECT r.versions_id
, r.sensors_id
, r.parent_sensors_id
, r.life_cycles_id
, s.source_id as sensor
, p.source_id as parent_sensor
, r.version_date
, r.label as life_cycle
, r.version_rank
FROM version_ranks r
JOIN sensors s ON (s.sensors_id = r.sensors_id)
JOIN sensors p ON (p.sensors_id = r.parent_sensors_id);


CREATE OR REPLACE VIEW stale_versions AS
SELECT sensors_id
FROM version_ranks
WHERE version_rank > 1
UNION ALL
SELECT parent_sensors_id
FROM versions;
