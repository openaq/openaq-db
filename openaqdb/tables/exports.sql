SET search_path = public;
-- A simple table to manage open data exports
-- includes some extra information just for qa/qc
CREATE TABLE IF NOT EXISTS open_data_export_logs (
   sensor_nodes_id int NOT NULL REFERENCES sensor_nodes ON DELETE CASCADE
  , day date NOT NULL
--  , utc_offset interval NOT NULL         -- how many hours from the server time (utc?)
  , records int NOT NULL                   -- how many entries do we have for this location/date
  , measurands int NOT NULL                -- how many unique measurands exist
  , added_on timestamptz DEFAULT now()     -- when was this date first added
  , modified_on timestamptz DEFAULT now()  -- when was this date last modidified
  , queued_on timestamptz                  -- when did we last queue up a change
  , exported_on timestamptz                -- and when did we last finish exporting
  , has_error boolean DEFAULT 'f'
  , key text
  , version int
	, checked_on timestamptz
  , metadata json
  , UNIQUE(sensor_nodes_id, day)
);


-- CREATE INDEX IF NOT EXISTS export_logs_exported_on_idx
-- ON open_data_export_logs USING btree (exported_on);

-- CREATE INDEX IF NOT EXISTS export_logs_queued_on_idx
-- ON open_data_export_logs USING btree (queued_on);

-- CREATE INDEX IF NOT EXISTS export_logs_modified_on_idx
-- ON open_data_export_logs USING btree (modified_on);

-- CREATE INDEX IF NOT EXISTS export_logs_day_idx
-- ON open_data_export_logs USING btree (day);

-- CREATE INDEX IF NOT EXISTS export_logs_exported_on_is_null
-- ON open_data_export_logs (day) WHERE exported_on IS NULL;

-- CREATE INDEX IF NOT EXISTS export_logs_queued_on_is_null
-- ON open_data_export_logs (day) WHERE queued_on IS NULL;

-- CREATE INDEX IF NOT EXISTS export_logs_nodes_exported_on_is_null
-- ON open_data_export_logs (sensor_nodes_id) WHERE exported_on IS NULL;

-- CREATE INDEX IF NOT EXISTS export_logs_nodes_queued_on_is_null
-- ON open_data_export_logs (sensor_nodes_id) WHERE queued_on IS NULL;


-- DROP INDEX IF EXISTS export_logs_exported_on_idx
-- , export_logs_queued_on_idx
-- , export_logs_modified_on_idx
-- , export_logs_day_idx
-- , export_logs_exported_on_is_null
-- , export_logs_queued_on_is_null
-- , export_logs_nodes_exported_on_is_null
-- , export_logs_nodes_queued_on_is_null
-- ;


CREATE TABLE IF NOT EXISTS export_stats (
    stats_interval interval NOT NULL PRIMARY KEY -- only one right now
  , days_modified bigint
  , days_exported bigint
  , days_added bigint
  , days_pending bigint
  , calculated_on timestamptz DEFAULT now()
);

CREATE OR REPLACE FUNCTION calculate_export_stats(ci interval) RETURNS timestamptz AS $$
WITH m AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE modified_on > added_on
  AND modified_on > now() - ci
), a AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE added_on > now() - ci
), e AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE exported_on > now() - ci
), p AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE exported_on IS NULL
  AND has_error = FALSE
)
INSERT INTO export_stats (
    stats_interval
  , days_modified
  , days_added
  , days_exported
  , days_pending
  , calculated_on)
SELECT ci as stats_interval
, m.n as days_modified
, a.n as days_added
, e.n as days_exported
, p.n as days_pending
, now() as calculated_on
FROM m,a,e,p
ON CONFLICT (stats_interval) DO UPDATE
SET days_modified = EXCLUDED.days_modified
, days_added = EXCLUDED.days_added
, days_exported = EXCLUDED.days_exported
, days_pending = EXCLUDED.days_pending
, calculated_on = EXCLUDED.calculated_on
RETURNING calculated_on;
$$ LANGUAGE SQL;

--SELECT calculate_export_stats('1day');

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
  , license text
  , metadata jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS providers_source_name_idx ON providers(source_name);

CREATE OR REPLACE FUNCTION get_providers_id(p text)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT providers_id
FROM providers
WHERE source_name = p
LIMIT 1;
$$;

	CREATE TABLE IF NOT EXISTS providers_stats (
	providers_id int NOT NULL REFERENCES providers
, day date NOT NULL
, sensor_nodes_count int
, sensors_count int
, measurements_count int
, offset_min interval
, offset_avg interval
, offset_max interval
, datetime_min timestamptz
, datetime_max timestamptz
, added_on_min timestamptz
, added_on_max timestamptz
, UNIQUE(providers_id, day)
);
