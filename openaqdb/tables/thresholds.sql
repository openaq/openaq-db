-- Table to hold the list of thresholds that we will
-- need to calculate for each rollup
CREATE SEQUENCE IF NOT EXISTS thresholds_sq START 10;
CREATE TABLE IF NOT EXISTS thresholds (
  thresholds_id int PRIMARY KEY DEFAULT nextval('thresholds_sq')
  , measurands_id int NOT NULL REFERENCES measurands
  , value double precision NOT NULL
  , UNIQUE (measurands_id, value)
);

DROP TABLE IF EXISTS sensor_exceedances;
CREATE TABLE IF NOT EXISTS sensor_exceedances (
  sensors_id int NOT NULL REFERENCES sensors ON DELETE CASCADE
  , threshold_value double precision
  , datetime_latest timestamptz
  , updated_on timestamptz NOT NULL DEFAULT now()
  , UNIQUE(sensors_id, threshold_value)
);
-- add index
CREATE INDEX sensor_exceedances_sensors_id_idx ON sensor_exceedances USING btree (sensors_id);
CREATE INDEX sensor_exceedances_threshold_value ON sensor_exceedances USING btree (threshold_value);


-- a table to track the entities specific sets of thresholds
-- this will allow us to define groups of thresholds for display purposes
-- e.g. epa, who, other orgs
CREATE SEQUENCE IF NOT EXISTS entities_thresholds_sq START 10;
CREATE TABLE IF NOT EXISTS entities_thresholds (
  entities_thresholds_id int PRIMARY KEY DEFAULT nextval('entities_thresholds_sq')
  , entities_id int NOT NULL REFERENCES entities ON DELETE CASCADE
  , thresholds_id int NOT NULL REFERENCES thresholds ON DELETE CASCADE
  , UNIQUE(entities_id, thresholds_id)
);

-- this should be made into a normal table that we manage
-- because this is a lot of overhead for things that dont
-- need to be updated all the time
CREATE MATERIALIZED VIEW sensor_node_daily_exceedances AS
SELECT sy.sensor_nodes_id
, h.measurands_id
, date_trunc('day', datetime - '1sec'::interval) as day
, t.value as threshold_value
, SUM((h.value_avg >= t.value)::int) as exceedance_count
, SUM(value_count) as total_count
, COUNT(*) AS hourly_count
FROM hourly_data h
JOIN sensors s ON (h.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (sy.sensor_systems_id = s.sensor_systems_id)
JOIN thresholds t ON (t.measurands_id = h.measurands_id)
GROUP BY 1,2,3,4;

CREATE UNIQUE INDEX ON sensor_node_daily_exceedances (sensor_nodes_id, measurands_id, threshold_value, day);


-- This could stay a materialized view
-- because we will need to refresh the whole thing all the time
-- we could move the intervals to a table
CREATE MATERIALIZED VIEW sensor_node_range_exceedances AS
WITH intervals AS (
   SELECT UNNEST(ARRAY[1,14,30,90]) as days
)
SELECT sensor_nodes_id
, days
, measurands_id
, threshold_value
, SUM(exceedance_count) as exceedance_count
, SUM(total_count) as total_count
FROM sensor_node_daily_exceedances
, intervals
WHERE day > current_date - days
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX ON sensor_node_range_exceedances (sensor_nodes_id, measurands_id, threshold_value, days);
