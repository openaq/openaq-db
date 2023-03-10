-- A table to hold the spatial rollups
CREATE TABLE IF NOT EXISTS sensor_nodes_spatial_rollup (
    sensor_nodes_id int NOT NULL REFERENCES sensor_nodes
  , geom geometry
  , cell_size double precision
  , start_datetime timestamptz
  , end_datetime timestamptz
  , measurements_count int
  , added_on timestamptz DEFAULT now()
  , modified_on timestamptz
  , UNIQUE(sensor_nodes_id, geom)
);
CREATE INDEX ON sensor_nodes_spatial_rollup (sensor_nodes_id);
CREATE INDEX ON sensor_nodes_spatial_rollup USING GIST (geom, sensor_nodes_id);




CREATE OR REPLACE FUNCTION upsert_sensor_nodes_spatial_rollups_by_sensor(int, double precision DEFAULT 30.0) RETURNS int AS $$
INSERT INTO sensor_nodes_spatial_rollup (
sensor_nodes_id
, geom
, cell_size
, start_datetime
, end_datetime
, measurements_count
, added_on)
SELECT sensor_nodes_id
, st_snaptogrid(pt3857(lon, lat), $2)
, $2
, MIN(datetime) as start_datetime
, MAX(datetime) as end_datetime
, COUNT(DISTINCT datetime) as measurements
, now()
FROM measurements m
JOIN sensors s USING (sensors_id)
JOIN sensor_systems ss USING (sensor_systems_id)
WHERE sensors_id = $1
GROUP BY 1,2
ON CONFLICT (sensor_nodes_id, geom) DO UPDATE SET
  start_datetime = LEAST(sensor_nodes_spatial_rollup.start_datetime, EXCLUDED.start_datetime)
, end_datetime = GREATEST(sensor_nodes_spatial_rollup.end_datetime, EXCLUDED.end_datetime)
, measurements_count = sensor_nodes_spatial_rollup.measurements_count + EXCLUDED.measurements_count
, modified_on = now()
RETURNING 1
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION upsert_sensor_nodes_spatial_rollups_by_sensor(int, double precision) RETURNS int AS $$
WITH updates AS (
INSERT INTO sensor_nodes_spatial_rollup (
sensor_nodes_id
, geom
, cell_size
, start_datetime
, end_datetime
, measurements_count
, added_on)
SELECT sensor_nodes_id
, st_snaptogrid(pt3857(lon, lat), $2)
, $2
, MIN(datetime) as start_datetime
, MAX(datetime) as end_datetime
, COUNT(DISTINCT datetime) as measurements
, now()
FROM measurements m
JOIN sensors s USING (sensors_id)
JOIN sensor_systems ss USING (sensor_systems_id)
WHERE sensors_id = $1
GROUP BY 1,2
ON CONFLICT (sensor_nodes_id, geom) DO UPDATE SET
  start_datetime = EXCLUDED.start_datetime
, end_datetime = EXCLUDED.end_datetime
, measurements_count = EXCLUDED.measurements_count
, modified_on = now()
RETURNING 1)
SELECT COUNT(1) FROM updates;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION upsert_sensor_nodes_spatial_rollups_by_node(int, double precision) RETURNS int AS $$
WITH updates AS (
INSERT INTO sensor_nodes_spatial_rollup (
  sensor_nodes_id
, geom
, cell_size
, start_datetime
, end_datetime
, measurements_count
, added_on)
SELECT sensor_nodes_id
, st_snaptogrid(pt3857(lon, lat), $2)
, $2
, MIN(datetime) as start_datetime
, MAX(datetime) as end_datetime
, COUNT(DISTINCT datetime) as measurements
, now()
FROM measurements m
JOIN sensors s USING (sensors_id)
JOIN sensor_systems ss USING (sensor_systems_id)
WHERE sensor_nodes_id = $1
GROUP BY 1,2
ON CONFLICT (sensor_nodes_id, geom) DO UPDATE SET
  start_datetime = EXCLUDED.start_datetime
, end_datetime = EXCLUDED.end_datetime
, measurements_count = EXCLUDED.measurements_count
, modified_on = now()
RETURNING 1)
SELECT COUNT(1) FROM updates;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION upsert_sensor_nodes_spatial_rollups(int) RETURNS int AS $$
WITH updates AS (
INSERT INTO sensor_nodes_spatial_rollup (
  sensor_nodes_id
, geom
, cell_size
, start_datetime
, end_datetime
, measurements_count
, added_on)
SELECT sensor_nodes_id
, st_snaptogrid(pt3857(lon, lat), $1)
, $1
, MIN(datetime) as start_datetime
, MAX(datetime) as end_datetime
, COUNT(DISTINCT datetime) as measurements
, now()
FROM measurements m
JOIN sensors s USING (sensors_id)
JOIN sensor_systems ss USING (sensor_systems_id)
WHERE m.lat IS NOT NULL AND m.lon IS NOT NULL
GROUP BY 1,2
ON CONFLICT (sensor_nodes_id, geom) DO UPDATE SET
  start_datetime = EXCLUDED.start_datetime
, end_datetime = EXCLUDED.end_datetime
, measurements_count = EXCLUDED.measurements_count
, modified_on = now()
RETURNING 1)
SELECT COUNT(1) FROM updates;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION upsert_sensor_nodes_spatial_rollups_latest(double precision) RETURNS int AS $$
WITH updates AS (
INSERT INTO sensor_nodes_spatial_rollup (
  sensor_nodes_id
, geom
, cell_size
, start_datetime
, end_datetime
, measurements_count
, added_on)
SELECT sensor_nodes_id
, st_snaptogrid(pt3857(lon, lat), $1)
, $1
, MIN(datetime) as start_datetime
, MAX(datetime) as end_datetime
, COUNT(DISTINCT datetime) as measurements
, now()
FROM sensors_latest m
JOIN sensors s USING (sensors_id)
JOIN sensor_systems ss USING (sensor_systems_id)
JOIN sensor_nodes sn USING (sensor_nodes_id)
WHERE m.lat IS NOT NULL AND m.lon IS NOT NULL
GROUP BY 1,2
ON CONFLICT (sensor_nodes_id, geom) DO UPDATE SET
  start_datetime = EXCLUDED.start_datetime
, end_datetime = EXCLUDED.end_datetime
, measurements_count = EXCLUDED.measurements_count
, modified_on = now()
RETURNING 1)
SELECT COUNT(1)
FROM updates;
$$ LANGUAGE SQL;


SELECT upsert_sensor_nodes_spatial_rollups_by_sensor(126656, 35.0);

SELECT upsert_sensor_nodes_spatial_rollups_by_node(24867, 30.0);

--SELECT upsert_sensor_nodes_spatial_rollups_latest(30.0);


-- sandbox
\set sensors_id 2957942

CREATE TEMP TABLE IF NOT EXISTS temp_inserted_measurements (
  sensors_id int
  , datetime timestamptz
  , value double precision
  , lat double precision
  , lon double precision
  , fetchlogs_id int
);


INSERT INTO temp_inserted_measurements (sensors_id, datetime, value, lat, lon)
SELECT sensors_id
, datetime
, value
, lat
, lon
FROM measurements
WHERE sensors_id = :sensors_id;



\set sensors_id 41234






SELECT upsert_sensor_nodes_spatial_rollups_by_sensor(:sensors_id, 250.0);

SELECT COUNT(1)
, MIN(start_datetime)
, MAX(start_datetime)
, MAX(measurements_count)
, AVG(measurements_count)
FROM sensor_nodes_spatial_rollup;


TRUNCATE sensor_nodes_spatial_rollup;
