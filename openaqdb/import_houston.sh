
# file format: csv
# fields
# ingest_id: houston_mobile-9fcb40a7-f737-49bd-8f4e-37654b50cb4e-pm25
# value: 9.6
# datetime: 2017-07-05 14:49:46+00
# lon: -95.407143
# lat: 29.719042


URL=postgres://$DATABASE_WRITE_USER:$DATABASE_WRITE_PASSWORD@localhost:5432/$DATABASE_DB
# s3 bucket and base location to use
BUCKET=s3://openaq-fetches/uploaded/measures/houston

# we are going to
# stream data into the database as a temp table
# then we are going to create/lookup the node to use
# and then import into the measurements
# add to the summary method


CREATE_SQL=$(cat <<-EOF
CREATE TABLE IF NOT EXISTS measurements_temp (
    ingest_id text
    , value double precision
    , datetime timestamptz
    , lon double precision
    , lat double precision
);
CREATE TABLE IF NOT EXISTS stations_temp (
    ingest_id text
    , source_name text
    , source_id text
    , parameter text
    , measurands_id int
    , sensors_id int
    , sensor_systems_id int
    , sensor_nodes_id int
);
EOF
                   )

INSERT_SQL=$(cat <<-EOF
SELECT COUNT(1) FROM measurements_temp
EOF
                   )



FILE="61507_00.csv.gz"

aws s3 cp "${BUCKET}/${FILE}" - \
    | gunzip -f -c \
    | psql $URL -c "$CREATE_SQL" \
           -c "copy measurements_temp from stdin DELIMITER ',' NULL ''" \
           -c "$INSERT_SQL"



INSERT INTO stations_temp (ingest

WITH stations AS (
  SELECT ingest_id
  FROM measurements_temp
  GROUP BY 1)
INSERT INTO stations_temp (ingest_id, source_name, source_id, parameter)
SELECT ingest_id
, split_ingest_id(ingest_id, 1) as source_name
, split_ingest_id(ingest_id, 2) as source_id
, split_ingest_id(ingest_id, 3) as parameter
FROM stations;

-- now we need to check and see if any of this exists
SELECT *
FROM sensors
WHERE sensors_id iN (SELECT sensors_id FROM sensor_nodes_check WHERE source_name = 'houston_mobile');
-- update the current list of sensors to match
UPDATE sensors
SET source_id = s.source_id||'-'||lower(s.parameter)
FROM sensor_nodes_check s
WHERE sensors.sensors_id = s.sensors_id
AND s.source_name = 'houston_mobile';


-- match the node
UPDATE stations_temp
SET sensor_nodes_id = n.sensor_nodes_id
FROM sensor_nodes n
WHERE stations_temp.source_name = n.source_name
AND stations_temp.source_id = n.source_id;

-- match the sensor
UPDATE stations_temp
SET sensors_id = s.sensors_id
FROM sensors s
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
WHERE sn.source_name = 'houston_mobile'
AND stations_temp.ingest_id = format('%s-%s', sn.source_name, s.source_id);

-- Now add any missing sensor nodes
WITH stations AS (
  INSERT INTO sensor_nodes (source_name, source_id, site_name, metadata)
  SELECT source_name
  , source_id
  , source_id
  , jsonb_build_object('imported', 'houston', 'ingest_id', ingest_id)
  FROM stations_temp
  WHERE sensor_nodes_id IS NULL
  ON CONFLICT (source_name, source_id) DO UPDATE
  SET metadata = EXCLUDED.metadata
  RETURNING sensor_nodes_id, metadata->>'ingest_id' as ingest_id)
UPDATE stations_temp
SET sensor_nodes_id = stations.sensor_nodes_id
FROM stations
WHERE stations.ingest_id = stations_temp.ingest_id;

-- do the same with systems
WITH systems AS (
  INSERT INTO sensor_systems (sensor_nodes_id, source_id, metadata)
  SELECT sensor_nodes_id
  , source_id
  , jsonb_build_object('imported', 'houston', 'ingest_id', format('%s-%s', source_name, source_id))
  FROM stations_temp
  WHERE sensor_systems_id IS NULL
  GROUP BY 1,2,3
  ON CONFLICT (sensor_nodes_id, source_id) DO UPDATE
  SET metadata = EXCLUDED.metadata
  RETURNING sensor_systems_id, sensor_nodes_id)
UPDATE stations_temp
SET sensor_systems_id = stations.sensor_systems_id
FROM systems
WHERE systems.sensor_nodes_id = stations_temp.sensor_nodes_id;

-- and the measurands
UPDATE stations_temp
SET measurands_id = m.measurands_id
FROM measurands m
WHERE m.measurands_id IN (2, 7, 10, 11, 21, 35, 130)
AND stations_temp.parameter = m.measurand;

-- and finally the sensors
WITH sens AS (
  INSERT INTO sensors (
    sensor_systems_id
    , source_id
    , measurands_id
    , data_averaging_period_seconds
    , data_logging_period_seconds
    , metadata)
  SELECT sensor_systems_id
  , format('%s-%s', t.source_id, t.parameter)
  , measurands_id
  , 1
  , 1
  , jsonb_build_object('imported', 'houston')
  FROM stations_temp t
  WHERE sensors_id IS NULL
  AND measurands_id IS NOT NULL
  ON CONFLICT (sensor_systems_id, measurands_id) DO UPDATE
  SET metadata = EXCLUDED.metadata
  RETURNING sensor_systems_id, measurands_id, sensors_id)
UPDATE stations_temp
SET sensors_id = sens.sensors_id
FROM sens
WHERE sens.sensor_systems_id = stations_temp.sensor_systems_id
AND sens.measurands_id = stations_temp.measurands_id;
-- And now we can add the data
INSERT INTO measurements (sensors_id, datetime, value, lon, lat)
SELECT t.sensors_id
, m.datetime
, m.value
, m.lon
, m.lat
FROM measurements_temp m
JOIN stations_temp t ON (m.ingest_id = t.ingest_id)
ON CONFLICT DO NOTHING;

-- and clean up
--DROP TABLE IF EXISTS measurements_temp, stations_temp;

WITH spatial AS (
SELECT t.sensor_nodes_id
, st_snaptogrid(pt3857(lon, lat), 50.0) as geom
, MIN(datetime) as start_datetime
, MAX(datetime) as end_datetime
, COUNT(DISTINCT datetime) as measurements
FROM measurements_temp m
JOIN stations_temp t USING (ingest_id)
GROUP BY 1,2)
SELECT sensor_nodes_id
, start_datetime
, end_datetime - start_datetime as dur
, measurements
, st_distancesphere(st_transform(geom, 4326), st_transform(LAG(geom) OVER(PARTITION BY sensor_nodes_id ORDER BY end_datetime), 4326)) as distance_m
, st_distance(geom, LAG(geom) OVER(PARTITION BY sensor_nodes_id ORDER BY end_datetime)) as distance
FROM spatial
ORDER BY 4 DESC
LIMIT 25;


SELECT
, COUNT(1)
, AVG(measurements)
, MIN(start_datetime)
, MAX(end_datetime)
,
FROM spatial;

SELECT *
FROM measurements
WHERE lat IS NOT NULL
LIMIT 10;

SELECT sensor_nodes_id
, st_snaptogrid(pt3857(lon, lat), 3000.0)
, MIN(datetime) as start_datetime
, MAX(datetime) as end_datetime
, COUNT(DISTINCT datetime) as measurements
, now()
FROM measurements m
JOIN sensors s USING (sensors_id)
JOIN sensor_systems ss USING (sensor_systems_id)
WHERE sensors_id = 2957942
GROUP BY 1,2
LIMIT 15;


SELECT lon
, lat
--, pt3857(lon, lat)
, st_astext(st_transform(pt3857(lon, lat), 4326))
, st_astext(st_transform(st_setsrid(st_makepoint(lon, lat), 4326), 3857))
, st_astext(st_setsrid(st_makepoint(lon, lat), 4326))
FROM measurements_temp
LIMIT 10;
