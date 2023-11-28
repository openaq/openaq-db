#!/bin/bash

while getopts ":e:s:h:m:" opt
do
   case "$opt" in
       e ) ED="$OPTARG" ;;
       s ) SD="$OPTARG" ;;
       h ) HOST="$OPTARG" ;;
       m ) METHOD="$OPTARG" ;;
   esac
done

if [ -z ${SD} ]; then
    SD=$(date -d '1 week ago' '+%Y-%m-%d')
fi;

if [ -z ${ED} ]; then
    ED=$(date '+%Y-%m-%d')
fi;

if [ -z ${DUR} ]; then
    DUR=24h
fi;

if [ -z ${HOST} ]; then
    HOST=localhost
fi;

if [ -z ${METHOD} ]; then
    METHOD=direct
fi;

URL=postgres://$DATABASE_WRITE_USER:$DATABASE_WRITE_PASSWORD@localhost:5432/$DATABASE_DB
exists=True
# s3 bucket and base location to use
BUCKET=s3://openaq-db-backups
# dump the sensor nodes
# import the

day=$(date -d "$SD" +%Y%m%d)
ends=$(date -d "$ED" +%Y%m%d)


# make sure that we table to hold the sensors and sensor nodes
MIGRATE_SENSOR_NODES=$(cat <<-EOF
CREATE TEMP TABLE IF NOT EXISTS sensor_nodes_migrate (
    sensor_nodes_id int primary key,
    timezones_id int,
    ismobile boolean,
    geom geometry,
    site_name text,
    source_name text NOT NULL,
    source_id text NOT NULL,
    origin text,
    metadata jsonb,
    added_on timestamptz,
    modified_on timestamptz,
    modified_times int,
    UNIQUE(source_name, source_id)
);
copy sensor_nodes_migrate from stdin DELIMITER '|' NULL '';
INSERT INTO sensor_nodes (
  sensor_nodes_id
, timezones_id
, ismobile
, geom
, site_name
, source_id
, source_name
, providers_id
, origin
, metadata
, added_on
, modified_on
, countries_id ) OVERRIDING SYSTEM VALUE
SELECT
  sensor_nodes_id
, timezones_id
, ismobile
, geom
, site_name
, source_id
, m.source_name
, p.providers_id
, origin
, m.metadata
, added_on
, modified_on
, get_countries_id(geom)
FROM sensor_nodes_migrate m
LEFT JOIN providers p ON (m.source_name = p.source_name)
ON CONFLICT DO NOTHING;
SELECT setval(
 pg_get_serial_sequence('sensor_nodes', 'sensor_nodes_id'),
 (SELECT MAX(sensor_nodes_id) FROM public.sensor_nodes)
);
EOF
                )





MIGRATE_SENSOR_SYSTEMS=$(cat <<-EOF
CREATE TEMP TABLE IF NOT EXISTS sensor_systems_migrate (
    sensor_systems_id int primary key,
    sensor_nodes_id int not null,
    source_id text,
    metadata jsonb,
    UNIQUE(source_id)
);
copy sensor_systems_migrate from stdin DELIMITER '|' NULL '';
-------------------
INSERT INTO sensor_systems (
  sensor_systems_id
, sensor_nodes_id
, source_id
, metadata ) OVERRIDING SYSTEM VALUE
SELECT
  m.sensor_systems_id
, m.sensor_nodes_id
, m.source_id
, m.metadata
FROM sensor_systems_migrate m
JOIN sensor_nodes n USING (sensor_nodes_id)
ON CONFLICT DO NOTHING;
-----------------
SELECT
  m.sensor_systems_id
, m.sensor_nodes_id
, m.source_id
, m.metadata
INTO sensor_systems_not_migrated
FROM sensor_systems_migrate m
LEFT JOIN sensor_nodes n USING (sensor_nodes_id)
WHERE n.sensor_nodes_id IS NULL;
-----------------
SELECT setval(
 pg_get_serial_sequence('sensor_systems', 'sensor_systems_id'),
 (SELECT MAX(sensor_systems_id) FROM public.sensor_systems)
);
EOF
                  )




MIGRATE_SENSORS=$(cat <<-EOF
CREATE TABLE IF NOT EXISTS sensors_migrate (
    sensors_id int primary key,
    sensor_systems_id int NOT NULL,
    measurands_id int not null,
    source_id text,
    data_averaging_period_seconds int,
    data_logging_period_seconds int,
    metadata jsonb,
    added_on timestamptz,
    modified_on timestamptz,
    modified_times int,
    UNIQUE(sensor_systems_id, measurands_id)
);
-------------------
copy sensors_migrate from stdin DELIMITER '|' NULL '';
-------------------
INSERT INTO measurands (measurands_id, measurand, units)
OVERRIDING SYSTEM VALUE VALUES (97, 'pm25-old', 'ugm3')
ON CONFLICT DO NOTHING;
-------------------
INSERT INTO sensors (
  sensors_id
, sensor_systems_id
, measurands_id
, source_id
, data_averaging_period_seconds
, data_logging_period_seconds
, metadata
, added_on
, modified_on ) OVERRIDING SYSTEM VALUE
SELECT
  m.sensors_id
, m.sensor_systems_id
, m.measurands_id
, m.source_id
, m.data_averaging_period_seconds
, m.data_logging_period_seconds
, m.metadata
, m.added_on
, m.modified_on
FROM sensors_migrate m
JOIN sensor_systems sy USING (sensor_systems_id)
JOIN measurands p USING (measurands_id)
ON CONFLICT DO NOTHING;
---------------------
SELECT setval(
 pg_get_serial_sequence('sensors', 'sensors_id'),
 (SELECT MAX(sensors_id) FROM public.sensors)
);
EOF
                  )



MIGRATE_EXPORT_LOGS=$(cat <<-EOF
CREATE TEMP TABLE IF NOT EXISTS export_logs_migrate (
    sensor_nodes_id int,
    day date,
    records int,
    measurands int,
    modified_on timestamptz,
    queued_on timestamptz,
    exported_on timestamptz,
    key text,
    bucket text,
    sec double precision,
    error text,
    version int,
    old_sensor_nodes_id int
);
copy export_logs_migrate from stdin DELIMITER '|' NULL ''
EOF
                   )


# Two different strategies for insert
# directly into the measurements
# faster but would fail on conflict
# ~4m
# direct measurement likely wont work once we have started importing

MIGRATE_MEASUREMENTS_DIRECT=$(cat <<-EOF
copy measurements(sensors_id, datetime, value, lat, lon) from stdin DELIMITER '|' NULL ''
EOF
                   )

MIGRATE_MEASUREMENTS_INDIRECT=$(cat <<-EOF
CREATE TEMP TABLE IF NOT EXISTS measurements_import (
   sensors_id int
 , datetime timestamptz
 , value double precision
 , lat double precision
 , lon double precision
);
---------------
copy measurements_import(sensors_id, datetime, value, lat, lon) from stdin DELIMITER '|' NULL '';
---------------
INSERT INTO measurements (sensors_id, datetime, value, lat, lon)
SELECT i.sensors_id
, i.datetime
, i.value
, i.lat
, i.lon
FROM measurements_import i
ON CONFLICT DO NOTHING;
EOF
                   )


# aws s3 cp "${BUCKET}/metadata/sensor_nodes_v1.csv.gz" - \
#     | gunzip -f -c \
#     | psql $URL -c "$MIGRATE_SENSOR_NODES"

# aws s3 cp "${BUCKET}/metadata/sensor_systems_v1.csv.gz" - \
#     | gunzip -f -c \
#     | psql $URL -c "$MIGRATE_SENSOR_SYSTEMS"

# aws s3 cp "${BUCKET}/metadata/sensors_v1.csv.gz" - \
#     | gunzip -f -c \
#     | psql $URL -c "$MIGRATE_SENSORS"

# aws s3 cp "${BUCKET}/metadata/export_logs_v1.csv.gz" - \
#     | gunzip -f -c \
#     | psql $URL -c "$MIGRATE_EXPORT_LOGS"


if [ $METHOD == "direct" ]; then
    echo "Using direct method"
    IMPORT_SQL="${MIGRATE_MEASUREMENTS_DIRECT}"
else
    echo "Using indirect method"
    IMPORT_SQL="${MIGRATE_MEASUREMENTS_INDIRECT}"
fi

while [[ $day -le $ends ]]; do
    PREFIX="${BUCKET}/measurements/measurements_v1_${day}00"
    day=$(date -d "${day} + 1 days" +"%Y%m%d")
    FILE="${PREFIX}_${day}00.csv.gz"
    echo $FILE
    exists=$(aws s3 ls $FILE)
    start_time=`date +%s`
    if [ -n "$exists" ]; then
        aws s3 cp "$FILE" - \
            | gunzip -f -c \
            | psql $URL -c "$IMPORT_SQL"
        echo 'TIME:' $((`date +%s`-start_time))
    fi
done
