#!/bin/bash

URL=postgres://$DATABASE_WRITE_USER:$DATABASE_WRITE_PASSWORD@localhost:5432/$DATABASE_DB
exists=True
# s3 bucket and base location to use
BUCKET=s3://openaq-db-backups/measurements
# dump the sensor nodes
# import the

# make sure that we table to hold the sensors and sensor nodes
CREATE_NODES_SQL=$(cat <<-EOF
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
EOF
                   )

INSERT_NODES_SQL=$(cat <<-EOF
INSERT INTO sensor_nodes (
  sensor_nodes_id
, timezones_id
, ismobile
, geom
, site_name
, source_id
, source_name
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
, source_name
, origin
, metadata
, added_on
, modified_on
, get_countries_id(geom)
FROM sensor_nodes_migrate;

SELECT setval(
 pg_get_serial_sequence('sensor_nodes', 'sensor_nodes_id'),
 (SELECT MAX(sensor_nodes_id) FROM public.sensor_nodes)
);
EOF
                   )


CREATE_SYSTEMS_SQL=$(cat <<-EOF
CREATE TEMP TABLE IF NOT EXISTS sensor_systems_migrate (
    sensor_systems_id int primary key,
    sensor_nodes_id int not null,
    source_id text,
    metadata jsonb,
    UNIQUE(source_id)
);
EOF
                   )

INSERT_SYSTEMS_SQL=$(cat <<-EOF
INSERT INTO sensor_systems (
  sensor_systems_id
, sensor_nodes_id
, source_id
, metadata ) OVERRIDING SYSTEM VALUE
SELECT
  sensor_systems_id
, sensor_nodes_id
, source_id
, metadata
FROM sensor_systems_migrate;

SELECT setval(
 pg_get_serial_sequence('sensor_systems', 'sensor_systems_id'),
 (SELECT MAX(sensor_systems_id) FROM public.sensor_systems)
);
EOF
                   )


CREATE_SENSORS_SQL=$(cat <<-EOF
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
EOF
                   )


INSERT_SENSORS_SQL=$(cat <<-EOF
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
  sensors_id
, sensor_systems_id
, measurands_id
, source_id
, data_averaging_period_seconds
, data_logging_period_seconds
, metadata
, added_on
, modified_on
FROM sensors_migrate;

SELECT setval(
 pg_get_serial_sequence('sensors', 'sensors_id'),
 (SELECT MAX(sensors_id) FROM public.sensors)
);
EOF
                   )

# Import a measurement file
CREATE_MEASUREMENTS_SQL=$(cat <<-EOF
DROP TABLE IF EXISTS measurements_import;
CREATE TEMP TABLE IF NOT EXISTS measurements_import (
   sensors_id int
 , datetime timestamptz
 , value double precision
 , lon double precision
 , lat double precision
);
EOF
          )

# # requires a map to clean the data
# IMPORT_SQL=$(cat <<-EOF
# INSERT INTO measurements (sensors_id, datetime, value, lat, lon)
# SELECT m.sensors_id
# , i.datetime
# , i.value
# , i.lat
# , i.lon
# FROM measurements_import i
# JOIN public.sensors_map m ON (i.sensors_id = m.old_sensors_id)
# ON CONFLICT DO NOTHING;
# EOF
#                    )

# no need for the map when importing cleaned data
INSERT_MEASUREMENTS_SQL=$(cat <<-EOF
INSERT INTO measurements (sensors_id, datetime, value, lat, lon)
SELECT sensors_id
, datetime
, value
, lat
, lon
FROM measurements_import
ON CONFLICT DO NOTHING;
EOF
                   )




aws s3 cp "${BUCKET}/sensor_nodes_v1.csv.gz" - \
    | gunzip -f -c \
    | psql $URL -c "$CREATE_NODES_SQL" \
           -c "copy sensor_nodes_migrate from stdin DELIMITER '|' NULL ''" \
           -c "$INSERT_NODES_SQL"


aws s3 cp "${BUCKET}/sensor_systems_v1.csv.gz" - \
    | gunzip -f -c \
    | psql $URL -c "$CREATE_SYSTEMS_SQL" \
           -c "copy sensor_systems_migrate from stdin DELIMITER '|' NULL ''" \
           -c "$INSERT_SYSTEMS_SQL"


aws s3 cp "${BUCKET}/sensors_v1.csv.gz" - \
    | gunzip -f -c \
    | psql $URL -c "$CREATE_SENSORS_SQL" \
           -c "copy sensors_migrate from stdin DELIMITER '|' NULL ''" \
           -c "$INSERT_SENSORS_SQL"




day=$(date -d "2022-10-01" +%Y%m%d)
d2=$(date -d "2022-10-01" +%Y%m%d)
ends=$(date -d "2022-10-02" +%Y%m%d)

while [[ $day -le $ends ]]; do
    PREFIX="${BUCKET}/measurements_v1_${day}00"
    day=$(date -d "${day} + 1 days" +"%Y%m%d")
    FILE="${PREFIX}_${day}00.csv.gz"
    echo $FILE
    exists=$(aws s3 ls $FILE)
    start=`date +%s`
    if [ -n "$exists" ]; then
        # Two different strategies for insert
        # directly into the measurements
        # faster but would fail on conflict
        # ~4m
        aws s3 cp "$FILE" - \
            | gunzip -f -c \
            | psql $URL -c "copy measurements(sensors_id, datetime, value, lat, lon) from stdin DELIMITER '|' NULL ''"
        # or by importing and then bulk inserting
        # slower but wont fail on conflict
        # ~15m
        # aws s3 cp "$FILE" - \
            #     | gunzip -f -c \
            # | psql $URL \
            #        -c "$CREATE_SQL" \
            #        -c "copy measurements_import from stdin DELIMITER '|' NULL ''" \
            #        -c "$IMPORT_SQL"
        echo 'TIME:' $((`date +%s`-start))
    fi
done
