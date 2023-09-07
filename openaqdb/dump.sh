#!/bin/bash
# create a db connection url
while getopts ":e:s:d:h:" opt
do
   case "$opt" in
       e ) ED="$OPTARG" ;;
       s ) SD="$OPTARG" ;;
       d ) DUR="$OPTARG" ;;
       h ) HOST="$OPTARG" ;;
   esac
done

if [ -z ${SD} ]; then
    SD=$(date -d '1 month ago' '+%Y-%m-%d')
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

URL=postgres://$DATABASE_WRITE_USER:$DATABASE_WRITE_PASSWORD@$HOST:5432/$DATABASE_DB
exists=True
# s3 bucket and base location to use
BUCKET=s3://openaq-db-backups
# dump the sensor nodes

psql $URL -XAwtq -c "SELECT * FROM public.sources_from_openaq" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/sources_from_openaq.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.origins" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/origins.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.groups" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/groups.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.groups_sensors" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/groups_sensors.csv.gz"

# adding quotes to make it easier to read back in
psql $URL -XAwtq -c "SELECT '\"'||slug||'\"', '\"'||readme||'\"' FROM public.readmes" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/readmes.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.sensor_nodes_migrate" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/sensor_nodes_v1.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.sensor_systems_migrate" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/sensor_systems_v1.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.sensors_migrate" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/sensors_v1.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.sensor_nodes_map" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/sensor_nodes_map_v1.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.sensors_map" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/sensors_map_v1.csv.gz"

psql $URL -XAwtq -c "SELECT * FROM public.export_logs_migrate" \
    | gzip -f -c \
    | aws s3 cp - "${BUCKET}/metadata/export_logs_v1.csv.gz"



SQL=$(cat <<-EOF
  WITH days AS (
  SELECT generate_series('${SD}'::date, '${ED}'::date, '${DUR}'::interval) as start_ts)
  SELECT start_ts
  , start_ts + '${DUR}'::interval as end_ts
  , chunk_schema||'.'||chunk_name as table_name
  , to_char(start_ts,'YYYYMMDDHH24') as formatted_start_ts
  , to_char(start_ts + '${DUR}'::interval,'YYYYMMDDHH24') as formatted_end_ts
  FROM days d
  JOIN timescaledb_information.chunks c ON (d.start_ts >= c.range_start AND d.start_ts + '${DUR}'::interval < c.range_end)
  WHERE hypertable_name = 'measurements';
EOF
   )

EXPORT_SQL=
psql $URL -At -F ' ' -c "$SQL" \
    | while read -a Record; do
    # ------------------------------
    STARTS="${Record[0]} ${Record[1]}"
    ENDS="${Record[2]} ${Record[3]}"
    TABLE=${Record[4]}
    FORMATTED_START=${Record[5]}
    FORMATTED_END=${Record[6]}
    # -------------------------------
    TIMEOUT="SET statement_timeout TO '1h'"

    CMD=$(cat <<-EOF
SELECT m.sensors_id
, i.datetime
, MAX(i.value) as value
, i.lat
, i.lon
FROM public.measurements i
JOIN public.sensors_map m ON (i.sensors_id = m.old_sensors_id)
WHERE datetime > '${STARTS}'::timestamptz AND datetime <= '${ENDS}'::timestamptz
GROUP BY 1,2,4,5;
EOF
   )
    #CMD="SELECT * FROM ${TABLE} WHERE datetime > '${STARTS}'::timestamptz AND datetime <= '${ENDS}'::timestamptz"
    FILE="${BUCKET}/measurements/measurements_v1_${FORMATTED_START}_${FORMATTED_END}.csv.gz"
    # -------------------------------
    exists=$(aws s3 ls $FILE)
    if [ -z "$exists" ]; then
        start=`date +%s`
        echo "Exporting ${STARTS} -> ${ENDS} to $FILE";
        psql $URL -XAwtq \
             -c "$TIMEOUT" \
             -c "$CMD" \
            | gzip -f -c \
            | aws s3 cp - "$FILE"
        echo 'TIME:' $((`date +%s`-start))
    else
        echo "$FILE exists"
    fi

done
