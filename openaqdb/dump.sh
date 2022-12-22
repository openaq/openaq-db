#!/bin/bash
# create a db connection url
URL=postgres://$DATABASE_WRITE_USER:$DATABASE_WRITE_PASSWORD@localhost:5432/$DATABASE_DB

SQL=$(cat <<-EOF
  SELECT chunk_name
  , chunk_schema
  , to_char(range_start,'YYYYMMDD') as starts
  , to_char(range_end,'YYYYMMDD') as ends
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'measurements'
  ORDER BY range_start
  OFFSET 0
  LIMIT 200
EOF
   )

psql $URL -At -F ' ' -c "$SQL" \
    | while read -a Record; do
    # ------------------------------
    chunk_schema=${Record[1]}
    chunk_name=${Record[0]}
    starts=${Record[2]}
    ends=${Record[3]}
    # -------------------------------
    TIMEOUT="SET statement_timeout TO '1h'"
    TABLE="${chunk_schema}.${chunk_name}"
    LIMIT=100000000
    OFFSET=
    CMD="SELECT * FROM ${TABLE} LIMIT $LIMIT"
    FILE="s3://openaq-db-backups/measurements/measurements_${starts}_${ends}.csv"
    # -------------------------------
    #TOTAL=$(psql $URL -qtAXc "SELECT COUNT(1) FROM ${TABLE}")
    exists=$(aws s3 ls $FILE)
    if [ -z "$exists" ]; then
        FILE="s3://openaq-db-backups/measurements/measurements_${starts}_${ends}_${LIMIT}.csv"
        echo "Exporting -> $chunk_name to $FILE";
        psql $URL -XAwt \
             -c "$TIMEOUT" \
             -c "$CMD" \
            | aws s3 cp - "$FILE"
    else
        echo "$FILE exists"
    fi

done
