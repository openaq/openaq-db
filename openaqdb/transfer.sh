#!/bin/bash
# the following script is to aid in the transfer of data from one database to another
# it is currently not set up to run unassisted though it should

# fill out the info for where you want to pull data from
SOURCE_HOST=
SOURCE_PASSWORD=
SOURCE_PORT=
SOURCE_USER=
SOURCE_DB=

# and where you want it to be transferred to. Defaults to local db
DEST_HOST=localhost
DEST_PORT=$DATABASE_PORT
DEST_USER=$DATABASE_WRITE_USER
DEST_PASSWORD=$DATABASE_WRITE_PASSWORD
DEST_DB=$DATABASE_DB

# add any tables that need to be transferred
# refrain from adding large tables here and instead write something
# specific for those (see the measurements example)
TABLES=("providers" "sensor_nodes" "sensor_systems" "measurands" "sensors" )

for TABLE in "${TABLES[@]}"
do
    echo "Copying ${TABLE}..."
    PGPASSWORD=$SOURCE_PASSWORD psql \
          -h $SOURCE_HOST \
          -p $SOURCE_PORT \
          -U $SOURCE_USER \
          -d $SOURCE_DB \
          -c "SET statement_timeout='8h'" \
          -c "\COPY (SELECT * FROM ${TABLE}) TO ${TABLE}.csv DELIMITER ',' CSV"
done


for TABLE in "${TABLES[@]}"
do
    echo "Importing ${TABLE}..."
    PGPASSWORD=$DEST_PASSWORD psql \
          -h $DEST_HOST \
          -p $DEST_PORT \
          -U $DEST_USER \
          -d $DEST_DB \
          -c "CREATE TEMP TABLE ${TABLE}_tmp AS SELECT * FROM ${TABLE} WITH NO DATA" \
          -c "\COPY ${TABLE}_tmp FROM ${TABLE}.csv DELIMITER ',' CSV" \
          -c "INSERT INTO ${TABLE} OVERRIDING SYSTEM VALUE SELECT * FROM ${TABLE} ON CONFLICT DO NOTHING"
done


# Copy over the measurements
# do this in chunks in case something fails and we need to restart
# will need to adjust the parameters to fit the data
PAGE=1
PAGES=5
LIMIT=100000
until [ $PAGE -gt $PAGES ]
do
      OFFSET=$(((PAGE-1)*LIMIT))
      echo "Getting page ${PAGE} from ${OFFSET} for ${LIMIT}"
      PGPASSWORD=$SOURCE_PASSWORD psql \
                -h $SOURCE_HOST \
                -p $SOURCE_PORT \
                -U $SOURCE_USER \
                -d $SOURCE_DB \
                -c "SET statement_timeout='8h'" \
                -c "\COPY (SELECT * FROM measurements OFFSET ${OFFSET} LIMIT ${LIMIT}) TO measurements_page${PAGE}.csv DELIMITER ',' CSV"
      ((PAGE++))
done


# Copy over some fetchlog files for testing
PGPASSWORD=$SOURCE_PASSWORD psql \
      -h $SOURCE_HOST \
      -p $SOURCE_PORT \
      -U $SOURCE_USER \
      -d $SOURCE_DB \
      -c "\COPY (SELECT * FROM fetchlogs WHERE key ~* 'stations' LIMIT 25) TO fetchlogs_stations.csv DELIMITER ',' CSV" \
      -c "\COPY (SELECT * FROM fetchlogs WHERE key ~* 'measures' LIMIT 25) TO fetchlogs_measures.csv DELIMITER ',' CSV"


PGPASSWORD=$DEST_PASSWORD psql \
      -h $DEST_HOST \
      -p $DEST_PORT \
      -U $DEST_USER \
      -d $DEST_DB \
      -c "\COPY fetchlogs FROM fetchlogs_stations.csv DELIMITER ',' CSV" \
      -c "\COPY fetchlogs FROM fetchlogs_measures.csv DELIMITER ',' CSV" \
      -c "UPDATE fetchlogs SET loaded_datetime=NULL, completed_datetime=NULL WHERE key ~* 'measure|station'"
