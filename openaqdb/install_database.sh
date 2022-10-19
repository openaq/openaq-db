#!/bin/bash

# if we are installing from amazon-linux-extras
# which we are as of right now
# db should already be mounted at this point
PGPATH=/usr/bin
PGDATA=/db/data # mount point of snapshot
PGCONFIG=$PGDATA/postgresql.conf

# make sure that postgres owns the $PGDATA directory
# as long as you are building this from a snapshot they do
# start postgres as postgres user
# you cant do
# sudo -i -u postgres $PGPATH/initdb -D $PGDATA
# ls $PGDATA --ignore=sed* --ignore=core* -lht
# ls $PGDATA/*postgres*.conf -lht
# if we dont have a snapshot id we need to create and init
if [ -z "$SNAPSHOT_ID" ]; then
    mkdir -p $PGDATA
    chown postgres:postgres $PGDATA
    sudo -i -u postgres $PGPATH/initdb -D $PGDATA
else
    # now create the log file. This needs to be done because
    # this is where the snapshot expects the log file to be
    mkdir /var/log/postgresql
    chown postgres:postgres /var/log/postgresql
    sudo -i -u postgres touch /var/log/postgresql/postgresql.log
fi

# instead of modifying the base config we will add any updates
# to a different file which will be easier for us to track and update
echo "include 'openaq_postgresql.conf'" >> $PGCONFIG
echo "shared_preload_libraries = 'timescaledb, pg_cron'" >> $PGDATA/openaq_postgresql.conf
# the next two lines make the database accessible to outside connections
# this may or may not be what you want to do
echo "listen_addresses='*'" >> $PGDATA/openaq_postgresql.conf
# printf to include the line break
printf "# TYPE DATABASE USER CIDR-ADDRESS  METHOD\nhost  all  all 0.0.0.0/0 md5" >> $PGDATA/pg_hba.conf
# and start it
sudo -i -u postgres $PGPATH/pg_ctl -D $PGDATA -o "-c listen_addresses='*' -p 5432" -m fast -w start

if [ -z "$SNAPSHOT_ID" ]; then
    # install the database
    cd /app/openaqdb
    sudo -u postgres ./init.sh > openaq_install.log 2>&1
else
    PROD_URL=postgres://$DATABASE_READ_USER:$DATABASE_READ_PASSWORD@$DATABASE_HOST:$DATABASE_PORT/$DATABASE_DB
    LOCAL_URL=postgres://$DATABASE_WRITE_USER:$DATABASE_WRITE_PASSWORD@localhost:5432/$DATABASE_DB
    # now copy the last known fetchlogs id
    FETCHLOGS_ID=$(psql $LOCAL_URL -XAwtc "SELECT MAX(fetchlogs_id) FROM fetchlogs")
    # save it for refernce later
    echo FETCHLOGS_ID=${FETCHLOGS_ID} >> /etc/environment
    # use it to load any keys we might have missed and then clean up
    psql $PROD_URL -XAwtc "SELECT key FROM fetchlogs WHERE fetchlogs_id > $FETCHLOGS_ID" > '/tmp/fetchlog_keys.csv'
    psql $LOCAL_URL \
         -c "BEGIN" \
         -c "DROP TABLE IF EXISTS fetchlog_keys" \
         -c "CREATE TABLE IF NOT EXISTS fetchlog_keys (key varchar)" \
         -c "COPY fetchlog_keys FROM '/tmp/fetchlog_keys.csv' WITH (FORMAT csv)" \
         -c "INSERT INTO fetchlogs (key) SELECT key FROM fetchlog_keys ON CONFLICT DO NOTHING" \
         -c "DROP TABLE IF EXISTS fetchlog_keys" \
         -c "COMMIT"

fi
