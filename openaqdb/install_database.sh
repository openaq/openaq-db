#!/bin/bash

# if we are installing from amazon-linux-extras
# which we are as of right now
# db should already be mounted at this point
#PGPATH=/usr/bin
#PGDATA=/db/data # mount point of snapshot
#PGCONFIG=$PGDATA/postgresql.conf
mkdr -p /var/log/openaq # should have already been created but..

# make sure things are updated
yum update

# make sure that a postgres user exists
if ! id "postgres" >/dev/null 2>&1; then
    echo "No postgres user found. Adding now."
    adduser postgres
fi

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
    mkdir -p /var/log/postgresql
    chown postgres:postgres /var/log/postgresql
    sudo -i -u postgres touch /var/log/postgresql/postgresql.log
fi

# instead of modifying the base config we will add any updates
# to a different file which will be easier for us to track and update

if [ -z "$SNAPSHOT_ID" ]; then

    echo "include 'openaq_postgresql.conf'" >> $PGCONFIG
    echo "shared_preload_libraries = 'pg_cron,pg_stat_statements'" >> $PGDATA/openaq_postgresql.conf
    echo "log_directory = '$PGDATA/log'" >> $PGDATA/openaq_postgresql.conf
    echo "logging_collector = on" >> $PGDATA/openaq_postgresql.conf
    if [ ! -z "$PG_SHARED_BUFFERS" ]; then echo "shared_buffers = $PG_SHARED_BUFFERS" >> $PGDATA/openaq_postgresql.conf; fi
    if [ ! -z "$PG_WAL_BUFFERS" ]; then echo "wal_buffers = $PG_WAL_BUFFERS" >> $PGDATA/openaq_postgresql.conf; fi
    if [ ! -z "$PG_EFFECTIVE_CACHE_SIZE" ]; then echo "effective_cache_size = $PG_EFFECTIVE_CACHE_SIZE" >> $PGDATA/openaq_postgresql.conf; fi
    if [ ! -z "$PG_WORM_MEM" ]; then echo "work_mem = $PG_WORK_MEM" >> $PGDATA/openaq_postgresql.conf; fi
    if [ ! -z "$PG_MAINTENANCE_WORK_MEM" ]; then echo "maintenance_work_mem = $PG_MAINTENANCE_WORK_MEM" >> $PGDATA/openaq_postgresql.conf; fi

    # the next two lines make the database accessible to outside connections
    # this may or may not be what you want to do
    echo "listen_addresses='*'" >> $PGDATA/openaq_postgresql.conf
    # printf to include the line break
    printf "# TYPE DATABASE USER CIDR-ADDRESS  METHOD\nhost  all  all 0.0.0.0/0 md5" >> $PGDATA/pg_hba.conf
    # and start it
    sudo -i -u postgres $PGPATH/pg_ctl -D $PGDATA -o "-c listen_addresses='*' -p 5432" -m fast -w start

    # install the database
    sudo -i -u postgres /app/openaqdb/init.sh > /var/log/openaq/install_openaq_database.log 2>&1
    # install pg_cron
    sudo -i -u postgres psql -d postgres -c 'CREATE EXTENSION pg_cron' -f /app/openaqdb/cron.sql

    #./install_pgbouncer.sh

else

    #echo "include 'openaq_postgresql.conf'" >> $PGCONFIG
    #echo "shared_preload_libraries = 'timescaledb,pg_stat_statements'" >> $PGDATA/openaq_postgresql.conf
    # the next two lines make the database accessible to outside connections
    # this may or may not be what you want to do
    #echo "listen_addresses='*'" >> $PGDATA/openaq_postgresql.conf
    # printf to include the line break
    #printf "# TYPE DATABASE USER CIDR-ADDRESS  METHOD\nhost  all  all 0.0.0.0/0 md5" >> $PGDATA/pg_hba.conf
    # and start it
IFS=
INI=$(cat <<EOF
shared_preload_libraries = 'pg_cron,pg_stat_statements'
shared_buffers = $PG_SHARED_BUFFERS
wal_buffers = $PG_WAL_BUFFERS
effective_cache_size = $PG_EFFECTIVE_CACHE_SIZE
work_mem = $PG_WORK_MEM
maintenance_work_mem = $PG_MAINTENANCE_WORK_MEM
listen_addresses='*'
max_connections = 300
log_directory = '$PGDATA/log'
logging_collector = on

checkpoint_timeout = 900
max_wal_size = 95112
min_wal_size = 1024
EOF
   )
echo $INI | tee $PGDATA/openaq_postgresql.conf  > /dev/null

    sudo -i -u postgres $PGPATH/pg_ctl -D $PGDATA -o "-c listen_addresses='*' -p 5432" -m fast -w start

fi

# If a monitoring user exists the following script will install the monitoring
#./setup_pgbouncer.sh
#./setup_prometheus_postgresql_exporter.sh
#./setup_prometheus_node_exporter.sh
echo "Installing pgbouncer"
/app/openaqdb/install_pgbouncer.sh
echo "Installing node exporter"
/app/openaqdb/install_prometheus_node_exporter.sh
echo "Installing postgresql exporter"
/app/openaqdb/install_prometheus_postgresql_exporter.sh
