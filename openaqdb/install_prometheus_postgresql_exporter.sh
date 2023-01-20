#!/bin/bash

# should be run as root

POSTGRES_EXPORTER_VERSION=0.11.1
DBUSER=$DATABASE_MONITOR_USER
PASSWORD=$DATABASE_MONITOR_PASSWORD
QUERIES_FILE=/app/openaqdb/queries.yml

# do not install anything unless we have a user name
if [ $DBUSER ]; then

    mkdir -p /etc/postgres_exporter

    sudo -i -u postgres psql --single-transaction -v ON_ERROR_STOP=1<<EOSQL
CREATE ROLE ${DBUSER} WITH LOGIN PASSWORD '${PASSWORD}';
ALTER USER ${DBUSER} SET SEARCH_PATH TO postgres_exporter,pg_catalog;
GRANT CONNECT ON DATABASE postgres TO ${DBUSER};
GRANT pg_monitor TO ${DBUSER};
EOSQL

    cd /tmp
    wget https://github.com/prometheus-community/postgres_exporter/releases/download/v${POSTGRES_EXPORTER_VERSION}/postgres_exporter-${POSTGRES_EXPORTER_VERSION}.linux-amd64.tar.gz
    tar -xvf postgres_exporter-${POSTGRES_EXPORTER_VERSION}.linux-amd64.tar.gz
    mv postgres_exporter-${POSTGRES_EXPORTER_VERSION}.linux-amd64/postgres_exporter /usr/bin

    IFS=
    EXP=$(cat <<EOF
DATA_SOURCE_NAME="postgresql://$DBUSER:$PASSWORD@localhost:$DATABASE_PORT/$DATABASE_DB?sslmode=disable"
PG_EXPORTER_EXTEND_QUERY_PATH="${QUERIES_FILE}"
PG_EXPORTER_CONSTANT_LABELS=name=$DATABASE_INSTANCE_ID
PG_EXPORTER_INCLUDE_DATABASES="${DATABASE_DB}"
EOF
       )
    echo $EXP | tee /etc/postgres_exporter/postgres_exporter.env  > /dev/null

    IFS=
    SRV=$(cat <<EOF
[Unit]
Description=Prometheus exporter for Postgresql
Wants=network-online.target
After=network-online.target

[Service]
User=postgres
Group=postgres
WorkingDirectory=/etc/postgres_exporter
EnvironmentFile=/etc/postgres_exporter/postgres_exporter.env
ExecStart=/usr/bin/postgres_exporter --web.listen-address=:9187 --web.telemetry-path=/metrics
Restart=always

[Install]
WantedBy=multi-user.target
EOF
       )
    echo $SRV | tee /etc/systemd/system/postgres-exporter.service  > /dev/null

    systemctl daemon-reload
    systemctl enable postgres-exporter
    systemctl start postgres-exporte
    systemctl status postgres-exporter

fi
