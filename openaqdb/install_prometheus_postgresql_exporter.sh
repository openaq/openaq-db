#!/bin/bash

# should be run as root

POSTGRES_EXPORTER_VERSION=0.11.1
USER=$DATABASE_MONITOR_USER
PASSWORD=$DATABASE_MONITOR_PASSWORD

sudo -u postgres psql --single-transaction -v ON_ERROR_STOP=1<<EOSQL
CREATE ROLE ${USER} WITH LOGIN PASSWORD '${PASSWORD}';
ALTER USER ${USER} SET SEARCH_PATH TO postgres_exporter,pg_catalog;
GRANT CONNECT ON DATABASE postgres TO ${USER};
GRANT pg_monitor TO ${USER};
EOSQL


mkdir -p /etc/postgres_exporter

cd /tmp
wget https://github.com/prometheus-community/postgres_exporter/releases/download/v${POSTGRES_EXPORTER_VERSION}/postgres_exporter-${POSTGRES_EXPORTER_VERSION}.linux-amd64.tar.gz
tar -xvf postgres_exporter-${POSTGRES_EXPORTER_VERSION}.linux-amd64.tar.gz
mv postgres_exporter-${POSTGRES_EXPORTER_VERSION}.linux-amd64/postgres_exporter /usr/bin

IFS=
EXP=$(cat <<EOF
DATA_SOURCE_NAME="postgresql://$USER:$PASSWORD@localhost:$DATABASE_PORT/postgres?sslmode=disable"
PG_EXPORTER_EXTEND_QUERY_PATH="/etc/postgres_exporter/queries.yml"
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
systemctl start postgres-exporter
systemctl status postgres-exporter
