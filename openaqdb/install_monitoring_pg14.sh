#!/bin/bash

function setenv() {
		export "$1"="$2" && echo "$1=${2}" >> /etc/environment
}

setenv PG_BOUNCER_VERSION 1.19.1
setenv PG_BOUNCER_FDW_VERSION 1.0.0
setenv PG_EXPORTER_VERSION 0.13.1
setenv NODE_EXPORTER_VERSION 1.6.0

mkdir /etc/pgbouncer
chown postgres:postgres /etc/pgbouncer

# PG_BOUNCER and CrunchyData's data wrapper to make it easier to query
cd /tmp
wget https://github.com/pgbouncer/pgbouncer/releases/download/pgbouncer_${PG_BOUNCER_VERSION//./_}/pgbouncer-$PG_BOUNCER_VERSION.tar.gz
tar -zxvf pgbouncer-$PG_BOUNCER_VERSION.tar.gz
cd pgbouncer-$PG_BOUNCER_VERSION
./configure
make -j $JOBS
make install

# Now install the fdw
cd /tmp
wget https://github.com/CrunchyData/pgbouncer_fdw/archive/refs/tags/v${PG_BOUNCER_FDW_VERSION}.tar.gz
tar -xvf v${PG_BOUNCER_FDW_VERSION}.tar.gz
cd pgbouncer_fdw-$PG_BOUNCER_FDW_VERSION
make -j $JOBS
make install

mkdir -p /etc/pgbouncer
chown postgres:postgres /etc/pgbouncer
mkdir -p /var/run/pgbouncer
chown postgres:postgres /var/run/pgbouncer
mkdir -p /var/log/pgbouncer
chown postgres:postgres /var/log/pgbouncer
sudo -i -u postgres touch /var/log/pgbouncer/pgbouncer.log

# Prometheus postgresql exporter
cd /tmp
wget https://github.com/prometheus-community/postgres_exporter/releases/download/v${PG_EXPORTER_VERSION}/postgres_exporter-${PG_EXPORTER_VERSION}.linux-amd64.tar.gz
tar -xvf postgres_exporter-${PG_EXPORTER_VERSION}.linux-amd64.tar.gz
mv postgres_exporter-${PG_EXPORTER_VERSION}.linux-amd64/postgres_exporter /usr/bin

cd /tmp
wget https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
tar -xvf node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
mv node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64/node_exporter /usr/local/bin

useradd -rs /bin/false node_exporter

cd /app/openaqdb

./install_pgbouncer.sh
./install_prometheus_postgresql_exporter.sh
./install_prometheus_node_exporter.sh
