# https://access.crunchydata.com/documentation/pgmonitor/latest/exporter/#setup-on-rhel-or-centos

CORES=$(grep -c 'cpu[0-9]' /proc/stat)
JOBS=$((CORES*2))

BLACKBOX_VERSION=0.22.0
NODE_VERSION=1.3.1
POSTGRES_VERSION=0.11.1
PGBOUNCER_VERSION=0.3
BLOAT_VERSION=2.7.0

sudo useradd -m -d /var/lib/ccp_monitoring ccp_monitoring


cd /tmp
wget https://github.com/prometheus/blackbox_exporter/releases/download/v${BLACKBOX_VERSION}/blackbox_exporter-${BLACKBOX_VERSION}.linux-amd64.tar.gz
tar -xvf blackbox_exporter-${BLACKBOX_VERSION}.linux-amd64.tar.gz
mv blackbox_exporter-${BLACKBOX_VERSION}.linux-amd64/blackbox_exporter /usr/bin


cd /tmp
wget https://github.com/prometheus/node_exporter/releases/download/v${NODE_VERSION}/node_exporter-${NODE_VERSION}.linux-amd64.tar.gz
tar -xvf node_exporter-${NODE_VERSION}.linux-amd64.tar.gz
mv node_exporter-${NODE_VERSION}.linux-amd64/node_exporter /usr/bin


cd /tmp
wget https://github.com/prometheus-community/postgres_exporter/releases/download/v${POSTGRES_VERSION}/postgres_exporter-${POSTGRES_VERSION}.linux-amd64.tar.gz
tar -xvf postgres_exporter-${POSTGRES_VERSION}.linux-amd64.tar.gz
mv postgres_exporter-${POSTGRES_VERSION}.linux-amd64/postgres_exporter /usr/bin


cd /tmp
wget https://github.com/CrunchyData/pgbouncer_fdw/archive/refs/tags/v${PGBOUNCER_VERSION}.tar.gz
tar -xvf v${PGBOUNCER_VERSION}.tar.gz
cd pgbouncer_fdw-$PGBOUNCER_VERSION
./configure
make -j $JOBS
make install
## installs to /usr/share/pgsql/extension
## ls /usr/share/pgsql/extension/ -lht | grep pgbouncer

## https://github.com/keithf4/pg_bloat_check
cd /tmp
wget https://github.com/keithf4/pg_bloat_check/archive/refs/tags/v${BLOAT_VERSION}.tar.gz
tar -xvf v${BLOAT_VERSION}.tar.gz
