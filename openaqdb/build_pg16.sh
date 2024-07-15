#!/bin/bash
# this script is designed to build and start a postgresql setup
# on an amazone linux 2 instance
# The final thing that is left to do would be to build
# sqlite from source so we can move beyond 6.1
# also, Amazon Linux 2 LTS goes until June 30, 2024
# https://aws.amazon.com/amazon-linux-2/faqs/
# by that time we should be moved to Amazon Linux 2023

# run as super user
# sudo su

dnf -y groupinstall "Development Tools"

dnf install -y \
    sqlite-devel \
    libtiff \
    cmake \
    python3.11 \
    python3.11-devel \
    python3.11-setuptools \
    libxml2-devel \
    libtiff-devel \
    curl-devel \
    libicu-devel \
    json-c-devel \
    libevent-devel \
    openssl-devel \
		protobuf \
		protobuf-c-devel \
    readline-devel \
    libuuid-devel \
    git


CORES=$(grep -c 'cpu[0-9]' /proc/stat)
JOBS=$((CORES*2))
# get prod version with `SELECT version()`
POSTGRES_VERSION=16.3 # current production version

SQLITE_VERSION=3410200
PROJ_VERSION=9.3.0
GDAL_VERSION=3.8.4
GEOS_VERSION=3.12.1
PGIS_VERSION=3.4.2
PCRE2_VERSION=10.43

PG_BOUNCER_VERSION=1.22.1
PG_BOUNCER_FDW_VERSION=1.1.0
PG_EXPORTER_VERSION=0.15.0
NODE_EXPORTER_VERSION=1.8.1


# proj4
if [ ! -z "$PROJ_VERSION" ]; then
    cd /tmp
    wget https://download.osgeo.org/proj/proj-$PROJ_VERSION.tar.gz
    tar -xvf proj-$PROJ_VERSION.tar.gz
    cd proj-$PROJ_VERSION
    mkdir build
    cd build
    cmake ..
    cmake --build .
    cmake --build . --target install
fi

if [ ! -z "$GEOS_VERSION" ]; then
    cd /tmp
    # Use the latest version if possible
    wget https://download.osgeo.org/geos/geos-$GEOS_VERSION.tar.bz2
    tar -xvf geos-$GEOS_VERSION.tar.bz2
    cd geos-$GEOS_VERSION
    mkdir build
    cd build
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        ..
    make -j $JOBS
    # ctest
    make install
    # builds to /usr/local/lib64
fi

if [ ! -z "$GDAL_VERSION" ]; then
    # now get and build gdal
    cd /tmp
    wget https://github.com/OSGeo/gdal/releases/download/v$GDAL_VERSION/gdal-$GDAL_VERSION.tar.gz
    tar -xvf gdal-$GDAL_VERSION.tar.gz
    cd gdal-$GDAL_VERSION
    mkdir build
    cd build
    cmake ..
    cmake --build .
    cmake --build . --target install
fi

if [ ! -z "$PCRE2_VERSION" ]; then
    # https://postgis.net/docs/manual-2.4/postgis_installation.html#installing_pagc_address_standardizer
    cd /tmp
    wget https://github.com/PCRE2Project/pcre2/releases/download/pcre2-$PCRE2_VERSION/pcre2-$PCRE2_VERSION.tar.gz
    tar -xvf pcre2-$PCRE2_VERSION.tar.gz
    cd pcre2-$PCRE2_VERSION
    ./configure
    make -j $JOBS
    make install
    # installs to /usr/local
fi

# To finish up we make sure that all the libraries we just installed
# are in the ld_library_path and therefor found by postgis
# without this postgis cannot be loaded as an extension
printf "/usr/local/lib64\n/usr/local/lib" >> /etc/ld.so.conf.d/postgis.conf
ldconfig

# fix the regex warning
semodule -B

if [ ! -z "$POSTGRES_VERSION" ]; then
    cd /tmp
    wget https://ftp.postgresql.org/pub/source/v$POSTGRES_VERSION/postgresql-$POSTGRES_VERSION.tar.gz
    tar -xvf postgresql-$POSTGRES_VERSION.tar.gz
    cd postgresql-$POSTGRES_VERSION
    ./configure --with-uuid=e2fs --with-openssl
    make -j $JOBS
    make install

    ## configure
    if ! id "postgres" >/dev/null 2>&1; then
        echo "No postgres user found. Adding now."
        adduser postgres
    fi

    echo 'testing psql'
    psql --version

    cat >> /etc/profile.d/openaq.sh << \EOF
  PATH=$PATH:/usr/local/pgsql/bin
EOF
    export PATH=$PATH:/usr/local/pgsql/bin
    export USE_PGXS=1

    ## now some extensions

    for extension in ltree btree_gin btree_gist pg_stat_statements uuid-ossp pgcrypto postgres_fdw file_fdw dblink unaccent
    do
        echo "Installing ${extension}"
        cd /tmp/postgresql-$POSTGRES_VERSION/contrib/$extension
        make -j $JOBS
        make install
    done
fi

if [ ! -z "$PGIS_VERSION" ]; then
    cd /tmp
    wget https://download.osgeo.org/postgis/source/postgis-$PGIS_VERSION.tar.gz
    tar -xvf postgis-$PGIS_VERSION.tar.gz
    cd postgis-$PGIS_VERSION
    ./configure \
        --with-geosconfig=/usr/local/bin/geos-config \
        --with-gdalconfig=/usr/local/bin/gdal-config \
        --with-pcredir=/usr/local \
        --with-pgconfig=/usr/local/pgsql/bin/pg_config
    make -j $JOBS
    make install
fi


cd /tmp
git clone https://github.com/citusdata/pg_cron.git
cd pg_cron
make -j $JOBS
make install
# installs to /usr/lib64/pgsql

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



exit
