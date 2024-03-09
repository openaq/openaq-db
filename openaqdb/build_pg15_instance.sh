#!/bin/bash
# this script is designed to build and start a postgresql setup
# on an amazone linux 2023 instance

CORES=$(grep -c 'cpu[0-9]' /proc/stat)
JOBS=$((CORES*2))

# pass key and value
function setenv() {
		export "$1"="$2" && echo "$1=${2}" >> /etc/environment
}

# get prod version with `SELECT version()`
# setting them this way so that they are avaiable globally
# and for future reference in scripts
setenv PG_VERSION 15 # current production version
setenv PROJ_VERSION 9.2.0
setenv GDAL_VERSION 3.7.0
setenv GEOS_VERSION 3.12.0
setenv PGIS_VERSION 3.3.3
setenv PG_BOUNCER_VERSION 1.19.1
setenv PG_BOUNCER_FDW_VERSION 1.0.0
setenv PG_EXPORTER_VERSION 0.13.1
setenv NODE_EXPORTER_VERSION 1.6.0

# add folder for logs
mkdir -p /var/log/openaq

dnf update -y

dnf install -y \
    postgresql$PG_VERSION \
    postgresql$PG_VERSION-contrib \
    postgresql$PG_VERSION-server \
    postgresql$PG_VERSION-server-devel

# and for building everything
# moved to using repo versions of protobug
dnf install -y \
    sqlite-devel \
    libtiff \
    cmake \
    libxml2-devel \
    libtiff-devel \
    curl-devel \
    json-c-devel \
    libevent-devel \
		protobuf \
		protobuf-c-devel \
    git

# # how to install gdal
# # this is what we built off of and there are some good things in there
# # if you ever get stuck
# # https://gist.github.com/abelcallejo/e75eb93d73db6f163b076d0232fc7d7e

# get and build proj
cd /tmp
wget https://download.osgeo.org/proj/proj-$PROJ_VERSION.tar.gz
tar -xvf proj-$PROJ_VERSION.tar.gz
cd proj-$PROJ_VERSION
mkdir build
cd build
cmake ..
cmake --build .
cmake --build . --target install

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

# update the path to include local because some things install to local
export PATH=$PATH:/usr/local/bin

# Build PostGIS
cd /tmp
https://download.osgeo.org/postgis/source/postgis-
wget https://download.osgeo.org/postgis/source/postgis-$PGIS_VERSION.tar.gz
tar -xvf postgis-$PGIS_VERSION.tar.gz
cd postgis-$PGIS_VERSION
./configure \
    --with-geosconfig=/usr/local/bin/geos-config \
    --with-gdalconfig=/usr/local/bin/gdal-config
make -j $JOBS
make install

# To finish up we make sure that all the libraries we just installed
# are in the ld_library_path and therefor found by postgis
# without this postgis cannot be loaded as an extension
printf "/usr/local/lib64\n/usr/local/lib" >> /etc/ld.so.conf.d/postgis.conf
ldconfig

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
