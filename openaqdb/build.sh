#!/bin/bash
# this script is designed to build and start a postgresql setup
# on an amazone linux 2 instance
CORES=$(grep -c 'cpu[0-9]' /proc/stat)
JOBS=$((CORES*2))

PG_MAJOR=13
PROJ_VERSION=6.1.1 # cant move to 6.2 unless we update sqlite
GDAL_VERSION=3.4.3
GEOS_VERSION=3.10.3
PGIS_VERSION=3.2.1
TIMESCALEDB_VERSION=2.7.0 # must be installed from source unless we move to pg14
GO_VERSION=1.18.3

# if we install from latest repo
PGPATH=/usr/pgsql-$PG_MAJOR/bin
PGDATA=/var/lib/pgsql/$PG_MAJOR/data
PGCONFIG=/var/lib/pgsql/$PG_MAJOR/data/postgresql.conf

# if we are installing from amazon-linux-extras
# which we are as of right now
PGPATH=/usr/bin
PGDATA=/var/lib/pgsql/data
PGCONFIG=$PGDATA/postgresql.conf

yum update -y

# this will make the default `postgresql` install 13
amazon-linux-extras enable postgresql$PG_MAJOR

yum clean metadata
yum install -y \
    postgresql \
    postgresql-devel \
    postgresql-contrib \
    postgresql-server \
    postgresql-server-devel

# and for building everything
yum install -y \
    gcc-c++ \
    cpp \
    sqlite-devel \
    libtiff \
    cmake3 \
    libxml2-devel \
    protobuf-c-devel \
    clang-devel \
    llvm-devel \
    openssl-devel \
    git

# how to install gdal
# https://gist.github.com/abelcallejo/e75eb93d73db6f163b076d0232fc7d7e
# add the repos

# get and build proj
# if we go higher than 6.1 we will need to also compile sqlite from source
# as the one that is included in the aws linux 2 instance is too old.
# once we can use the amazon linux 2022 this will not be an issue and we
# should update this
cd /tmp
wget https://download.osgeo.org/proj/proj-$PROJ_VERSION.tar.gz
tar -xvf proj-$PROJ_VERSION.tar.gz
cd proj-$PROJ_VERSION
./configure
make -j $JOBS
make install

# now get and build gdal
# Anything in the 3+ range is good adn they are on 3.5 as of may 2022
cd /tmp
# updated to use 3.4
wget https://github.com/OSGeo/gdal/releases/download/v$GDAL_VERSION/gdal-$GDAL_VERSION.tar.gz
tar -xvf gdal-$GDAL_VERSION.tar.gz
cd gdal-$GDAL_VERSION
./configure --with-proj=/usr/local
make -j $JOBS
make install

cd /tmp
# Use the latest version if possible
wget https://download.osgeo.org/geos/geos-$GEOS_VERSION.tar.bz2
tar -xvf geos-$GEOS_VERSION.tar.bz2
cd geos-$GEOS_VERSION
mkdir _build
cd _build
cmake3 \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    ..
make -j $JOBS
# ctest
make install
# builds to /usr/local/lib64

# Build PostGIS
# if we want to be able to use MVT method we will need to increase the
# version of protobuf-c which will mean building from source until (likely)
# we shift to the newer aws linux 2022
cd /tmp
wget https://postgis.net/stuff/postgis-$PGIS_VERSION.tar.gz
tar -xvf postgis-$PGIS_VERSION.tar.gz
cd postgis-$PGIS_VERSION
./configure \
    --with-geosconfig=/usr/local/bin/geos-config \
    --with-gdalconfig=/usr/local/bin/gdal-config \
    -without-protobuf
make -j $JOBS
make install
# To finish up we make sure that all the libraries we just installed
# are in the ld_library_path and therefor found by postgis
# without this postgis cannot be loaded as an extension
printf "/usr/local/lib64\n/usr/local/lib" >> /etc/ld.so.conf.d/postgis.conf
ldconfig

# install timescaledb
# currently using the most recent
# The version of timescale that is avaiable via repos will not install
# without a newer version of postgresql 13. We can get a newer version
# of pg13 but not the server and devel packages that would be required
# to build postgis.
# I dont think PG14 would have the same issue.
# add symlink  to cmake so the build process can find it
# timescale build requires cmake 3 but looks in cmake
# centos installs cmake3 so ...
ln -s /usr/bin/cmake3 /usr/bin/cmake
cd /tmp
git clone https://github.com/timescale/timescaledb.git
cd timescaledb
git checkout $TIMESCALEDB_VERSION
./bootstrap
cd ./build && make3 -j $JOBS
make install
# installs to /usr/lib64/pgsql

# install go
cd /tmp
wget https://go.dev/dl/go$GO_VERSION.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go$GO_VERSION.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# update the path to include local because some things install to local
export PATH=$PATH:/usr/local/bin

# start postgres as postgres user
sudo -i -u postgres $PGPATH/initdb -D $PGDATA

# instead of modifying the base config we will add any updates
# to a different file which will be easier for us to track and update
echo "include 'database.conf'" >> $PGCONFIG
echo "shared_preload_libraries = 'timescaledb'" >> $PGDATA/database.conf
# the next two lines make the database accessible to outside connections
# this may or may not be what you want to do
echo "listen_addresses='*'" >> $PGDATA/database.conf
# printf to include the line break
printf "# TYPE DATABASE USER CIDR-ADDRESS  METHOD\nhost  all  all 0.0.0.0/0 md5" >> $PGDATA/pg_hba.conf
# and start it
sudo -i -u postgres $PGPATH/pg_ctl -D $PGDATA -o "-c listen_addresses='*' -p 5432" -m fast -w start

# at this point everything we need is built and started
# but no databaseses or database users have been created
