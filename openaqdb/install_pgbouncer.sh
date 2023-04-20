#!/bin/bash
# Installs the designated version of pgbouncer and starts it
sudo su

PGBOUNCER_VERSION=1.17.0
PGBOUNCER_FDW_VERSION=0.4
ADMIN_USER=$DATABASE_WRITE_USER
ADMIN_PASSWORD=$DATABASE_WRITE_PASSWORD
AUTH_TYPE=trust # md5
POOL_SIZE=200 # 50
PORT=6432

CORES=$(grep -c 'cpu[0-9]' /proc/stat)
JOBS=$((CORES*2))

# pulled from the more generic build scripts, do not need
# all of these but they are likely already installed
# except for maybe libevent-devel

yum install -y \
    gcc-c++ \
    cpp \
    sqlite-devel \
    libtiff \
    cmake3 \
    libxml2-devel \
    clang-devel \
    llvm-devel \
    openssl-devel \
    libtiff-devel \
    curl-devel \
    json-c-devel \
    libevent-devel

cd /tmp
wget https://github.com/pgbouncer/pgbouncer/releases/download/pgbouncer_${PGBOUNCER_VERSION//./_}/pgbouncer-$PGBOUNCER_VERSION.tar.gz
tar -zxvf pgbouncer-$PGBOUNCER_VERSION.tar.gz
cd pgbouncer-$PGBOUNCER_VERSION
./configure
make -j $JOBS
make install
# INSTALL  pgbouncer /usr/local/bin
# INSTALL  README.md /usr/local/share/doc/pgbouncer
# INSTALL  NEWS.md /usr/local/share/doc/pgbouncer
# INSTALL  etc/pgbouncer.ini /usr/local/share/doc/pgbouncer
# INSTALL  etc/userlist.txt /usr/local/share/doc/pgbouncer
# INSTALL  doc/pgbouncer.1 /usr/local/share/man/man1
# INSTALL  doc/pgbouncer.5 /usr/local/share/man/man5

mkdir /etc/pgbouncer
chown postgres:postgres /etc/pgbouncer

# once its installed we need to create an ini file
# as well as a few other directories
IFS=
INI=$(cat <<EOF
[databases]
* = host=127.0.0.1

[pgbouncer]

logfile = /var/log/pgbouncer/pgbouncer.log
pidfile = /var/run/pgbouncer/pgbouncer.pid
listen_addr = *
listen_port = $PORT

;client_tls_sslmode = require
;client_tls_ca_file = /etc/pgbouncer/certs/root.crt
;client_tls_key_file = /etc/pgbouncer/certs/server.key
;client_tls_cert_file = /etc/pgbouncer/certs/server.crt

server_tls_sslmode = require
auth_type = $AUTH_TYPE
auth_file = /etc/pgbouncer/userlist.txt

;auth_query = SELECT * FROM scalegrid_pg.user_lookup($1);
auth_user = $ADMIN_USER, $DATABASE_MONITOR_USER
admin_users = $ADMIN_USER, $DATABASE_MONITOR_USER
stats_users = $ADMIN_USER, $DATABASE_MONITOR_USER

pool_mode = session
server_reset_query = DISCARD ALL
ignore_startup_parameters = extra_float_digits
application_name_add_host = 1
max_client_conn = 3000
default_pool_size = $POOL_SIZE
EOF
   )
echo $INI | sudo -i -u postgres tee /etc/pgbouncer/pgbouncer.ini  > /dev/null


IFS=
USERS=$(cat <<EOF
"$ADMIN_USER" "$ADMIN_PASSWORD"
"$DATABASE_MONITOR_USER" "$DATABASE_MONITOR_PASSWORD"
EOF
      )
echo $USERS | sudo -i -u postgres tee /etc/pgbouncer/userlist.txt  > /dev/null

mkdir /var/run/pgbouncer
chown postgres:postgres /var/run/pgbouncer
mkdir /var/log/pgbouncer
chown postgres:postgres /var/log/pgbouncer
sudo -i -u postgres touch /var/log/pgbouncer/pgbouncer.log

# start the server and then disconnect
# if already running than reboot
sudo -i -u postgres /usr/local/bin/pgbouncer -Rd /etc/pgbouncer/pgbouncer.ini -v

# Now install the fdw
cd /tmp
wget https://github.com/CrunchyData/pgbouncer_fdw/archive/refs/tags/v${PGBOUNCER_FDW_VERSION}.tar.gz
tar -xvf v${PGBOUNCER_FDW_VERSION}.tar.gz
cd pgbouncer_fdw-$PGBOUNCER_FDW_VERSION
make -j $JOBS
make install

## and then set it all up
sudo -i -u postgres \
     psql -d openaq \
     -c "BEGIN;" \
     -c "CREATE SERVER IF NOT EXISTS pgbouncer FOREIGN DATA WRAPPER dblink_fdw OPTIONS (host 'localhost',port '6432', dbname 'pgbouncer')" \
     -c "CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER pgbouncer OPTIONS (user '$DATABASE_MONITOR_USER', password '$DATABASE_MONITOR_PASSWORD')" \
     -c "CREATE EXTENSION IF NOT EXISTS pgbouncer_fdw" \
     -c "GRANT USAGE ON FOREIGN SERVER pgbouncer TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_clients TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_config TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_databases TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_dns_hosts TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_dns_zones TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_lists TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_pools TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_servers TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_sockets TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_stats TO $DATABASE_MONITOR_USER;" \
     -c "GRANT SELECT ON pgbouncer_users TO $DATABASE_MONITOR_USER;" \
     -c "COMMIT;"
