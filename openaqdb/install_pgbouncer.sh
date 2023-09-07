PGBOUNCER_VERSION=1.17.0
PGBOUNCER_FDW_VERSION=0.3
ADMIN_USER=$DATABASE_WRITE_USER
ADMIN_PASSWORD=$DATABASE_WRITE_PASSWORD
AUTH_TYPE=md5
POOL_SIZE=200 # 50
PORT=6432

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

;server_tls_sslmode = require
;client_tls_sslmode = require
;client_tls_ca_file = /etc/pgbouncer/certs/root.crt
;client_tls_key_file = /etc/pgbouncer/certs/server.key
;client_tls_cert_file = /etc/pgbouncer/certs/server.crt

auth_type = $AUTH_TYPE
auth_file = /etc/pgbouncer/userlist.txt

auth_user = $ADMIN_USER

admin_users = $ADMIN_USER
stats_users = $ADMIN_USER

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
"$DATABASE_READ_USER" "$DATABASE_READ_PASSWORD"
EOF
      )
echo $USERS | sudo -i -u postgres tee /etc/pgbouncer/userlist.txt  > /dev/null

# start the server and then disconnect
# if already running than reboot
sudo -i -u postgres /usr/local/bin/pgbouncer -Rd /etc/pgbouncer/pgbouncer.ini -v

sudo -i -u postgres \
     psql -d openaq \
     -c "BEGIN;" \
     -c "CREATE SERVER IF NOT EXISTS pgbouncer FOREIGN DATA WRAPPER dblink_fdw OPTIONS (host 'localhost',port '6432', dbname 'pgbouncer')" \
     -c "CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER pgbouncer OPTIONS (user '$DATABASE_WRITE_USER', password '$DATABASE_WRITE_PASSWORD')" \
     -c "CREATE EXTENSION IF NOT EXISTS pgbouncer_fdw" \
     -c "GRANT USAGE ON FOREIGN SERVER pgbouncer TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_clients TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_config TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_databases TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_dns_hosts TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_dns_zones TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_lists TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_pools TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_servers TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_sockets TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_stats TO $DATABASE_WRITE_USER;" \
     -c "GRANT SELECT ON pgbouncer_users TO $DATABASE_WRITE_USER;" \
     -c "COMMIT;"
