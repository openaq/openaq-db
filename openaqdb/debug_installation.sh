#!/bin/bash
echo 'Checking environment variables'
cat /etc/environment

echo 'Checking to see if the install finished'
sudo tail -n 10 /var/log/cloud-init-output.log

echo 'Checking if postgres is installed and running'
psql -XAwqt -U $DATABASE_WRITE_USER -d postgres -c 'SELECT TRUE'

echo 'Checking if openaq database was created'
psql -XAwqt -U $DATABASE_WRITE_USER -d openaq -c 'SELECT TRUE'

echo 'Check if the SQL install finished'
tail /app/openaqdb/openaq_install.log
