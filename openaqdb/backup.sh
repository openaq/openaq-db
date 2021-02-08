#!/bin/bash
mkdir -p backups
cd backups
mkdir -p measurements
# pg_dumpall -f globals.sql -g
# pg_dump -s -N _timescaledb_internal | grep -v _timescaledb_internal >schema.sql

bk () {
    echo "backing up $1"
    psql -Xc "copy $1 to stdout" | gzip > $1.tsv.gz
}

bk readmes

bk sensor_nodes
bk sensor_systems
bk sensors
bk measurands

bk sources
bk origins
bk sensor_nodes_sources

# bk sensor_nodes_history
# bk sensor_systems_history
# bk sensors_history
bk sensor_nodes_harrays

# daterange=$(psql -A -t -X -c "select min(datetime), max(datetime) from measurements")
daterange='2021-01-01|2021-01-13'
IFS='|'; read -ra startend <<< "$daterange"
start=$(date -d ${startend[0]} +%Y%m%d)
end=$(date -d ${startend[1]} +%Y%m%d)
echo $start
echo $end

while [[ $start -le $end ]]
do
    echo $start
    psql -Xc "copy (select * from measurements where datetime >='${start}'::timestamptz and datetime < '${start}'::timestamptz + '1 day'::interval) to stdout" | gzip > measurements/meas_${start}.tsv.gz
    start=$(date -d"$start + 1 day" +"%Y%m%d")
done
cd ..
