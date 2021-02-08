#!/bin/bash
BACKUPDIR=$1
source .env
cd $BACKUPDIR

restore () {
    echo "restoring $1"
    gunzip -c $1.tsv.gz | psql -c "copy $1 from stdin"
}

restore readmes
restore origins
restore measurands
restore sensor_nodes_harrays
restore sensor_nodes
restore sensor_systems
restore sensors
restore sources
restore sensor_nodes_sources

for i in measurements/meas_*.tsv.gz
do
    echo $i
    gunzip -c $i | psql -c "copy measurements from stdin"
done