#!/bin/bash

wget https://github.com/evansiroky/timezone-boundary-builder/releases/download/2024a/timezones-with-oceans.geojson.zip
unzip timezones-with-oceans.geojson.zip
ogr2ogr -f CSV timezones.csv combined-with-oceans.json -dialect SQLite -sql "SELECT tzid, 'SRID=4326;' || ST_AsText(geometry) AS geog FROM combined-with-oceans"
gzip -c timezones.csv > timezones.csv.gz


