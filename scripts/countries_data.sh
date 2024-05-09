#!/bin/bash

wget https://github.com/nvkelso/natural-earth-vector/archive/refs/tags/v5.1.2.tar.gz
tar -xvzf v5.1.2.tar.gz
ogr2ogr -f CSV countries.csv natural-earth-vector-5.1.2/geojson/ne_10m_admin_0_countries.geojson -dialect SQLite -sql "SELECT NAME_LONG as name, ISO_A3_EH AS iso_a3, ISO_A2_EH AS iso, 'SRID=4326;' || ST_AsText(geometry) AS geog FROM ne_10m_admin_0_countries"
gzip -c countries.csv > countries.csv.gz
