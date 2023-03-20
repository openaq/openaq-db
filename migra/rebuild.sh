#!/bin/bash
start=`date +%s`
PORT=5432
USER=postgres

while getopts ":h:p:u:" opt
do
   case "$opt" in
      p ) PORT="$OPTARG" ;;
      h ) HOST="$OPTARG" ;;
      u ) USER="$OPTARG" ;;
   esac
done


if [ -z ${HOST} ]; then
    printf '%s\n' "Missing host" >&2
    exit 1
fi;


# Drop and then recreate
psql -h $HOST \
     -U $USER \
     -p $PORT \
     -d postgres \
     -v ON_ERROR_STOP=1 \
     -v DATABASE_WRITE_USER=rwuser \
     -c 'DROP DATABASE IF EXISTS openaq' \
     -c 'CREATE DATABASE openaq'

# switch db and build
cd ../openaqdb

psql -h $HOST \
     -U $USER \
     -p $PORT \
     -d openaq \
     -P pager=off \
     -v ON_ERROR_STOP=1 \
     -v DATABASE_WRITE_USER=rwuser \
     -f init.sql
