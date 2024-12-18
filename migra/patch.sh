#!/bin/bash
start=`date +%s`
PORT=5432
USER=postgres
#HOST=localhost
DB=openaq

while getopts ":f:h:p:u:" opt
do
   case "$opt" in
      f ) FILE="$OPTARG" ;;
      p ) PORT="$OPTARG" ;;
      h ) HOST="$OPTARG" ;;
      u ) USER="$OPTARG" ;;
      d ) DB="$OPTARG" ;;
   esac
done


if [ -z ${HOST} ]; then
    printf '%s\n' "Missing host" >&2
    exit 1
fi;

if [ -z ${DB} ]; then
    printf '%s\n' "Missing db" >&2
    exit 1
fi;

if [ -z ${FILE} ]; then
    printf '%s\n' "Missing file" >&2
    exit 1
fi;

FILES=(-f "$FILE")

PGOPTIONS='--client-min-messages=warning' psql \
         -v ON_ERROR_STOP=1 \
         -h $HOST \
         -p $PORT \
         -U $USER \
         -d $DB \
         -c "BEGIN;" \
         "${FILES[@]}" \
         -c "COMMIT;"

echo 'TOTAL TIME:' $((`date +%s`-start))
