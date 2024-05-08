
#!/bin/bash
cd $( dirname "${BASH_SOURCE[0]}")

# defaults
PRODUCTION_HOST=55.55.55.55
PRODUCTION_PORT=5432
LOCAL_PORT=5432
PRODUCTION_DBNAME=openaq
LOCAL_DBNAME=openaqdev
# Options are dump and load
MODE=

TEMP=$(getopt -n "$0" -a -l "prod-host:,prod-port:,prod-db:,local-db:,local-port:,mode:" -- -- "$@")

eval set --  "$TEMP"

while [ $# -gt 0 ]
do
    case "$1" in
        --prod-host) PRODUCTION_HOST="$2"; shift;;
        --prod-port) PRODUCTION_PORT="$2"; shift;;
        --local-port) LOCAL_PORT="$2"; shift;;
        --mode) MODE="$2"; shift;;
        --prod-db) PRODUCTION_DBNAME="$2"; shift;;
        --local-db) LOCAL_DBNAME="$2"; shift;;
        --) shift;;
    esac
    shift;
done


if [[ -z "$MODE" ]] || [[ "$MODE" = "dump" ]]; then
    pg_dump --host $PRODUCTION_HOST \
            --port $PRODUCTION_PORT \
            --username postgres \
            --column-inserts \
            --data-only \
            -t providers \
            -t entities \
            -t sensor_nodes \
            -t instruments \
            -t measurands \
            -t sensor_systems \
            -t sensors \
            $PRODUCTION_DBNAME > ./sensors_dump.sql
fi

if [[ -z "$MODE" ]] || [[ "$MODE" = "load" ]]; then
    psql \
        --single-transaction \
        -v ON_ERROR_STOP=1 \
        -h localhost \
        -p $LOCAL_PORT \
        -U postgres \
        -d $LOCAL_DBNAME \
        -c 'SET search_path = public' \
        -c 'TRUNCATE public.providers, public.entities, public.measurands CASCADE' \
        -f ./sensors_dump.sql
fi
