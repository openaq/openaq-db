# This script can be modified rerun the rollup tables for a period of time.
export OPENAQ_APIUSER_PW=
export OPENAQ_RWUSER_PW=
export PGUSER=
export PGPASSWORD=
export PGDATABASE=
start=$(date +"%Y-%m-01" -d "-1 month")
end=$(date +"%Y-%m-01")
end=$(date -I -d "$end + 1 day")
while [[ "$start" < "$end" ]]; do
    next=$(date -I -d "$end - 1 day")
    echo "Refreshing from $next to $end"
    /usr/bin/psql -c "call run_updates(null,'{\"start\":\"$next\",\"end\":\"$next\"}');"
    end=$next
done