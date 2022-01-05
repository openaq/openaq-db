export PGDATABASE=openaq
st=${1:-$(date -d '-1 day' '+%Y-%m-%d')}
et=${1:-$(date '+%Y-%m-%d')}
st=$(date -d $st '+%Y-%m-%d')
et=$(date -d $et '+%Y-%m-%d')
/usr/bin/psql <<EOSQL
call run_updates(null, '{"start":"$st","end":"$et"}'::jsonb);
EOSQL