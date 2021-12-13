export PGDATABASE=openaq

/usr/bin/psql -c "refresh materialized view concurrently mobile_generalized;"
/usr/bin/psql -c "refresh materialized view concurrently mobile_gen_boxes;"
