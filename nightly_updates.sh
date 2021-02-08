export OPENAQ_APIUSER_PW=
export OPENAQ_RWUSER_PW=
export PGUSER=
export PGPASSWORD=
export PGDATABASE=

/usr/bin/psql -c "refresh materialized view concurrently mobile_generalized;"
/usr/bin/psql -c "refresh materialized view concurrently mobile_gen_boxes;"
