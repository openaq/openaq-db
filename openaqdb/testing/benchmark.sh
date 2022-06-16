PG_MAJOR=13
export PGDATA=/var/lib/pgsql/$PG_MAJOR/data
CORES=$(grep -c 'cpu[0-9]' /proc/stat)

# create the benchmark tables
sudo -i -u postgres psql -d postgres -c 'CREATE DATABASE tuning ';
sudo -i -u postgres pgbench -i -s 100 tuning
sudo -i -u postgres psql -c "SELECT pg_size_pretty(pg_database_size('tuning'))";

# baseline
sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET shared_buffers='128MB'";
sudo -i -u postgres pg_ctl -D $PGDATA restart
sudo -i -u postgres psql -d postgres -c "SHOW shared_buffers";
sudo -i -u postgres pgbench -c 10 -j 2 -t 10000 tuning
# 2782
# 5108 io2=3000, s=100

sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET shared_buffers='256MB'";
sudo -i -u postgres pg_ctl -D $PGDATA restart
sudo -i -u postgres psql -d postgres -c "SHOW shared_buffers";
sudo -i -u postgres pgbench -c 10 -j 2 -t 10000 tuning
# 2202
# 3843
# 1581 @ s=1000

sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET shared_buffers='512MB'";
sudo -i -u postgres pg_ctl -D $PGDATA restart
sudo -i -u postgres psql -d postgres -c "SHOW shared_buffers";
sudo -i -u postgres pgbench -c 10 -j 2 -t 10000 tuning
# 3874

sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET shared_buffers='1024MB'";
sudo -i -u postgres pg_ctl -D $PGDATA restart
sudo -i -u postgres psql -d postgres -c "SHOW shared_buffers";
sudo -i -u postgres pgbench -c 10 -j 2 -t 10000 tuning
# 3986
# 4052
# 3882
# 1976 @ s=1000
# 4558 @ io2=3000, s=100
# 5925 @ io2=3000, s=100
# 3228 @ io2=2000, s=100
# 5592 @ io2=2000, s=100

sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET shared_buffers='2048MB'";
sudo -i -u postgres pg_ctl -D $PGDATA restart
sudo -i -u postgres psql -d postgres -c "SHOW shared_buffers";
sudo -i -u postgres pgbench -c 10 -j 2 -t 10000 tuning
# 3988
# 5890 @ io2=3000, s=100
# 5199 @ io2=2000, s=100

sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET shared_buffers='9700MB'";
sudo -i -u postgres pg_ctl -D $PGDATA restart
sudo -i -u postgres psql -d postgres -c "SHOW shared_buffers";
sudo -i -u postgres pgbench -c 10 -j 2 -t 10000 tuning
# 3661

sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET shared_buffers='8GB'";
sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET effective_cache_size='4GB'";
sudo -i -u postgres pg_ctl -D $PGDATA restart
sudo -i -u postgres psql -d postgres -c "SHOW shared_buffers";
sudo -i -u postgres pgbench -c 10 -j 2 -t 10000 tuning

sudo -i -u postgres psql -d postgres -c "ALTER SYSTEM SET shared_buffers='8GB'";
sudo -i -u postgres pg_ctl -D $PGDATA restart
sudo -i -u postgres psql -d postgres -c "SHOW shared_buffers";
sudo -i -u postgres pgbench -c 10 -j 2 -t 10000 tuning
# 3661
# 2042 @ s=1000
# 5830 @ io2=3000, s=100
# 5904 @ io2=3000, s=100
# 4588 @ io2=2000, s=100

# An example of using pgbench for some custom scripts
# benchmark_script.sql should contain some common
sudo -i -u postgres pgbench \
     -f /app/openaqdb/testing/benchmark_script.sql \
     -n \
     -r \
     -c 10 \
     -j $CORES \
     -t 100 \
     openaq
