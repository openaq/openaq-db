SET search_path = public;

CREATE SCHEMA IF NOT EXISTS _measurements_internal;

CREATE SEQUENCE IF NOT EXISTS data_tables_sq START 10;
CREATE TABLE IF NOT EXISTS data_tables (
  data_tables_id int PRIMARY KEY DEFAULT nextval('data_tables_sq')
 , table_schema text NOT NULL
 , table_name text NOT NULL
 --, table_stats jsonb NOT NULL DEFAULT '{}'
 , calculated_on timestamp NOT NULL DEFAULT now()
 , UNIQUE(table_schema, table_name)
);

CREATE SEQUENCE IF NOT EXISTS data_table_partitions_sq START 10;
CREATE TABLE IF NOT EXISTS data_table_partitions (
  data_table_partitions_id int PRIMARY KEY DEFAULT nextval('data_table_partitions_sq')
  , data_tables_id int NOT NULL REFERENCES data_tables
  , table_schema text NOT NULL
  , table_name text NOT NULL
  , start_date date NOT NULL
  , end_date date NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS partitions_stats_sq START 10;
CREATE TABLE IF NOT EXISTS partitions_stats (
    data_table_partitions_id int PRIMARY KEY REFERENCES data_table_partitions
  , table_size bigint NOT NULL
  , index_size bigint NOT NULL
  , row_count bigint
  , calculated_on timestamptz NOT NULL DEFAULT now()
);




CREATE TABLE IF NOT EXISTS measurements (
    sensors_id integer,
    datetime timestamp with time zone,
    value double precision,
    lon double precision,
    lat double precision,
    added_on timestamp with time zone DEFAULT now()
) PARTITION BY RANGE (datetime);

CREATE INDEX IF NOT EXISTS measurements_datetime_idx
ON measurements USING btree (datetime);

CREATE INDEX IF NOT EXISTS measurements_lat_idx
ON measurements USING brin (lat);

CREATE INDEX IF NOT EXISTS measurements_lon_idx
ON measurements USING brin (lon);

CREATE INDEX IF NOT EXISTS measurements_added_on_idx
ON measurements USING btree (added_on);

CREATE UNIQUE INDEX IF NOT EXISTS measurements_sensors_id_datetime_idx
ON measurements USING btree (sensors_id, datetime);

CREATE INDEX IF NOT EXISTS measurements_value_idx
ON measurements USING brin (value);

-- not used but here just in case we need it. Helpful for migrating timescale data
CREATE OR REPLACE FUNCTION create_measurements_partition(sd date, ed date) RETURNS text AS $$
DECLARE
table_name text := 'measurements_'||to_char(sd, 'YYYYMMDD')||||to_char(ed, '_YYYYMMDD');
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS _measurements_internal.%s
          PARTITION OF measurements
          FOR VALUES
          FROM (''%s'')
          TO (''%s'');',
          table_name,
          sd,
          ed
          );
   RETURN table_name;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_measurements_partition(dt date) RETURNS text AS $$
DECLARE
_table_schema text := '_measurements_internal';
_table_name text := 'measurements_'||to_char(dt, 'YYYYMM');
sd date := date_trunc('month', dt);
ed date := date_trunc('month', dt + '1month'::interval);
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS %s.%s
          PARTITION OF measurements
          FOR VALUES
          FROM (''%s'')
          TO (''%s'');',
          _table_schema,
          _table_name,
          sd,
          ed
          );
   -- register that table
   INSERT INTO data_table_partitions (
   data_tables_id
   , table_schema
   , table_name
   , start_date
   , end_date)
   SELECT data_tables_id
   , _table_schema
   , _table_name
   , sd
   , ed
   FROM data_tables
   WHERE table_schema = 'public'
   AND table_name = 'measurements';
   RETURN _table_name;
END;
$$ LANGUAGE plpgsql;


INSERT INTO data_tables (data_tables_id, table_schema, table_name) VALUES
(1, 'public', 'measurements');

-- create some tables
WITH dates AS (
SELECT generate_series('2016-01-01'::date, '2024-01-01'::date, '1month'::interval) as dt)
SELECT create_measurements_partition(dt::date)
FROM dates;


CREATE TABLE IF NOT EXISTS analyses (LIKE measurements);
CREATE INDEX IF NOT EXISTS analyses_datetime_idx on analyses USING BTREE(datetime);
CREATE INDEX IF NOT EXISTS analyses_sensors_id_idx on analyses USING BTREE(sensors_id);
