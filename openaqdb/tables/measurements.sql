SET search_path = public;

CREATE SCHEMA IF NOT EXISTS _measurements_internal;

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


CREATE OR REPLACE FUNCTION create_partition(sd date, ed date) RETURNS text AS $$
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


CREATE OR REPLACE FUNCTION create_partition(dt date) RETURNS text AS $$
DECLARE
table_name text := 'measurements_'||to_char(dt, 'YYYYMM');
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS _measurements_internal.%s
          PARTITION OF measurements
          FOR VALUES
          FROM (''%s'')
          TO (''%s'');',
          table_name,
          date_trunc('month', dt),
          date_trunc('month', dt + '1month'::interval)
          );
   RETURN table_name;
END;
$$ LANGUAGE plpgsql;


-- create some tables
WITH dates AS (
SELECT generate_series('2016-01-01'::date, '2024-01-01'::date, '1month'::interval) as dt)
SELECT create_partition(dt::date)
FROM dates;


CREATE TABLE IF NOT EXISTS analyses (LIKE measurements);
CREATE INDEX IF NOT EXISTS analyses_datetime_idx on analyses USING BTREE(datetime);
CREATE INDEX IF NOT EXISTS analyses_sensors_id_idx on analyses USING BTREE(sensors_id);
