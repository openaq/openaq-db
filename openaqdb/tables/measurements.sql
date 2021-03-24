
CREATE TABLE measurements (
    sensors_id integer,
    datetime timestamp with time zone,
    value double precision,
    lon double precision,
    lat double precision
);


CREATE INDEX measurements_datetime_idx ON measurements USING btree (datetime);

CREATE INDEX measurements_lat_idx ON measurements USING brin (lat);

CREATE INDEX measurements_lon_idx ON measurements USING brin (lon);

CREATE UNIQUE INDEX measurements_sensors_id_datetime_idx ON measurements USING btree (sensors_id, datetime);

CREATE INDEX measurements_value_idx ON measurements USING brin (value);

SELECT create_hypertable('measurements', 'datetime', chunk_time_interval=>'1 month'::interval);

--ALTER TABLE measurements set (timescaledb.compress, timescaledb.compress_segmentby = 'sensors_id');

--SELECT add_compression_policy('measurements', '3 month'::interval);

CREATE TABLE analyses (LIKE measurements);
CREATE INDEX analyses_datetime_idx on analyses USING BTREE(datetime);
CREATE INDEX analyses_sensors_id_idx on analyses USING BTREE(sensors_id);