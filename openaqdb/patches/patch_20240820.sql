----------------
alter table "public"."rollups" drop constraint "rollups_groups_id_fkey";
----------------
alter table "public"."sensors_latest" drop constraint "sensors_latest_sensors_id_fkey";
----------------
drop index if exists "public"."locations_base_v2_country_idx";
----------------
drop index if exists "public"."locations_base_v2_geog_idx";
----------------
drop index if exists "public"."locations_base_v2_id_idx";
----------------
drop index if exists "public"."locations_base_v2_lastUpdated_idx";
----------------
drop index if exists "public"."locations_base_v2_name_idx";
----------------
drop index if exists "public"."locations_base_v2_parameters_idx";
----------------
drop index if exists "public"."locations_bounds_idx";
----------------
drop index if exists "public"."locations_geom_idx";
----------------
drop index if exists "public"."locations_last_datetime_idx";
----------------
drop index if exists "public"."locations_location_id_idx";
----------------
drop index if exists "public"."locations_measurands_id_idx";
----------------
drop index if exists "public"."sensor_nodes_json_geog_idx";
----------------
drop index if exists "public"."sensor_nodes_json_json_idx";
----------------
drop index if exists "public"."sensor_stats_sensors_id_idx";
----------------
drop index if exists "public"."sensors_first_last_sensors_id_idx";
----------------
drop function if exists "public"."calculate_daily_data_jobs"(n integer);
----------------
drop function if exists "public"."calculate_rollup_daily_stats"(day date);
----------------
drop materialized view if exists "public"."city_stats";
----------------
drop materialized view if exists "public"."country_stats";
----------------
drop function if exists "public"."get_closest_countries_id"(g geometry, w integer);
----------------
drop function if exists "public"."initialize_daily_stats"(sd date, ed date);
----------------
drop procedure if exists "public"."intialize_sensors_rollup"();
----------------
drop materialized view if exists "public"."locations";
----------------
drop materialized view if exists "public"."locations_base_v2";
----------------
drop materialized view if exists "public"."sensor_nodes_json";
----------------
drop view if exists "public"."sensor_systems_json";
----------------
drop materialized view if exists "public"."sensors_first_last";
----------------
drop materialized view if exists "public"."sensors_full_flat";
----------------
drop view if exists "public"."sensors_full_json";
----------------
drop function if exists "public"."as_timestamptz"(tstz timestamp with time zone, tz text);
----------------
drop function if exists "public"."as_timestamptz"(tstz timestamp without time zone, tz text);
----------------
drop function if exists "public"."as_utc"(dt timestamp with time zone, tz text);
----------------
drop function if exists "public"."calculate_hourly_data"(dt date);
----------------
drop function if exists "public"."calculate_hourly_data"(et timestamp with time zone);
----------------
drop function if exists "public"."calculate_hourly_data"(id integer, et timestamp with time zone);
----------------
drop materialized view if exists "public"."locations_view_cached";
----------------
drop view if exists "public"."sensors_full";
----------------
drop view if exists "public"."locations_view";
----------------
drop materialized view if exists "public"."sensor_stats";
----------------
alter table "public"."rollups" drop constraint "rollups_pkey";
----------------
alter table "public"."sensors_latest" drop constraint "sensors_latest_pkey";
----------------
drop index if exists "public"."rollups_measurands_id_idx";
----------------
drop index if exists "public"."rollups_pkey";
----------------
drop index if exists "public"."rollups_rollup_idx";
----------------
drop index if exists "public"."rollups_sensors_id_idx";
----------------
drop index if exists "public"."rollups_st_idx";
----------------
drop index if exists "public"."sensors_latest_pkey";
----------------
drop table IF EXISTS "public"."rollups";
----------------
drop table IF EXISTS "public"."sensors_latest";
----------------
drop table IF EXISTS "public"."sensors_rollup_patch";
----------------
create table "public"."annual_data" (
    "sensors_id" integer not null,
    "datetime" date not null,
    "datetime_first" timestamp with time zone not null,
    "datetime_last" timestamp with time zone not null,
    "value_count" integer not null,
    "value_avg" double precision,
    "value_sd" double precision,
    "value_min" double precision,
    "value_max" double precision,
    "value_p02" double precision,
    "value_p25" double precision,
    "value_p50" double precision,
    "value_p75" double precision,
    "value_p98" double precision,
    "value_raw_avg" double precision,
    "value_raw_count" double precision,
    "value_raw_min" double precision,
    "value_raw_max" double precision,
    "error_count" integer,
    "error_raw_count" integer,
    "updated_on" timestamp with time zone,
    "calculated_on" timestamp with time zone,
    "calculated_count" integer default 1
);

----------------
create table "public"."annual_data_queue" (
    "datetime" date not null,
    "tz_offset" integer not null,
    "added_on" timestamp with time zone not null default now(),
    "queued_on" timestamp with time zone,
    "modified_on" timestamp with time zone,
    "modified_count" integer not null default 0,
    "calculated_on" timestamp with time zone,
    "calculated_count" integer not null default 0,
    "calculated_seconds" real,
    "sensor_nodes_count" integer,
    "sensors_count" integer,
    "measurements_count" integer,
    "measurements_raw_count" integer
);

----------------
create table "public"."annual_stats" (
    "datetime" date not null,
    "added_on" timestamp with time zone not null default now(),
    "modified_on" timestamp with time zone,
    "calculated_count" integer not null default 0,
    "updated_on" timestamp with time zone,
    "calculated_on" timestamp with time zone,
    "sensor_nodes_count" integer,
    "measurements_count" integer,
    "measurements_raw_count" integer,
    "sensors_count" integer
);

----------------
create table "public"."daily_exported_stats" (
    "day" date not null,
    "sensor_nodes_count" bigint not null,
    "sensors_count" bigint not null,
    "hours_count" bigint not null,
    "measurements_count" bigint not null,
    "export_path" text,
    "calculated_on" timestamp without time zone,
    "initiated_on" timestamp without time zone,
    "exported_on" timestamp without time zone,
    "metadata" jsonb
);

----------------
alter table "public"."analyses_summary" drop column "first_datetime";
----------------
alter table "public"."analyses_summary" drop column "last_datetime";
----------------
alter table "public"."analyses_summary" add column "datetime_first" timestamp with time zone;
----------------
alter table "public"."analyses_summary" add column "datetime_last" timestamp with time zone;
----------------
alter table "public"."daily_data" drop column "first_datetime";
----------------
alter table "public"."daily_data" drop column "last_datetime";
----------------
alter table "public"."daily_data" add column "datetime_first" timestamp with time zone not null;
----------------
alter table "public"."daily_data" add column "datetime_last" timestamp with time zone not null;
----------------
alter table "public"."daily_data_queue" add column "calculated_seconds" real;
----------------
alter table "public"."hourly_data" drop column "first_datetime";
----------------
alter table "public"."hourly_data" drop column "last_datetime";
----------------
alter table "public"."hourly_data" add column "datetime_first" timestamp with time zone not null;
----------------
alter table "public"."hourly_data" add column "datetime_last" timestamp with time zone not null;
----------------
alter table "public"."licenses" alter column "attribution_required" set not null;
----------------
alter table "public"."licenses" alter column "commercial_use_allowed" set not null;
----------------
alter table "public"."licenses" alter column "modification_allowed" set not null;
----------------
alter table "public"."licenses" alter column "redistribution_allowed" set not null;
----------------
alter table "public"."licenses" alter column "share_alike_required" set not null;
----------------
alter table "public"."sensors_rollup" alter column "datetime_first" set not null;
----------------
alter table "public"."sensors_rollup" alter column "datetime_last" set not null;
----------------
alter table "public"."sensors_rollup" alter column "value_avg" set not null;
----------------
alter table "public"."sensors_rollup" alter column "value_latest" set not null;
----------------
alter table "public"."sensors_rollup" alter column "value_max" set not null;
----------------
alter table "public"."sensors_rollup" alter column "value_min" set not null;
----------------
alter table "public"."sensors_rollup" alter column "value_sd" set not null;
----------------
drop extension if exists "pgbouncer_fdw";
----------------
CREATE INDEX annual_data_day_idx ON public.annual_data USING btree (datetime);
----------------
CREATE UNIQUE INDEX annual_data_queue_datetime_tz_offset_key ON public.annual_data_queue USING btree (datetime, tz_offset);
----------------
CREATE UNIQUE INDEX annual_data_sensors_id_datetime_key ON public.annual_data USING btree (sensors_id, datetime);
----------------
CREATE INDEX annual_data_sensors_id_idx ON public.annual_data USING btree (sensors_id);
----------------
CREATE UNIQUE INDEX annual_stats_pkey ON public.annual_stats USING btree (datetime);
----------------
CREATE UNIQUE INDEX daily_exported_stats_day_key ON public.daily_exported_stats USING btree (day);
----------------
alter table "public"."annual_stats" add constraint "annual_stats_pkey" PRIMARY KEY using index "annual_stats_pkey";
----------------
alter table "public"."annual_data" add constraint "annual_data_sensors_id_datetime_key" UNIQUE using index "annual_data_sensors_id_datetime_key";
----------------
alter table "public"."annual_data" add constraint "annual_data_sensors_id_fkey" FOREIGN KEY (sensors_id) REFERENCES sensors(sensors_id) ON DELETE CASCADE not valid;
----------------
alter table "public"."annual_data" validate constraint "annual_data_sensors_id_fkey";
----------------
alter table "public"."annual_data_queue" add constraint "annual_data_queue_datetime_tz_offset_key" UNIQUE using index "annual_data_queue_datetime_tz_offset_key";
----------------
alter table "public"."daily_data" add constraint "daily_data_sensors_id_fkey" FOREIGN KEY (sensors_id) REFERENCES sensors(sensors_id) ON DELETE CASCADE not valid;
----------------
alter table "public"."daily_data" validate constraint "daily_data_sensors_id_fkey";
----------------
alter table "public"."daily_exported_stats" add constraint "daily_exported_stats_day_key" UNIQUE using index "daily_exported_stats_day_key";
----------------
set check_function_bodies = off;
----------------
CREATE OR REPLACE FUNCTION public.as_local(dt timestamp with time zone, tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT timezone(tz, dt);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_year(tstz timestamp with time zone, tz text)
 RETURNS date
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT date_trunc('year', timezone(tz, tstz + '-1sec'::interval))::date;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_annual_data_by_offset(dy date DEFAULT (CURRENT_DATE - 1), tz_offset integer DEFAULT 0)
 RETURNS TABLE(sensors_id integer, sensor_nodes_id integer, datetime date, updated_on timestamp with time zone, datetime_first timestamp with time zone, datetime_last timestamp with time zone, value_count bigint, value_avg real, value_sd real, value_min real, value_max real, value_raw_count bigint, value_raw_avg real, value_raw_min real, value_raw_max real, value_p02 real, value_p25 real, value_p50 real, value_p75 real, value_p98 real, error_raw_count bigint, error_count bigint)
 LANGUAGE sql
AS $function$
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_year(m.datetime, t.tzid)  as datetime
, MAX(m.updated_on) as updated_on
, MIN(datetime_first) as datetime_first
, MAX(datetime_last) as datetime_last
, COUNT(1) AS value_count
, AVG(value_avg) as value_avg
, STDDEV(value_avg) as value_sd
, MIN(value_avg) as value_min
, MAX(value_avg) as value_max
, SUM(value_count) as value_raw_count
, SUM(value_avg*value_count)/SUM(value_count) as value_raw_avg
, MIN(value_min) as value_raw_min
, MAX(value_max) as value_raw_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value_avg) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value_avg) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value_avg) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value_avg) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value_avg) as value_p98
, SUM(error_count) as error_raw_count
, SUM((value_avg IS NULL)::int) as error_count
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE value_count > 0
AND datetime > as_utc(date_trunc('year', dy), t.tzid)
AND datetime <= as_utc(date_trunc('year', dy + '1year'::interval), t.tzid)
AND utc_offset_hours(dy, t.tzid) = tz_offset
GROUP BY 1,2,3
HAVING COUNT(1) > 0;
  $function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_daily_data_by_offset(dy date DEFAULT (CURRENT_DATE - 1), tz_offset integer DEFAULT 0)
 RETURNS TABLE(sensors_id integer, sensor_nodes_id integer, datetime date, updated_on timestamp with time zone, datetime_first timestamp with time zone, datetime_last timestamp with time zone, value_count bigint, value_avg real, value_sd real, value_min real, value_max real, value_raw_count bigint, value_raw_avg real, value_raw_min real, value_raw_max real, value_p02 real, value_p25 real, value_p50 real, value_p75 real, value_p98 real, error_raw_count bigint, error_count bigint)
 LANGUAGE sql
AS $function$
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_date(m.datetime, t.tzid)  as datetime
, MAX(m.updated_on) as updated_on
, MIN(datetime_first) as datetime_first
, MAX(datetime_last) as datetime_last
, COUNT(1) AS value_count
, AVG(value_avg) as value_avg
, STDDEV(value_avg) as value_sd
, MIN(value_avg) as value_min
, MAX(value_avg) as value_max
, SUM(value_count) as value_raw_count
, SUM(value_avg*value_count)/SUM(value_count) as value_raw_avg
, MIN(value_min) as value_raw_min
, MAX(value_max) as value_raw_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value_avg) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value_avg) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value_avg) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value_avg) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value_avg) as value_p98
, SUM(error_count) as error_raw_count
, SUM((value_avg IS NULL)::int) as error_count
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE value_count > 0
AND datetime > as_utc(dy, t.tzid)
AND datetime <= as_utc(dy + 1, t.tzid)
AND utc_offset_hours(dy, t.tzid) = tz_offset
GROUP BY 1,2,3
HAVING COUNT(1) > 0;
  $function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_rollup_daily_exported_stats(day date)
 RETURNS bigint
 LANGUAGE sql
AS $function$
WITH data AS (
   SELECT (datetime - '1sec'::interval)::date as day
   , h.sensors_id
   , sensor_nodes_id
   , value_count
   FROM hourly_data h
   JOIN sensors s ON (h.sensors_id = s.sensors_id)
   JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
   WHERE datetime > day::timestamp
   AND  datetime <= day + '1day'::interval
), inserts AS (
INSERT INTO daily_exported_stats (
  day
, sensor_nodes_count
, sensors_count
, hours_count
, measurements_count
, calculated_on
)
SELECT day
, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
, COUNT(DISTINCT sensors_id) as sensors_count
, COUNT(1) as hours_count
, SUM(value_count) as measurements_count
, current_timestamp
FROM data
GROUP BY day
ON CONFLICT (day) DO UPDATE
SET sensor_nodes_count = EXCLUDED.sensor_nodes_count
, sensors_count = EXCLUDED.sensors_count
, hours_count = EXCLUDED.hours_count
, measurements_count = EXCLUDED.measurements_count
, calculated_on = EXCLUDED.calculated_on
RETURNING measurements_count)
SELECT measurements_count
FROM inserts;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.daily_data_updated_event(dy date, tz_offset_int integer)
 RETURNS boolean
 LANGUAGE sql
AS $function$
 SELECT update_annual_data_queue(dy, tz_offset_int)>0;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.fetch_annual_data_jobs(n integer DEFAULT 1, min_day date DEFAULT NULL::date, max_day date DEFAULT NULL::date)
 RETURNS TABLE(datetime date, tz_offset integer, queued_on timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
  BEGIN
        RETURN QUERY
        UPDATE annual_data_queue
        SET queued_on = CURRENT_TIMESTAMP
        , calculated_count = calculated_count + 1
        FROM (
          SELECT q.datetime
          , q.tz_offset
          FROM annual_data_queue q
          -- Its either not been calculated or its been modified
          WHERE q.datetime >= COALESCE(min_day, '-infinity'::date)
          AND q.datetime <= COALESCE(max_day, current_date - '1year'::interval)
          AND (q.calculated_on IS NULL OR q.modified_on > q.calculated_on)
          -- either its never been or it was resently modified but not queued
          AND (q.queued_on IS NULL -- has not been queued
          OR (
              q.queued_on < now() - '1h'::interval -- a set amount of time has passed AND
              AND (
                q.queued_on < q.modified_on  -- its been changed since being queued
                OR calculated_on IS NULL     -- it was never calculated
              )
          ))
          ORDER BY q.datetime, q.tz_offset
          LIMIT n
          FOR UPDATE SKIP LOCKED
        ) as d
        WHERE d.datetime = annual_data_queue.datetime
        AND d.tz_offset = annual_data_queue.tz_offset
        RETURNING annual_data_queue.datetime
        , annual_data_queue.tz_offset
        , annual_data_queue.queued_on;
  END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_datetime_object(tstz date, tz text DEFAULT 'UTC'::text)
 RETURNS json
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
  SELECT get_datetime_object(tstz::timestamp, tz);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_measurands_id(m text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT measurands_id
FROM measurands
WHERE lower(measurand) = lower(m)
LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.hourly_data_updated_event(hr timestamp with time zone)
 RETURNS boolean
 LANGUAGE sql
AS $function$
 SELECT update_daily_data_queue(hr)>0;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.initialize_daily_exported_stats(sd date DEFAULT '-infinity'::date, ed date DEFAULT 'infinity'::date)
 RETURNS bigint
 LANGUAGE sql
AS $function$
WITH first_and_last AS (
SELECT MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
FROM measurements
WHERE datetime >= sd
AND datetime <= ed
), datetimes AS (
SELECT generate_series(
   date_trunc('day', datetime_first)
   , date_trunc('day', datetime_last)
   , '1day'::interval) as day
FROM first_and_last
), inserts AS (
INSERT INTO daily_exported_stats (day, sensor_nodes_count, sensors_count, measurements_count, hours_count)
SELECT day::date, -1, -1, -1, -1
FROM datetimes
WHERE has_measurement(day::date)
ON CONFLICT (day) DO NOTHING
RETURNING 1)
SELECT COUNT(1) FROM inserts;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.insert_annual_data_by_offset(dy date DEFAULT (CURRENT_DATE - 1), tz_offset integer DEFAULT 0)
 RETURNS TABLE(sensor_nodes_count bigint, sensors_count bigint, measurements_hourly_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SET LOCAL work_mem = '512MB';
WITH data_rollup AS (
  SELECT *
  FROM calculate_annual_data_by_offset(dy, tz_offset)
), data_inserted AS (
INSERT INTO annual_data (
  sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, calculated_on)
	SELECT sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, current_timestamp as calculated_on
	FROM data_rollup
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, updated_on = EXCLUDED.updated_on
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_raw_avg = EXCLUDED.value_raw_avg
, value_raw_min = EXCLUDED.value_raw_min
, value_raw_max = EXCLUDED.value_raw_max
, value_raw_count = EXCLUDED.value_raw_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, error_raw_count = EXCLUDED.error_raw_count
, calculated_on = EXCLUDED.calculated_on
  RETURNING sensors_id, value_count, value_raw_count
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_hourly_count
	, SUM(value_raw_count) as measurements_count
	FROM data_rollup;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.insert_daily_data_by_offset(dy date DEFAULT (CURRENT_DATE - 1), tz_offset integer DEFAULT 0)
 RETURNS TABLE(sensor_nodes_count bigint, sensors_count bigint, measurements_hourly_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SET LOCAL work_mem = '512MB';
WITH data_rollup AS (
  SELECT *
  FROM calculate_daily_data_by_offset(dy, tz_offset)
), data_inserted AS (
INSERT INTO daily_data (
  sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, calculated_on)
	SELECT sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, current_timestamp as calculated_on
	FROM data_rollup
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, updated_on = EXCLUDED.updated_on
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_raw_avg = EXCLUDED.value_raw_avg
, value_raw_min = EXCLUDED.value_raw_min
, value_raw_max = EXCLUDED.value_raw_max
, value_raw_count = EXCLUDED.value_raw_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, error_raw_count = EXCLUDED.error_raw_count
, calculated_on = EXCLUDED.calculated_on
  RETURNING sensors_id, value_count, value_raw_count
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_hourly_count
	, SUM(value_raw_count) as measurements_count
	FROM data_rollup;
$function$
;
----------------
create or replace view "public"."location_licenses_view" as  SELECT sn.sensor_nodes_id,
    json_agg(json_build_object('id', pl.licenses_id, 'name', l.name, 'date_from', lower(pl.active_period), 'date_to', upper(pl.active_period), 'attribution', json_build_object('name', e.full_name, 'url', COALESCE((e.metadata ->> 'url'::text), NULL::text)))) AS licenses,
    array_agg(DISTINCT pl.licenses_id) AS license_ids
   FROM (((providers_licenses pl
     JOIN sensor_nodes sn USING (providers_id))
     JOIN entities e ON ((sn.owner_entities_id = e.entities_id)))
     JOIN licenses l ON ((l.licenses_id = pl.licenses_id)))
  GROUP BY sn.sensor_nodes_id;

----------------
CREATE OR REPLACE PROCEDURE public.update_annual_data(IN n integer DEFAULT 5, IN min_day date DEFAULT NULL::date, IN max_day date DEFAULT NULL::date)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  rw record;
BEGIN
FOR rw IN (
    SELECT datetime
    , tz_offset
     FROM fetch_annual_data_jobs(n, min_day, max_day))
LOOP
  RAISE NOTICE 'updating year: % - %', rw.datetime, rw.tz_offset;
  PERFORM update_annual_data(rw.datetime, rw.tz_offset);
  COMMIT;
END LOOP;
END;
$procedure$
;
----------------
CREATE OR REPLACE FUNCTION public.update_annual_data(dy date DEFAULT (CURRENT_DATE - 1), tz_offset_int integer DEFAULT 0)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
nw timestamptz := clock_timestamp();
mc bigint;
BEGIN
WITH inserted AS (
  SELECT sensor_nodes_count
  , sensors_count
  , measurements_hourly_count
  , measurements_count
  FROM insert_annual_data_by_offset(dy, tz_offset_int))
  INSERT INTO annual_data_queue (
    datetime
  , tz_offset
  , calculated_on
  , calculated_count
  , sensor_nodes_count
  , sensors_count
  , measurements_count
  , measurements_raw_count
  , calculated_seconds
  )
  SELECT dy
  , tz_offset_int
  , now()
  , 1
  , sensor_nodes_count
  , sensors_count
  , measurements_hourly_count
  , measurements_count
  , EXTRACT(EPOCH FROM clock_timestamp() - nw)
  FROM inserted
  ON CONFLICT (datetime, tz_offset) DO UPDATE
  SET calculated_on = EXCLUDED.calculated_on
  , calculated_count = annual_data_queue.calculated_count + 1
  , measurements_count = EXCLUDED.measurements_count
  , sensors_count = EXCLUDED.sensors_count
  , calculated_seconds = EXCLUDED.calculated_seconds
  RETURNING measurements_count INTO mc;
  -- PERFORM annual_data_updated_event(dy, tz_offset_int);
  RETURN mc;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.update_annual_data_queue(dt date, tz_offset_int integer)
 RETURNS bigint
 LANGUAGE sql
AS $function$
 WITH annual_inserts AS (
  INSERT INTO annual_data_queue (datetime, tz_offset) VALUES
  (date_trunc('year', dt + make_interval(hours=>tz_offset_int, secs=>-1))
  , tz_offset_int)
  ON CONFLICT (datetime, tz_offset) DO UPDATE
  SET modified_on = now()
  , modified_count = annual_data_queue.modified_count + 1
  RETURNING datetime, tz_offset
  ) SELECT COUNT(*)
  FROM annual_inserts;
  $function$
;
----------------
CREATE OR REPLACE PROCEDURE public.update_daily_data(IN n integer DEFAULT 5, IN min_day date DEFAULT NULL::date, IN max_day date DEFAULT NULL::date)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  rw record;
BEGIN
FOR rw IN (
    SELECT datetime
    , tz_offset
     FROM fetch_daily_data_jobs(n, min_day, max_day))
LOOP
  RAISE NOTICE 'updating day: % - %', rw.datetime, rw.tz_offset;
  PERFORM update_daily_data(rw.datetime, rw.tz_offset);
  COMMIT;
END LOOP;
END;
$procedure$
;
----------------
CREATE OR REPLACE FUNCTION public.update_daily_data(dy date DEFAULT (CURRENT_DATE - 1), tz_offset_int integer DEFAULT 0)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
nw timestamptz := clock_timestamp();
mc bigint;
BEGIN
WITH inserted AS (
  SELECT sensor_nodes_count
  , sensors_count
  , measurements_hourly_count
  , measurements_count
  FROM insert_daily_data_by_offset(dy, tz_offset_int))
  INSERT INTO daily_data_queue (
    datetime
  , tz_offset
  , calculated_on
  , calculated_count
  , sensor_nodes_count
  , sensors_count
  , measurements_count
  , measurements_raw_count
  , calculated_seconds
  )
  SELECT dy
  , tz_offset_int
  , now()
  , 1
  , sensor_nodes_count
  , sensors_count
  , measurements_hourly_count
  , measurements_count
  , EXTRACT(EPOCH FROM clock_timestamp() - nw)
  FROM inserted
  ON CONFLICT (datetime, tz_offset) DO UPDATE
  SET calculated_on = EXCLUDED.calculated_on
  , calculated_count = daily_data_queue.calculated_count + 1
  , measurements_count = EXCLUDED.measurements_count
  , sensors_count = EXCLUDED.sensors_count
  , calculated_seconds = EXCLUDED.calculated_seconds
  RETURNING measurements_count INTO mc;
  PERFORM daily_data_updated_event(dy, tz_offset_int);
  RETURN mc;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.update_daily_data_queue(dt timestamp with time zone)
 RETURNS bigint
 LANGUAGE sql
AS $function$
 WITH affected_offsets AS (
  -- the following will just queue up every possible offset
  -- regardless of whether we have a sensor node
  -- SELECT generate_series(-12, 14, 1) as tz_offset
  -- and this will only do the queue the offsets that we have
  SELECT utc_offset_hours(dt, tzid) as tz_offset
  FROM sensor_nodes n
  JOIN timezones t USING (timezones_id)
  GROUP BY 1
 ), daily_inserts AS (
  INSERT INTO daily_data_queue (datetime, tz_offset)
  SELECT (dt + make_interval(hours=>tz_offset::int, secs=>-1))::date
  , tz_offset
  FROM affected_offsets
  ON CONFLICT (datetime, tz_offset) DO UPDATE
  SET modified_on = now()
  , modified_count = daily_data_queue.modified_count + 1
  RETURNING datetime, tz_offset
  ) SELECT COUNT(*)
  FROM daily_inserts;
  $function$
;
----------------
CREATE OR REPLACE FUNCTION public.utc_offset_hours(dt date, tz text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT date_part('hours', utc_offset(dt::timestamptz,tz)) as tz_offset
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.utc_offset_hours(dt timestamp with time zone, tz text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT date_part('hours', utc_offset(dt,tz)) as tz_offset
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.utc_offset_hours(tz text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT date_part('hours', utc_offset(tz)) as tz_offset
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.array_distinct(anyarray, boolean DEFAULT false)
 RETURNS anyarray
 LANGUAGE sql
 IMMUTABLE
AS $function$
      SELECT array_agg(DISTINCT x)
      FROM unnest($1) t(x)
      WHERE CASE WHEN $2 THEN x IS NOT NULL ELSE true END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.array_merge(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
    SELECT
            CASE
                WHEN $1 IS NULL THEN $2
                WHEN $2 IS NULL THEN $1
                ELSE $1 || $2
            END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_date(tstz timestamp with time zone, tz text)
 RETURNS date
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT date_trunc('day', timezone(tz, tstz + '-1sec'::interval))::date;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_local_hour(dt timestamp with time zone, tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT timezone(tz, date_trunc('hour', dt));
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_local_hour(tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT timezone(tz, date_trunc('hour', now()));
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_local_hour_int(tz text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT date_part('hour', timezone(tz, current_time));
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_timestamptz(tstz date, tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT timezone(tz, tstz::timestamp);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_timestamptz(tstz timestamp with time zone, tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT tstz;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_timestamptz(tstz timestamp without time zone, tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT timezone(tz, tstz);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_utc(dt timestamp with time zone, tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT timezone(tz, timezone('UTC', dt));
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.as_utc(dt timestamp without time zone, tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT timezone(tz, dt);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.bbox(geom geometry)
 RETURNS double precision[]
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT ARRAY[st_x(geom),st_y(geom),st_x(geom),st_y(geom)];
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.bench(query text, iterations integer DEFAULT 100)
 RETURNS TABLE(avg_n numeric, sd_n numeric, r double precision, avg double precision, sd double precision, min double precision, q1 double precision, median double precision, q3 double precision, p95 double precision, max double precision)
 LANGUAGE plpgsql
AS $function$
DECLARE
  _start TIMESTAMPTZ;
  _end TIMESTAMPTZ;
  _delta DOUBLE PRECISION;
	_records INT;
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS _bench_results (
      elapsed DOUBLE PRECISION,
		  n INT
  );
  -- Warm the cache
  FOR i IN 1..5 LOOP
    EXECUTE query;
  END LOOP;
  -- Run test and collect elapsed time into _bench_results table
  FOR i IN 1..iterations LOOP
    _start = clock_timestamp();
    EXECUTE query INTO _records;
    _end = clock_timestamp();
    _delta = 1000 * ( extract(epoch from _end) - extract(epoch from _start) );
		--GET DIAGNOSTICS _records = ROW_COUNT;
    INSERT INTO _bench_results VALUES (_delta, _records);
  END LOOP;

  RETURN QUERY SELECT
	  avg(n),
	  stddev(n),
		corr(n::float, elapsed),
    avg(elapsed),
	  stddev(elapsed),
    min(elapsed),
    percentile_cont(0.25) WITHIN GROUP (ORDER BY elapsed),
    percentile_cont(0.5) WITHIN GROUP (ORDER BY elapsed),
    percentile_cont(0.75) WITHIN GROUP (ORDER BY elapsed),
    percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed),
    max(elapsed)
    FROM _bench_results;
  DROP TABLE IF EXISTS _bench_results;

END
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.bounds(double precision, double precision, double precision, double precision)
 RETURNS geometry
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT st_setsrid(st_makebox2d(st_makepoint($1,$2),st_makepoint($3,$4)),4326);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_coverage(obs integer, averaging numeric, logging numeric, dt_first timestamp with time zone, dt_last timestamp with time zone)
 RETURNS jsonb
 LANGUAGE sql
 PARALLEL SAFE
AS $function$
SELECT calculate_coverage(
	obs
	, averaging
	, logging
	, EXTRACT(EPOCH FROM dt_last - dt_first)
	);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_coverage(obs integer, averaging numeric DEFAULT 3600, logging numeric DEFAULT 3600, dur numeric DEFAULT 3600)
 RETURNS jsonb
 LANGUAGE sql
 PARALLEL SAFE
AS $function$
SELECT jsonb_build_object(
       'observed_count', obs
       , 'observed_interval', make_interval(secs => averaging * obs)
       , 'expected_count', ROUND(dur/logging)
       , 'expected_interval', make_interval(secs => (dur/logging) * averaging)
       , 'percent_complete', ROUND((obs/(dur/logging))*100.0)
       , 'percent_coverage', ROUND((obs/(dur/averaging))*100.0)
       );
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_daily_data(dy date DEFAULT (CURRENT_DATE - 1), lag integer DEFAULT 2)
 RETURNS TABLE(sensor_nodes_count bigint, sensors_count bigint, measurements_hourly_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SET LOCAL work_mem = '512MB';
WITH sensors_rollup AS (
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_date(m.datetime, t.tzid)  as datetime
, MAX(m.updated_on) as updated_on
, MIN(datetime_first) as datetime_first
, MAX(datetime_last) as datetime_last
, COUNT(1) AS value_count
, AVG(value_avg) as value_avg
, STDDEV(value_avg) as value_sd
, MIN(value_avg) as value_min
, MAX(value_avg) as value_max
, SUM(value_count) as value_raw_count
, SUM(value_avg*value_count)/SUM(value_count) as value_raw_avg
, MIN(value_min) as value_raw_min
, MAX(value_max) as value_raw_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value_avg) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value_avg) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value_avg) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value_avg) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value_avg) as value_p98
, SUM(error_count) as error_raw_count
, SUM((value_avg IS NULL)::int) as error_count
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE value_count > 0
AND datetime > as_utc(dy, t.tzid)
AND datetime <= as_utc(dy + 1, t.tzid)
AND as_local_hour_int(t.tzid) = lag
GROUP BY 1,2,3
HAVING COUNT(1) > 0
	), inserted AS (
INSERT INTO daily_data (
  sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, calculated_on)
	SELECT sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, current_timestamp as calculated_on
	FROM sensors_rollup
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, updated_on = EXCLUDED.updated_on
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_raw_avg = EXCLUDED.value_raw_avg
, value_raw_min = EXCLUDED.value_raw_min
, value_raw_max = EXCLUDED.value_raw_max
, value_raw_count = EXCLUDED.value_raw_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, error_raw_count = EXCLUDED.error_raw_count
, calculated_on = EXCLUDED.calculated_on
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_hourly_count
	, SUM(value_raw_count) as measurements_count
	FROM sensors_rollup;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_daily_data(lag integer DEFAULT 2)
 RETURNS TABLE(sensor_nodes_count bigint, sensors_count bigint, measurements_hourly_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SELECT * FROM calculate_daily_data(current_date - 1, lag);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_daily_data_full(dt date DEFAULT CURRENT_DATE)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
	DECLARE
	 o record;
	 obj json;
	BEGIN
  FOR o IN SELECT generate_series(0,23,1) as tz_offset
	LOOP
		PERFORM calculate_daily_data(dt, o.tz_offset);
	END LOOP;
	-- update the stats table
	SELECT upsert_daily_stats(dt) INTO obj;
	RETURN obj;
	END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_daily_data_jobs(n integer, min_day date DEFAULT '-infinity'::date, max_day date DEFAULT 'infinity'::date)
 RETURNS interval
 LANGUAGE plpgsql
AS $function$
	DECLARE
	 rw record;
   nw timestamptz;
   nodes bigint;
   sensors bigint;
   meas bigint;
   hours bigint;
	BEGIN
  nw = clock_timestamp();
  FOR rw IN (SELECT datetime, tz_offset FROM fetch_daily_data_jobs(n,min_day,max_day)) LOOP
    RAISE NOTICE 'Calculating % - %', rw.datetime, rw.tz_offset;
    -- calculate
    SELECT sensor_nodes_count
    , sensors_count
    , measurements_count
    , measurements_hourly_count
    INTO nodes, sensors, meas, hours
    FROM calculate_daily_data(rw.datetime, rw.tz_offset);
    -- update queue table
    UPDATE daily_data_queue
    SET calculated_on = clock_timestamp()
    , sensor_nodes_count = nodes
    , sensors_count = sensors
    , measurements_count = hours
    , measurements_raw_count = meas
    WHERE datetime = rw.datetime
    AND tz_offset = rw.tz_offset;
	END LOOP;
	RETURN clock_timestamp() - nw;
	END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_export_stats(ci interval)
 RETURNS timestamp with time zone
 LANGUAGE sql
AS $function$
WITH m AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE modified_on > added_on
  AND modified_on > now() - ci
), a AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE added_on > now() - ci
), e AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE exported_on > now() - ci
), p AS (
  SELECT COUNT(1) as n
  FROM open_data_export_logs
  WHERE exported_on IS NULL
  AND has_error = FALSE
)
INSERT INTO export_stats (
    stats_interval
  , days_modified
  , days_added
  , days_exported
  , days_pending
  , calculated_on)
SELECT ci as stats_interval
, m.n as days_modified
, a.n as days_added
, e.n as days_exported
, p.n as days_pending
, now() as calculated_on
FROM m,a,e,p
ON CONFLICT (stats_interval) DO UPDATE
SET days_modified = EXCLUDED.days_modified
, days_added = EXCLUDED.days_added
, days_exported = EXCLUDED.days_exported
, days_pending = EXCLUDED.days_pending
, calculated_on = EXCLUDED.calculated_on
RETURNING calculated_on;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_hourly_data(dt date)
 RETURNS TABLE(sensors_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SELECT * FROM calculate_hourly_data(dt::timestamptz, dt + '1day'::interval);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_hourly_data(et timestamp with time zone DEFAULT (now() - '01:00:00'::interval))
 RETURNS TABLE(sensors_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SELECT * FROM calculate_hourly_data(et - '1hour'::interval, et);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_hourly_data(id integer, dt date)
 RETURNS TABLE(sensors_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SELECT calculate_hourly_data(id, dt::timestamptz, dt + '1day'::interval);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_hourly_data(id integer, et timestamp with time zone)
 RETURNS TABLE(sensors_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SELECT calculate_hourly_data(id, et - '1hour'::interval, et);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_hourly_data(id integer, st timestamp with time zone, et timestamp with time zone)
 RETURNS TABLE(sensors_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
WITH measurements_filtered AS (
	SELECT
  	m.sensors_id
	, measurands_id
	, date_trunc('hour', datetime - '1sec'::interval) + '1hour'::interval as datetime
	, CASE WHEN value < -900 THEN NULL ELSE value END as value
	FROM measurements m
	JOIN sensors s ON (m.sensors_id = s.sensors_id)
	WHERE m.sensors_id = id
	AND datetime > date_trunc('hour', st)
	AND datetime <= date_trunc('hour', et)
), inserted AS (
INSERT INTO hourly_data (
  sensors_id
, measurands_id
, datetime
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, calculated_on
) SELECT
  sensors_id
, measurands_id
, datetime
, MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
, COUNT(1) as value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value) as value_p98
, SUM((value IS NULL)::int)
, current_timestamp as calculated_on
FROM measurements_filtered m
GROUP BY 1,2,3
HAVING COUNT(1) > 0
ON CONFLICT (sensors_id, measurands_id, datetime) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, calculated_on = EXCLUDED.calculated_on
RETURNING value_count)
SELECT COUNT(1) as sensors_count
, SUM(value_count) as measurements_count
FROM inserted;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_hourly_data(st timestamp with time zone, et timestamp with time zone)
 RETURNS TABLE(sensors_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SET LOCAL work_mem = '512MB';
WITH measurements_filtered AS (
	SELECT
  	m.sensors_id
	, measurands_id
	, date_trunc('hour', datetime - '1sec'::interval) + '1hour'::interval as datetime
	, CASE WHEN value < -900 THEN NULL ELSE value END as value
	FROM measurements m
	JOIN sensors s ON (m.sensors_id = s.sensors_id)
	WHERE datetime > date_trunc('hour', st)
	AND datetime <= date_trunc('hour', et)
), inserted AS (
INSERT INTO hourly_data (
  sensors_id
, measurands_id
, datetime
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, calculated_on
) SELECT
  sensors_id
, measurands_id
, datetime
, MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
, COUNT(1) as value_count
, AVG(value) as value_avg
, STDDEV(value) as value_sd
, MIN(value) as value_min
, MAX(value) as value_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value) as value_p98
, SUM((value IS NULL)::int)
, current_timestamp as calculated_on
FROM measurements_filtered m
GROUP BY 1,2,3
HAVING COUNT(1) > 0
ON CONFLICT (sensors_id, measurands_id, datetime) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, calculated_on = EXCLUDED.calculated_on
RETURNING value_count)
SELECT COUNT(1) as sensors_count
, SUM(value_count) as measurements_count
FROM inserted;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_hourly_data_partial(st timestamp with time zone)
 RETURNS TABLE(sensors_count bigint, measurements_count bigint)
 LANGUAGE plpgsql
AS $function$
DECLARE
	--__st timestamptz := '2023-05-18 15:00:00+00'::timestamptz;
	et timestamptz;
	__calculated_on timestamptz := '-infinity';
	__ns bigint;
	__nm bigint;
BEGIN
	st := date_trunc('hour', st);
	et := st + '1hour'::interval;
	SELECT calculated_on INTO __calculated_on
	FROM hourly_stats h
	WHERE h.datetime = st;
	---
	WITH sensors AS (
	  SELECT sensors_id
		FROM measurements
  	WHERE datetime > st
  	AND datetime <= et
		AND added_on > __calculated_on
		GROUP BY sensors_id
	), hourly AS (
		SELECT (stats).measurements_count
		FROM sensors, calculate_hourly_data(sensors_id, st, et) as stats
	) SELECT COUNT(1)
	, SUM(h.measurements_count) INTO __ns, __nm
		FROM hourly h;
	---
	RETURN QUERY
	SELECT COUNT(1) as sensors_count
	, SUM(value_count) as measurements_count
	FROM hourly_data
	WHERE datetime = st;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_next_available_day()
 RETURNS json
 LANGUAGE sql
AS $function$
  WITH days AS (
	SELECT MIN(datetime) as datetime_min
	, MAX(datetime) as datetime_max
	FROM hourly_data
	), available_days AS (
	SELECT generate_series(
   datetime_min::date
	, datetime_max::date
  , '1day'::interval)::date as datetime
	FROM days
	), selected_day AS (
	SELECT a.datetime
	FROM available_days a
	LEFT JOIN daily_stats s ON (a.datetime = s.datetime)
	WHERE s.datetime IS NULL
	ORDER BY a.datetime ASC
	LIMIT 1
	) SELECT calculate_daily_data_full(datetime)
	FROM selected_day;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_partition_stats()
 RETURNS timestamp with time zone
 LANGUAGE sql
AS $function$
WITH measurement_tables AS (
  SELECT data_table_partitions_id
  , ('"' || table_schema || '"."' || table_name || '"') AS table_name
  FROM data_table_partitions
), measurement_sizes AS (
  SELECT data_table_partitions_id
  , table_name
  , pg_table_size(table_name) as table_size
  , pg_indexes_size(table_name) as index_size
  FROM measurement_tables
), stats_inserted AS (
  INSERT INTO partitions_stats (
  data_table_partitions_id
  , table_size
  , index_size
  , row_count
  , calculated_on
  )
  SELECT data_table_partitions_id
  , table_size
  , index_size
  , row_count_estimate(table_name) as row_count
  , now() as calculated_on
  FROM measurement_sizes
  ON CONFLICT(data_table_partitions_id) DO UPDATE
  SET table_size = EXCLUDED.table_size
  , index_size = EXCLUDED.index_size
  , row_count = EXCLUDED.row_count
  , calculated_on = EXCLUDED.calculated_on
  RETURNING calculated_on
  ) SELECT MAX(calculated_on) FROM stats_inserted;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.calculate_sensor_daily_data(id integer, sd date, ed date)
 RETURNS TABLE(sensor_nodes_count bigint, sensors_count bigint, measurements_hourly_count bigint, measurements_count bigint)
 LANGUAGE sql
AS $function$
SET LOCAL work_mem = '512MB';
WITH sensors_rollup AS (
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_date(m.datetime, t.tzid)  as datetime
, MAX(m.updated_on) as updated_on
, MIN(datetime_first) as datetime_first
, MAX(datetime_last) as datetime_last
, COUNT(1) AS value_count
, AVG(value_avg) as value_avg
, STDDEV(value_avg) as value_sd
, MIN(value_avg) as value_min
, MAX(value_avg) as value_max
, SUM(value_count) as value_raw_count
, SUM(value_avg*value_count)/SUM(value_count) as value_raw_avg
, MIN(value_min) as value_raw_min
, MAX(value_max) as value_raw_max
, PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value_avg) as value_p02
, PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value_avg) as value_p25
, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value_avg) as value_p50
, PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value_avg) as value_p75
, PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value_avg) as value_p98
, SUM(error_count) as error_raw_count
, SUM((value_avg IS NULL)::int) as error_count
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE value_count > 0
AND datetime > as_utc(sd, t.tzid)
AND datetime <= as_utc(ed, t.tzid)
AND m.sensors_id = id
GROUP BY 1,2,3
HAVING COUNT(1) > 0
	), inserted AS (
INSERT INTO daily_data (
  sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, calculated_on)
	SELECT sensors_id
, datetime
, updated_on
, datetime_first
, datetime_last
, value_count
, value_avg
, value_sd
, value_min
, value_max
, value_raw_count
, value_raw_avg
, value_raw_min
, value_raw_max
, value_p02
, value_p25
, value_p50
, value_p75
, value_p98
, error_count
, error_raw_count
, current_timestamp as calculated_on
	FROM sensors_rollup
ON CONFLICT (sensors_id, datetime) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, updated_on = EXCLUDED.updated_on
, value_avg = EXCLUDED.value_avg
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_count = EXCLUDED.value_count
, value_raw_avg = EXCLUDED.value_raw_avg
, value_raw_min = EXCLUDED.value_raw_min
, value_raw_max = EXCLUDED.value_raw_max
, value_raw_count = EXCLUDED.value_raw_count
, value_p02 = EXCLUDED.value_p02
, value_p25 = EXCLUDED.value_p25
, value_p50 = EXCLUDED.value_p50
, value_p75 = EXCLUDED.value_p75
, value_p98 = EXCLUDED.value_p98
, error_count = EXCLUDED.error_count
, error_raw_count = EXCLUDED.error_raw_count
, calculated_on = EXCLUDED.calculated_on
	) SELECT COUNT(DISTINCT sensors_id) as sensors_count
	, COUNT(DISTINCT sensor_nodes_id) as sensor_nodes_count
	, SUM(value_count) as measurements_hourly_count
	, SUM(value_raw_count) as measurements_count
	FROM sensors_rollup;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.cancel_ingestions(age interval DEFAULT '04:00:00'::interval)
 RETURNS TABLE(pid integer, locks bigint, age interval, canceled bigint)
 LANGUAGE sql
AS $function$
 SELECT pid
, COUNT(1) as locks
, MAX(age(now(), query_start)) as process_age
, COUNT(pg_cancel_backend(pid)) as canceled
FROM pg_stat_activity
WHERE query~*'ingest'
AND pg_backend_pid() != pid
AND age(now(), query_start) > age
GROUP BY 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.cancel_processes(pattern text, age interval DEFAULT '04:00:00'::interval)
 RETURNS TABLE(pid integer, locks bigint, age interval, canceled bigint)
 LANGUAGE sql
AS $function$
 SELECT pid
, COUNT(1) as locks
, MAX(age(now(), query_start)) as process_age
, COUNT(pg_cancel_backend(pid)) as canceled
FROM pg_stat_activity
WHERE query~*pattern
AND pg_backend_pid() != pid
AND age(now(), query_start) > age
GROUP BY 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.check_entities_path()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  IF NEW.entities_path IS NOT NULL THEN
    IF index(NEW.entities_path, NEW.entities_id::text::ltree, 0)<0 THEN
      NEW.entities_path = NEW.entities_path||NEW.entities_id::text::ltree;
    END IF;
  ELSE
      NEW.entities_path = NEW.entities_id::text::ltree;
  END IF;
  RETURN NEW;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.check_flagged_measurements()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  measurands_check boolean;
BEGIN
  IF NEW.measurands_id IS NOT NULL THEN
    -- -- check against current ids
    SELECT NEW.measurands_id <@ array_agg(measurands_id)
    INTO measurands_check
    FROM measurands;
    IF NOT measurands_check THEN
        RAISE EXCEPTION 'Measurand not found';
    END IF;
  END IF;
  RETURN NEW;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.check_ingest_id(iid text)
 RETURNS TABLE(ingest_id text, source_name text, source_id text, parameter text, measurands_id integer, measurand text, units text)
 LANGUAGE sql
AS $function$
WITH arr AS (
SELECT iid as ingest_id
, split_ingest_id(iid) as iid
), sen AS (
  SELECT ingest_id
  , iid[1] as source_name
  , iid[2] as source_id
  , iid[3] as parameter
  FROM arr)
SELECT s.*
, m.measurands_id
, m.measurand
, m.units
FROM sen s
LEFT JOIN measurands_map_view mp ON (mp.key = s.parameter)
LEFT JOIN measurands m ON (mp.measurands_id = m.measurands_id);
$function$
;
----------------
CREATE OR REPLACE PROCEDURE public.check_metadata()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
BEGIN
    ----------------------------------------
    ----------------------------------------
    RAISE NOTICE 'UPDATING missing providers';
    UPDATE sensor_nodes
    SET providers_id = providers.providers_id
    FROM providers
    WHERE lower(sensor_nodes.source_name) = lower(providers.source_name)
    AND sensor_nodes.providers_id IS NULL;
    ----------------------------------------
    ----------------------------------------
    RAISE NOTICE 'UPDATING missing averaging periods';
    UPDATE sensors
    SET data_averaging_period_seconds = ROUND((metadata->>'data_averaging_period_seconds')::numeric)::int
    , data_logging_period_seconds = ROUND((metadata->>'data_averaging_period_seconds')::numeric)::int
    WHERE data_averaging_period_seconds IS NULL
    AND metadata->>'data_averaging_period_seconds' IS NOT NULL;
    ---------
    UPDATE sensors
    SET data_averaging_period_seconds = 60
    , data_logging_period_seconds = 60
    WHERE source_id ~* 'senstate'
    AND data_averaging_period_seconds IS NULL;
    ---------
    UPDATE sensors
    SET data_averaging_period_seconds = 120
    , data_logging_period_seconds = 120
    WHERE source_id ~* 'purple'
    AND data_averaging_period_seconds IS NULL;
    -----------
    UPDATE sensors
    SET data_averaging_period_seconds = 90
    , data_logging_period_seconds = 300
    WHERE source_id ~* 'clarity'
    AND data_averaging_period_seconds IS NULL;
    -----------
    UPDATE sensors
    SET data_averaging_period_seconds = 3600
    , data_logging_period_seconds = 3600
    WHERE source_id ~* 'airgradient'
    AND data_averaging_period_seconds IS NULL;
    -----------
    UPDATE sensors
    SET data_averaging_period_seconds = 1
    , data_logging_period_seconds = 1
	    WHERE source_id ~* 'habitatmap'
    AND data_averaging_period_seconds IS NULL;
		------------ make all openaq origin data gov
	  --- source_name, entities_id, instruments_id
	  PERFORM update_instruments('openaq', 4, 2);
		PERFORM update_instruments('airgradient', 12, 7);
		PERFORM update_instruments('clarity', 9, 4);
		PERFORM update_instruments('purpleair', 8, 3);
		PERFORM update_instruments('habitatmap', 11, 5);
		PERFORM update_instruments('senstate', 10, 6);
END;
$procedure$
;
----------------
CREATE OR REPLACE FUNCTION public.clean_sensor_nodes()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
DELETE
FROM sensors
WHERE sensors_id IN (SELECT sensors_id FROM sensor_nodes_check WHERE datetime_first IS NULL);
DELETE
FROM sensor_systems
WHERE sensor_systems_id NOT IN (SELECT sensor_systems_id FROM sensors);
DELETE
FROM sensor_nodes_sources
WHERE sensor_nodes_id NOT IN (SELECT sensor_nodes_id FROM sensor_systems);
DELETE
FROM sensor_nodes
WHERE sensor_nodes_id NOT IN (SELECT sensor_nodes_id FROM sensor_systems);
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.countries(nodes integer[])
 RETURNS text[]
 LANGUAGE sql
AS $function$
WITH t AS (
        SELECT DISTINCT country
        FROM sensor_nodes WHERE
        sensor_nodes_id = ANY(nodes)
) SELECT array_agg(country) FROM t;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.country(g geography)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT iso from countries WHERE st_intersects(g, geog) LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.country(g geometry)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT iso from countries WHERE st_intersects(g::geography, geog) LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.create_hourly_data_partition(dt date)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
_table_schema text := '_measurements_internal';
_table_name text := 'hourly_data_'||to_char(dt, 'YYYYMM');
sd date := date_trunc('month', dt);
ed date := date_trunc('month', dt + '1month'::interval);
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS %s.%s
          PARTITION OF hourly_data
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
   AND table_name = 'hourly_data';
   RETURN _table_name;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.create_hourly_data_partition(sd date, ed date)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
table_name text := 'hourly_data_'||to_char(sd, 'YYYYMMDD')||||to_char(ed, '_YYYYMMDD');
BEGIN
  EXECUTE format('
          CREATE TABLE IF NOT EXISTS _measurements_internal.%s
          PARTITION OF hourly_data
          FOR VALUES
          FROM (''%s'')
          TO (''%s'');',
          table_name,
          sd,
          ed
          );
   RETURN table_name;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.create_list(_users_id integer, _label text DEFAULT 'My first list'::text, _description text DEFAULT 'A custom list of AQ monitoring sites.'::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
  _lists_id int;
BEGIN
  INSERT INTO
      lists (users_id, label, description)
  VALUES
      (_users_id, _label, _description);
  SELECT currval('lists_sq') INTO _lists_id;
  RETURN _lists_id;
END
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.create_measurements_partition(dt date)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
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
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.create_measurements_partition(sd date, ed date)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
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
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.create_user(full_name text, email_address text, password_hash text, ip_address text, entity_type text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
  users_id integer;
  entities_id integer;
  verification_token text;
BEGIN
  INSERT INTO
    users (email_address, password_hash, added_on, verification_code, expires_on, ip_address, is_active)
  VALUES
    (email_address, password_hash, NOW(), generate_token(), (timestamptz (NOW() + INTERVAL '30min') AT TIME ZONE 'UTC') AT TIME ZONE 'UTC', ip_address::cidr, FALSE)
  RETURNING users.users_id, verification_code INTO users_id, verification_token;

  INSERT INTO
    entities (full_name, entity_type, added_on)
  VALUES
    (full_name, entity_type::entity_type, NOW())
  RETURNING entities.entities_id INTO entities_id;

  INSERT INTO
    users_entities (users_id, entities_id)
  VALUES
    (users_id, entities_id);
  RETURN verification_token;
END
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.daily_data_check(sd date, ed date, sids integer[])
 RETURNS TABLE(sensors_id integer, sensor_nodes_id integer, datetime date, utc_offset interval, datetime_min timestamp with time zone, daettime_max timestamp with time zone, value_count bigint, value_raw_count bigint, value_avg double precision, value_raw_avg double precision)
 LANGUAGE sql
AS $function$
SELECT
  m.sensors_id
, sn.sensor_nodes_id
, as_date(m.datetime, t.tzid) as datetime
, utc_offset(t.tzid) as utc_offset
, MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
, COUNT(1) AS value_count
, SUM(value_count) as value_raw_count
, AVG(value_avg) as value_avg
, SUM(value_avg*value_count)/SUM(value_count) as value_raw_avg
FROM hourly_data m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
JOIN timezones t ON (sn.timezones_id = t.timezones_id)
WHERE value_count > 0
AND datetime > as_utc(sd, t.tzid)
AND datetime <= as_utc(ed, t.tzid)
AND m.sensors_id = ANY(sids)
GROUP BY 1,2,3,4
HAVING COUNT(1) > 0;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.delete_list(_lists_id integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
  DELETE FROM
    sensor_nodes_list
  WHERE
    lists_id = _lists_id;
  DELETE FROM
    users_lists
  WHERE
    lists_id = _lists_id;
  DELETE FROM
    lists
  WHERE
    lists_id = _lists_id;
END
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.deployment_sources(pid integer, aid integer)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
SELECT --jsonb_build_object('count', COUNT(1))
json_agg(metadata)
FROM providers p
JOIN adapters a ON (p.adapters_id = a.adapters_id)
WHERE (pid IS NULL AND aid IS NULL AND p.is_active)
OR (aid IS NULL AND p.providers_id = pid)
OR (pid IS NULL AND a.adapters_id = aid AND p.is_active);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.expected_hours(sd timestamp with time zone, ed timestamp with time zone, tp text, gp text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
  wf text; -- format to use for filtering
  sp interval; -- the expected step interval
  n int;
  dt text; -- how to truncate sd/ed
BEGIN
  IF tp = 'hour' THEN
    wf := 'HH24';
    sp := '1day -1sec'::interval;
    dt := 'day';
  ELSIF tp = 'day' THEN
    wf := 'ID';
    sp := '1day -1sec'::interval;
    dt := 'day';
  ELSIF tp = 'month' THEN
    wf := 'MM';
    sp := '1month -1sec'::interval;
    dt := 'month';
  END IF;
  SELECT COUNT(1) INTO n
  FROM generate_series(date_trunc(dt, sd), date_trunc(dt, ed - '1sec'::interval) + sp, '1hour'::interval) d
  WHERE to_char(d, wf) = gp;
  RETURN n;
END
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.fetch_daily_data_jobs(n integer DEFAULT 1, min_day date DEFAULT NULL::date, max_day date DEFAULT NULL::date)
 RETURNS TABLE(datetime date, tz_offset integer, queued_on timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
  BEGIN
        RETURN QUERY
        UPDATE daily_data_queue
        SET queued_on = CURRENT_TIMESTAMP
        , calculated_count = calculated_count + 1
        FROM (
          SELECT q.datetime
          , q.tz_offset
          FROM daily_data_queue q
          -- Its either not been calculated or its been modified
          WHERE q.datetime >= COALESCE(min_day, '-infinity'::date)
          AND q.datetime <= COALESCE(max_day, current_date - '1day'::interval)
          AND (q.calculated_on IS NULL OR q.modified_on > q.calculated_on)
          -- either its never been or it was resently modified but not queued
          AND (q.queued_on IS NULL -- has not been queued
          OR (
              q.queued_on < now() - '1h'::interval -- a set amount of time has passed AND
              AND (
                q.queued_on < q.modified_on  -- its been changed since being queued
                OR calculated_on IS NULL     -- it was never calculated
              )
          ))
          ORDER BY q.datetime, q.tz_offset
          LIMIT n
          FOR UPDATE SKIP LOCKED
        ) as d
        WHERE d.datetime = daily_data_queue.datetime
        AND d.tz_offset = daily_data_queue.tz_offset
        RETURNING daily_data_queue.datetime
        , daily_data_queue.tz_offset
        , daily_data_queue.queued_on;
  END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.first_notnull_agg(anyelement, anyelement)
 RETURNS anyelement
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
        SELECT coalesce($1, $2);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.format_timestamp(tstz timestamp with time zone, tz text DEFAULT 'UTC'::text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT replace(format(
                '%sT%s+%s',
                to_char(timezone(COALESCE(tz, 'UTC'), tstz), 'YYYY-MM-DD'),
                --timezone(tz, tstz)::time,
                to_char(timezone(COALESCE(tz, 'UTC'), tstz)::time, 'HH24:MI:SS'),
                to_char(timezone(COALESCE(tz, 'UTC'), tstz) - timezone('UTC',tstz), 'HH24:MI')
            ),'+-','-')
;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.generate_token()
 RETURNS text
 LANGUAGE sql
AS $function$
SELECT encode(digest(uuid_generate_v4():: text, 'sha256'), 'hex');
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_countries_id(g geography)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT countries_id from countries WHERE st_intersects(g, geog) LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_countries_id(g geometry)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT countries_id from countries WHERE st_intersects(g::geography, geog) LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_datetime_object(tstz timestamp with time zone, tz text DEFAULT 'UTC'::text)
 RETURNS json
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT json_build_object(
       'utc', format_timestamp(tstz, 'UTC')
     , 'local', format_timestamp(tstz, tz)
       , 'timezone', tz
     ) WHERE tstz IS NOT NULL;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_datetime_object(tstz timestamp without time zone, tz text DEFAULT 'UTC'::text)
 RETURNS json
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT json_build_object(
       'utc', format_timestamp(tstz AT TIME ZONE tz, 'UTC')
     , 'local', format_timestamp(tstz AT TIME ZONE tz, tz)
     , 'timezone', tz
     ) WHERE tstz IS NOT NULL;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_latest_measurement_added(rng tstzrange)
 RETURNS timestamp with time zone
 LANGUAGE sql
AS $function$
  SELECT MAX(added_on)
  FROM measurements
  WHERE datetime > lower(rng)
  AND datetime <= upper(rng);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_pending(lmt integer DEFAULT 100)
 RETURNS TABLE(sensor_nodes_id integer, day date, records integer, measurands integer, modified_on timestamp with time zone, queued_on timestamp with time zone, exported_on timestamp with time zone, utc_offset interval)
 LANGUAGE sql
AS $function$
WITH pending AS (
  SELECT *
  FROM pending_location_days
  LIMIT lmt
	FOR UPDATE SKIP LOCKED)
UPDATE public.open_data_export_logs
SET queued_on = now()
FROM pending
WHERE pending.day = open_data_export_logs.day
AND pending.sensor_nodes_id = open_data_export_logs.sensor_nodes_id
RETURNING pending.*;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_providers_id(p text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT providers_id
FROM providers
WHERE lower(source_name) = lower(p)
LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_timezones_id(g geometry)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
	SELECT timezones_id
	FROM timezones
	WHERE st_intersects(g::geography, geog)
	ORDER BY timezones_id ASC
	LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_timezones_id(tz text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
	SELECT timezones_id
	FROM timezones
	WHERE lower(tzid) = lower(tz)
	LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.get_user_token(_users_id integer, _label text DEFAULT 'general'::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
  _token text;
BEGIN
  UPDATE
      users
  SET
      verified_on = NOW(),
      is_active = TRUE
  WHERE
      users_id = _users_id;

  INSERT INTO
    user_keys (users_id, token, label, added_on)
  VALUES
    (_users_id, generate_token(), _label, NOW())
  ON CONFLICT (users_id, label) DO UPDATE
  SET token = EXCLUDED.token
  RETURNING token INTO _token;
  RETURN _token;
END
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.has_measurement(date)
 RETURNS boolean
 LANGUAGE sql
AS $function$
WITH m AS (
SELECT datetime
FROM measurements
WHERE datetime > $1
AND datetime <= $1 + '1day'::interval
LIMIT 1)
SELECT COUNT(1) > 0
FROM m;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.has_measurement(integer)
 RETURNS boolean
 LANGUAGE sql
AS $function$
WITH m AS (
SELECT datetime
FROM measurements
WHERE sensors_id = $1
LIMIT 1)
SELECT COUNT(1) > 0
FROM m;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.has_measurement(timestamp with time zone)
 RETURNS boolean
 LANGUAGE sql
AS $function$
WITH m AS (
SELECT datetime
FROM measurements
WHERE datetime = $1
LIMIT 1)
SELECT COUNT(1) > 0
FROM m;
$function$
;
----------------
CREATE OR REPLACE PROCEDURE public.initialize_sensors_rollup()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
BEGIN
  CREATE TEMP TABLE sensors_missing_from_rollup AS
  -- Get a list of all sensors missing data
  WITH missing AS (
    SELECT sensors_id
    FROM sensors
    LEFT JOIN sensors_rollup s USING (sensors_id)
    WHERE s.sensors_id IS NULL
    OR value_avg IS NULL
  ), data AS (
  -- use that list to aggregate based on the measurements
    SELECT m.sensors_id
    , MIN(datetime) as datetime_first
    , MAX(datetime) as datetime_last
    , COUNT(1) as value_count
    , AVG(value) as value_avg
    , STDDEV(value) as value_sd
    , MIN(value) as value_min
    , MAX(value) as value_max
    FROM missing m
    JOIN measurements USING (sensors_id)
    GROUP BY 1)
  -- now get the latest value
  SELECT d.sensors_id
  , d.datetime_first
  , d.datetime_last
  , d.value_count
  , d.value_avg
  , d.value_sd
  , d.value_min
  , d.value_max
  , m.value as value_latest
  FROM data d
  JOIN measurements m ON (d.datetime_last = m.datetime AND d.sensors_id = m.sensors_id);
  -- Now add those to the rollups
  INSERT INTO sensors_rollup (
  sensors_id
  , datetime_first
  , datetime_last
  , value_count
  , value_avg
  , value_sd
  , value_min
  , value_max
  , value_latest)
  SELECT
  sensors_id
  , datetime_first
  , datetime_last
  , value_count
  , value_avg
  , value_sd
  , value_min
  , value_max
  , value_latest
  FROM sensors_missing_from_rollup
ON CONFLICT (sensors_id) DO UPDATE
SET datetime_first = EXCLUDED.datetime_first
, datetime_last = EXCLUDED.datetime_last
, value_count  = EXCLUDED.value_count
, value_min = EXCLUDED.value_min
, value_max = EXCLUDED.value_max
, value_avg = EXCLUDED.value_avg
, value_latest = COALESCE(sensors_rollup.value_latest, EXCLUDED.value_latest);
END;
$procedure$
;
----------------
CREATE OR REPLACE FUNCTION public.jsonb_array(anyarray)
 RETURNS jsonb[]
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
SELECT array_agg(to_jsonb(a)) FROM unnest($1) a;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.jsonb_array(jsonb)
 RETURNS jsonb[]
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
SELECT array_agg(j) FROM jsonb_array_elements($1) j;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.jsonb_array_query(text, anyarray)
 RETURNS jsonb[]
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
WITH j AS (
SELECT jsonb_agg(jsonb_build_object($1, val)) j
FROM unnest($2) AS val
)
SELECT array_agg(
        j
) FROM j;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.jsonb_merge(jsonb, jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
        SELECT
            CASE
                WHEN $1 IS NULL THEN $2
                WHEN $2 IS NULL THEN $1
                ELSE $1 || $2
            END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.last_time_agg(anyelement, anyelement, timestamp with time zone)
 RETURNS anyelement
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
        SELECT $2;
$function$
;
----------------
create or replace view "public"."locations_view" as  WITH nodes_instruments AS (
         SELECT sn.sensor_nodes_id,
            bool_or(i.is_monitor) AS is_monitor,
            json_agg(json_build_object('id', i.instruments_id, 'name', i.label, 'manufacturer', jsonb_build_object('id', i.manufacturer_entities_id, 'name', mc.full_name))) AS instruments,
            array_agg(DISTINCT i.instruments_id) AS instrument_ids,
            array_agg(DISTINCT mc.full_name) AS manufacturers,
            array_agg(DISTINCT i.manufacturer_entities_id) AS manufacturer_ids
           FROM (((sensor_nodes sn
             JOIN sensor_systems ss USING (sensor_nodes_id))
             JOIN instruments i USING (instruments_id))
             JOIN entities mc ON ((mc.entities_id = i.manufacturer_entities_id)))
          GROUP BY sn.sensor_nodes_id
        ), nodes_sensors AS (
         SELECT sn.sensor_nodes_id,
            min(sl.datetime_first) AS datetime_first,
            max(sl.datetime_last) AS datetime_last,
            json_agg(jsonb_build_object('id', s.sensors_id, 'name', ((m.measurand || ' '::text) || m.units), 'parameter', jsonb_build_object('id', m.measurands_id, 'name', m.measurand, 'units', m.units, 'value_last', sl.value_latest, 'datetime_last', sl.datetime_last, 'display_name', m.display))) AS sensors,
            array_agg(DISTINCT m.measurand) AS parameters,
            array_agg(DISTINCT m.measurands_id) AS parameter_ids
           FROM ((((sensor_nodes sn
             JOIN sensor_systems ss USING (sensor_nodes_id))
             JOIN sensors s USING (sensor_systems_id))
             LEFT JOIN sensors_rollup sl USING (sensors_id))
             JOIN measurands m USING (measurands_id))
          GROUP BY sn.sensor_nodes_id
        )
 SELECT l.sensor_nodes_id AS id,
    l.site_name AS name,
    l.ismobile,
    t.tzid AS timezone,
    ni.is_monitor AS ismonitor,
    l.city,
    jsonb_build_object('id', c.countries_id, 'code', c.iso, 'name', c.name) AS country,
    jsonb_build_object('id', oc.entities_id, 'name', oc.full_name, 'type', oc.entity_type) AS owner,
    jsonb_build_object('id', p.providers_id, 'name', p.label) AS provider,
    jsonb_build_object('latitude', st_y(l.geom), 'longitude', st_x(l.geom)) AS coordinates,
    ni.instruments,
    ns.sensors,
    get_datetime_object(ns.datetime_first, t.tzid) AS datetime_first,
    get_datetime_object(ns.datetime_last, t.tzid) AS datetime_last,
    l.geom,
    (l.geom)::geography AS geog,
    c.countries_id,
    ns.parameters,
    ns.parameter_ids,
    ((oc.entity_type)::text ~* 'research'::text) AS is_analysis,
    ni.manufacturers,
    ni.manufacturer_ids,
    ni.instrument_ids,
    ll.licenses,
    ll.license_ids,
    l.providers_id
   FROM (((((((sensor_nodes l
     JOIN timezones t ON ((l.timezones_id = t.timezones_id)))
     JOIN countries c ON ((c.countries_id = l.countries_id)))
     JOIN entities oc ON ((oc.entities_id = l.owner_entities_id)))
     JOIN providers p ON ((p.providers_id = l.providers_id)))
     JOIN nodes_instruments ni USING (sensor_nodes_id))
     JOIN nodes_sensors ns USING (sensor_nodes_id))
     LEFT JOIN location_licenses_view ll USING (sensor_nodes_id))
  WHERE l.is_public;

----------------
create materialized view "public"."locations_view_cached" as  SELECT id,
    name,
    ismobile,
    timezone,
    ismonitor,
    city,
    country,
    owner,
    provider,
    coordinates,
    instruments,
    sensors,
    datetime_first,
    datetime_last,
    geom,
    geog,
    countries_id,
    parameters,
    parameter_ids,
    is_analysis,
    manufacturers,
    manufacturer_ids,
    instrument_ids,
    licenses,
    license_ids,
    providers_id
   FROM locations_view;

----------------
CREATE OR REPLACE FUNCTION public.log_performance(text, timestamp with time zone)
 RETURNS timestamp with time zone
 LANGUAGE sql
AS $function$
  INSERT INTO performance_log (process_name, start_datetime, end_datetime)
  VALUES (pg_backend_pid()||'-'||$1, $2, current_timestamp)
  RETURNING end_datetime;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.measurements_per_hour(dur interval DEFAULT '1 day'::interval)
 RETURNS TABLE(sensor_nodes_count bigint, sensors_count bigint, measurements_per_hour_expected double precision, measurements_per_hour_observed double precision)
 LANGUAGE sql
AS $function$
  SELECT COUNT(DISTINCT n.sensor_nodes_id) as sensor_nodes_count
  , COUNT(DISTINCT n.sensors_id) as sensors_count
  , ROUND(SUM(3600.0/s.data_logging_period_seconds)) as measurements_per_hour_expected
  , ROUND(SUM((3600.0/s.data_logging_period_seconds)*(percent_complete/100.0))) as measurements_per_hour_observed
  FROM sensor_nodes_check n
  JOIN sensors s USING (sensors_id)
  WHERE datetime_last > now() - dur;
  $function$
;
----------------
CREATE OR REPLACE FUNCTION public.measurements_per_hour(param text, dur interval DEFAULT '1 day'::interval)
 RETURNS TABLE(sensor_nodes_count bigint, sensors_count bigint, measurements_per_hour_expected double precision, measurements_per_hour_observed double precision)
 LANGUAGE sql
AS $function$
  SELECT COUNT(DISTINCT n.sensor_nodes_id) as sensor_nodes_count
  , COUNT(DISTINCT n.sensors_id) as sensors_count
  , ROUND(SUM(3600.0/s.data_logging_period_seconds)) as measurements_per_hour_expected
  , ROUND(SUM((3600.0/s.data_logging_period_seconds)*(percent_complete/100.0))) as measurements_per_hour_observed
  FROM sensor_nodes_check n
  JOIN sensors s USING (sensors_id)
  WHERE datetime_last > now() - dur
  AND n.parameter ~* param;
  $function$
;
----------------
CREATE OR REPLACE FUNCTION public.mfr(sensor_systems_metadata jsonb)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
WITH t AS (
        SELECT
        $1->>'manufacturer_name' as "manufacturerName",
        $1->>'model_name' as "modelName"
) SELECT
        CASE WHEN
        "manufacturerName" is not null AND
        "modelName" IS NOT NULL
        THEN
        to_jsonb(t)
        ELSE NULL END
        FROM t;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.node_from_group(integer)
 RETURNS integer
 LANGUAGE sql
AS $function$
WITH ids AS (
    SELECT $1 as groups_id
)
SELECT sensor_nodes_id FROM
ids
LEFT JOIN groups_sensors USING (groups_id)
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN sensor_systems USING (sensor_systems_id)
;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.node_from_sensor(integer)
 RETURNS integer
 LANGUAGE sql
AS $function$
WITH ids AS (
    SELECT $1 as sensors_id
)
SELECT sensor_nodes_id FROM
ids
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN sensor_systems USING (sensor_systems_id)
;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.nodes_from_project(integer)
 RETURNS integer[]
 LANGUAGE sql
AS $function$
select array_agg( DISTINCT sensor_nodes_id) from groups left join groups_sensors using (groups_id) left join sensors using (sensors_id) left join sensor_systems using (sensor_systems_id) where groups_id=$1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.nodes_from_project(text)
 RETURNS integer[]
 LANGUAGE sql
AS $function$
select array_agg(DISTINCT sensor_nodes_id) from groups left join groups_sensors using (groups_id) left join sensors using (sensors_id) left join sensor_systems using (sensor_systems_id) where name=$1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.nodes_from_sensors(integer[])
 RETURNS integer[]
 LANGUAGE sql
AS $function$
WITH ids AS (
    SELECT unnest($1) as sensors_id
)
SELECT array_agg(sensor_nodes_id) FROM
ids
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN sensor_systems USING (sensor_systems_id)
;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.notify(message text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
RAISE NOTICE '% | %', clock_timestamp(), message;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.outdated_location_days(vsn integer DEFAULT 0, lmt integer DEFAULT 100)
 RETURNS TABLE(sensor_nodes_id integer, day date, records integer, measurands integer, modified_on timestamp with time zone, queued_on timestamp with time zone, exported_on timestamp with time zone, utc_offset interval, metadata json)
 LANGUAGE sql
AS $function$
WITH pending AS (
  SELECT l.sensor_nodes_id
  , day
  , records
  , measurands
  , l.modified_on
  , queued_on
  , exported_on
  , utc_offset(sn.metadata->>'timezone') as utc_offset
  FROM public.open_data_export_logs l
  JOIN public.sensor_nodes sn ON (l.sensor_nodes_id = sn.sensor_nodes_id)
  WHERE
  -- first the requirements
  (day < current_date AND (queued_on IS NULL OR age(now(), queued_on) > '4hour'::interval) AND l.metadata->>'error' IS NULL)
  -- now the optional
  AND (
    -- its never been exported
    l.exported_on IS NULL
    -- or its been re-queued
    OR (l.queued_on > l.exported_on)
    -- or its an older version
    OR (l.metadata->>'version' IS NULL OR (l.metadata->>'version')::int < vsn)
  ) ORDER BY day
    LIMIT lmt
    FOR UPDATE
    SKIP LOCKED)
UPDATE public.open_data_export_logs
SET queued_on = now()
FROM pending
WHERE pending.day = open_data_export_logs.day
AND pending.sensor_nodes_id = open_data_export_logs.sensor_nodes_id
RETURNING pending.*, metadata;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.parameter(p integer)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
WITH t AS (
        SELECT
                measurands_id as "parameterId",
                measurand as "parameter",
                units as "unit",
                display as "displayName"
        FROM measurands WHERE measurands_id=$1
) SELECT to_jsonb(t) FROM t;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.project_in_nodes(nodes integer[], projectid integer[])
 RETURNS boolean
 LANGUAGE sql
 PARALLEL SAFE
AS $function$
SELECT EXISTS (SELECT 1
FROM
groups_sensors
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN sensor_systems USING (sensor_systems_id)
WHERE sensor_nodes_id= ANY($1) AND groups_id=ANY($2)
);
$function$
;
----------------
create or replace view "public"."provider_licenses_view" as  SELECT p.providers_id,
    json_agg(json_build_object('id', p.licenses_id, 'name', l.name, 'date_from', lower(p.active_period), 'date_to', upper(p.active_period))) AS licenses
   FROM (providers_licenses p
     JOIN licenses l ON ((l.licenses_id = p.licenses_id)))
  GROUP BY p.providers_id;

----------------
CREATE OR REPLACE FUNCTION public.pt3857(double precision, double precision)
 RETURNS geometry
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT st_transform(st_setsrid(st_makepoint($1,$2),4326),3857);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.rank_duplicates_by_source(src text, ptrn text)
 RETURNS TABLE(new_sensor_nodes_id integer, new_sensor_systems_id integer, sensor_nodes_rw integer, sensor_nodes_id integer, site_name text, sensors_id integer, source_id text, new_geom geometry, geom geometry, measurands_id text, sensors_source_id text, sensor_systems_id integer, added_on timestamp with time zone, grouper text, rw integer, new_sensors_id integer, new_grouper text)
 LANGUAGE sql
AS $function$
  WITH sensor_nodes_ranked AS (
 SELECT sensor_nodes_id
  , geom
  , site_name
  , source_id
  , regexp_replace(source_id, ptrn, '', 'g') as grouper
  , row_number() OVER (PARTITION BY regexp_replace(source_id, ptrn, '', 'g') ORDER BY source_id) as rw
  FROM sensor_nodes
  WHERE source_name = src
  ), sensor_systems_grouped AS (
    SELECT sensor_nodes_id
  , MIN(sensor_systems_id) as new_sensor_systems_id
  FROM sensor_systems
  GROUP BY 1
  ), sensor_nodes_duplicates AS (
    SELECT n1.*
  , n2.sensor_nodes_id as new_sensor_nodes_id
  , n2.geom as new_geom
  , sy.new_sensor_systems_id
  FROM sensor_nodes_ranked n1
  JOIN sensor_nodes_ranked n2 ON (n1.grouper = n2.grouper AND n2.rw = 1)
  JOIN sensor_systems_grouped sy ON (n2.sensor_nodes_id = sy.sensor_nodes_id)
  ), sensors_ranked AS (
  SELECT new_sensor_nodes_id
  , sn.new_sensor_systems_id
  , sn.rw as sensor_nodes_rw
  , sn.sensor_nodes_id
  , sn.site_name
  , s.sensors_id
  , sn.source_id
  , sn.new_geom
  , sn.geom
  , s.measurands_id
  , s.source_id as sensors_source_id
  , s.sensor_systems_id
  , s.added_on
  , sn.grouper
  , row_number() OVER (PARTITION BY new_sensor_nodes_id, measurands_id ORDER BY s.added_on) as rw
  FROM sensors s
  JOIN sensor_systems sy USING (sensor_systems_id)
  JOIN sensor_nodes_duplicates sn USING (sensor_nodes_id)
  ORDER BY new_sensor_nodes_id, measurands_id)
  SELECT s1.*
  , s2.sensors_id as new_sensors_id
  , s2.grouper as new_grouper
  FROM sensors_ranked s1
  JOIN sensors_ranked s2 ON (s1.new_sensor_nodes_id = s2.new_sensor_nodes_id AND s1.measurands_id = s2.measurands_id AND s2.rw = 1);
  $function$
;
----------------
CREATE OR REPLACE FUNCTION public.rank_duplicates_by_source_geom(src text)
 RETURNS TABLE(new_sensor_nodes_id integer, sensor_nodes_rw integer, sensor_nodes_id integer, site_name text, sensors_id integer, source_id text, new_geom geometry, geom geometry, measurands_id text, sensors_source_id text, sensor_systems_id integer, added_on timestamp with time zone, rw integer, new_sensors_id integer)
 LANGUAGE sql
AS $function$
  WITH sensor_nodes_ranked AS (
 SELECT sensor_nodes_id
  , geom
  , site_name
  , source_id
  , row_number() OVER (PARTITION BY geom ORDER BY source_id) as rw
  FROM sensor_nodes
  WHERE source_name = src
  ), sensor_nodes_duplicates AS (
    SELECT n1.*
  , n2.sensor_nodes_id as new_sensor_nodes_id
  , n2.geom as new_geom
  FROM sensor_nodes_ranked n1
  JOIN sensor_nodes_ranked n2 ON (n1.geom = n2.geom AND n2.rw = 1)
  ), sensors_ranked AS (
  SELECT new_sensor_nodes_id
  , sn.rw as sensor_nodes_rw
  , sn.sensor_nodes_id
  , sn.site_name
  , s.sensors_id
  , sn.source_id
  , sn.new_geom
  , sn.geom
  , s.measurands_id
  , s.source_id as sensors_source_id
  , s.sensor_systems_id
  , s.added_on
  , row_number() OVER (PARTITION BY new_sensor_nodes_id, measurands_id ORDER BY s.added_on) as rw
  FROM sensors s
  JOIN sensor_systems sy USING (sensor_systems_id)
  JOIN sensor_nodes_duplicates sn USING (sensor_nodes_id)
  ORDER BY new_sensor_nodes_id, measurands_id)
  SELECT s1.*
  , s2.sensors_id as new_sensors_id
  FROM sensors_ranked s1
  JOIN sensors_ranked s2 ON (s1.new_sensor_nodes_id = s2.new_sensor_nodes_id AND s1.measurands_id = s2.measurands_id AND s2.rw = 1);
  $function$
;
----------------
CREATE OR REPLACE FUNCTION public.recalculate_modified_days(lmt integer DEFAULT 10)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
	d record;
	days int := 0;
BEGIN
	FOR d IN SELECT datetime
		FROM daily_stats
		WHERE datetime < current_date
		AND modified_on IS NOT NULL
		AND modified_on > calculated_on
		LIMIT lmt
	  LOOP
	    RAISE NOTICE 'Running % of %', d.datetime, lmt;
	 		PERFORM calculate_daily_data_full(d.datetime);
	  	days := days+1;
	END LOOP;
	RETURN days;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.records_inserted(text DEFAULT 'hour'::text, timestamp with time zone DEFAULT (CURRENT_DATE - 1), text DEFAULT 'realtime'::text)
 RETURNS TABLE(datetime timestamp with time zone, period text, pattern text, inserted bigint, records bigint, percentage numeric, files bigint, min integer, max integer)
 LANGUAGE sql
AS $function$
WITH inserted AS (
 SELECT date_trunc($1, init_datetime) as datetime
 , SUM(inserted) as inserted
 , SUM(records) as records
 , COUNT(1) as files
 , MIN(inserted) as min
 , MAX(inserted) as max
 FROM fetchlogs
 WHERE init_datetime>$2
 AND key~* $3
 GROUP BY 1
 ORDER BY 1)
 SELECT datetime
 , $1 as period
 , $3 as pattern
 , inserted
 , records
 , CASE WHEN records>0 THEN ROUND((inserted::numeric/records::numeric) * 100.0) ELSE 0 END as percentage
 , files
 , min
 , max
 FROM inserted;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.regenerate_token(_users_id integer)
 RETURNS void
 LANGUAGE sql
AS $function$
UPDATE user_keys
SET token = encode(digest(uuid_generate_v4():: text, 'sha256'), 'hex')
WHERE users_id = _users_id
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.remove_sensor_data(id integer, delete_sensor boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
	BEGIN
		-- annual data
		DELETE FROM annual_data WHERE sensors_id = id;
		-- daily data
		DELETE FROM daily_data WHERE sensors_id = id;
		--- hourly data
		DELETE FROM hourly_data WHERE sensors_id = id;
		-- latest
		DELETE FROM sensors_rollup WHERE sensors_id = id;
		-- exceedances
		DELETE FROM sensor_exceedances WHERE sensors_id = id;
		-- sensors_history
		DELETE FROM sensors_history WHERE sensors_id = id;
		-- measurements
		DELETE FROM measurements WHERE sensors_id = id;
			-- sensors
		IF delete_sensor THEN
		  DELETE FROM sensors WHERE sensors_id = id;
		END IF;
	  RETURN (SELECT EXISTS(SELECT 1 FROM sensors WHERE sensors_id = id) != delete_sensor);
	END;
	$function$
;
----------------
CREATE OR REPLACE FUNCTION public.reset_export_logs()
 RETURNS TABLE(sensor_nodes_id integer, first_day date, last_day date, days integer, records integer, measurands integer)
 LANGUAGE sql
AS $function$
WITH inserts AS (
  INSERT INTO public.open_data_export_logs (sensor_nodes_id, day, records, measurands)
  SELECT m.sensor_nodes_id
  , (m.datetime - '1sec'::interval)::date as day
  , COUNT(m.value) as records
  , COUNT(DISTINCT m.measurands_id) as measurands
  FROM public.measurement_data_export m
  GROUP BY m.sensor_nodes_id, (m.datetime-'1sec'::interval)::date
  ON CONFLICT(sensor_nodes_id, day) DO UPDATE
  SET modified_on = now(), exported_on = null, queued_on = null
  RETURNING sensor_nodes_id, day, records, measurands)
  SELECT sensor_nodes_id
  , MIN(day) as first_day
  , MAX(day) as last_day
  , COUNT(day)::int as days
  , SUM(records)::int as records
  , MAX(measurands)::int as measurands
  FROM inserts
  GROUP BY sensor_nodes_id;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.reset_hourly_stats(st timestamp with time zone DEFAULT '-infinity'::timestamp with time zone, et timestamp with time zone DEFAULT 'infinity'::timestamp with time zone)
 RETURNS bigint
 LANGUAGE sql
AS $function$
WITH first_and_last AS (
SELECT MIN(datetime) as datetime_first
, MAX(datetime) as datetime_last
FROM measurements
WHERE datetime >= st
AND datetime <= et
), datetimes AS (
SELECT generate_series(
   date_trunc('hour', datetime_first)
   , date_trunc('hour', datetime_last)
   , '1hour'::interval) as datetime
FROM first_and_last
), inserts AS (
INSERT INTO hourly_stats (datetime, modified_on)
SELECT datetime
, now()
FROM datetimes
WHERE has_measurement(datetime)
ON CONFLICT (datetime) DO UPDATE
SET modified_on = GREATEST(EXCLUDED.modified_on, hourly_stats.modified_on)
RETURNING 1)
SELECT COUNT(1) FROM inserts;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.rollups_daily(_start timestamp with time zone DEFAULT now())
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
_st timestamptz := date_trunc('day', _start);
_et timestamptz := date_trunc('day', _start) + '1 day'::interval - '1 second'::interval;
BEGIN
RAISE NOTICE 'Updating daily Rollups  %  --- %', _start, clock_timestamp();
--RAISE NOTICE '% %', _st, _et;
--RAISE NOTICE 'Deleting %', clock_timestamp();
    DELETE FROM rollups
    WHERE
        rollup='day'
        AND
        st=_st
    ;
--RAISE NOTICE 'Creating temp table by sensor %', clock_timestamp();
CREATE TEMP TABLE dailyrolluptemp_by_sensor AS
SELECT
        sensors_id,
        'day' as rollup,
        _st as st,
        _et as et,
        min(datetime) as datetime_first,
        max(datetime) as datetime_last,
        count(*) as value_count,
        sum(value) as value_sum,
        last(value, datetime) as last_value,
        min(lon) as minx,
        min(lat) as miny,
        max(lon) as maxx,
        max(lat) as maxy,
        last(st_makepoint(lon,lat)::geometry, datetime) as last_point
    FROM measurements
    JOIN groups_sensors USING (sensors_id)
    JOIN sensors USING (sensors_id)
    WHERE datetime >= _st
    AND datetime <= _et
    GROUP BY 1,2,3,4
        ;

--RAISE NOTICE 'Created temp table by sensor from % to % - %: %', _st, _et, clock_timestamp(), (SELECT COUNT(1) FROM dailyrolluptemp_by_sensor);
--RAISE NOTICE 'Creating temp table by group %', clock_timestamp();

    CREATE TEMP TABLE dailyrolluptemp AS
    SELECT
        groups_id,
        measurands_id,
        last(sensors_id, datetime_last) as sensors_id,
        rollup,
        st,
        et,
        min(datetime_first) as datetime_first,
        max(datetime_last) as datetime_last,
        sum(value_count) as value_count,
        sum(value_sum) as value_sum,
        last(last_value, datetime_last) as last_value,
        min(minx) as minx,
        min(miny) as miny,
        max(maxx) as maxx,
        max(maxy) as maxy,
        last(last_point, datetime_last) as last_point
    FROM dailyrolluptemp_by_sensor
    JOIN groups_sensors USING (sensors_id)
    JOIN sensors USING (sensors_id)
    GROUP BY 1,2,4,5,6
        ;


    RAISE NOTICE 'inserting % records - %', (SELECT COUNT(1) FROM dailyrolluptemp), clock_timestamp();

    INSERT INTO rollups (
        groups_id,
        measurands_id,
        sensors_id,
        rollup,
        st,
        et,
        datetime_first,
        datetime_last,
        value_count,
        value_sum,
        last_value,
        minx,
        miny,
        maxx,
        maxy,
        last_point
    ) SELECT * FROM dailyrolluptemp;

    drop table dailyrolluptemp_by_sensor;
    drop table dailyrolluptemp;

END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.rollups_daily_full()
 RETURNS void
 LANGUAGE sql
AS $function$
WITH days AS (
  SELECT date_trunc('day', datetime - '1sec'::interval) as day
  FROM measurements
  GROUP BY date_trunc('day', datetime - '1sec'::interval)
)
SELECT rollups_daily(day)
FROM days;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.rollups_monthly(_start timestamp with time zone DEFAULT now())
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
_st timestamptz := date_trunc('month', _start);
_et timestamptz := date_trunc('month', _start) + '1 month'::interval - '1 second'::interval;
BEGIN
RAISE NOTICE 'Updating Monthly Rollups  %  --- %', _start, clock_timestamp();

RAISE NOTICE '% %', _st, _et;
    DELETE FROM rollups
    WHERE
        rollup='month'
        AND
        st=_st
    ;
    INSERT INTO rollups (
        groups_id,
        measurands_id,
        sensors_id,
        rollup,
        st,
        et,
        datetime_first,
        datetime_last,
        value_count,
        value_sum,
        last_value,
        minx,
        miny,
        maxx,
        maxy,
        last_point
    ) SELECT
        groups_id,
        measurands_id,
        last(sensors_id, datetime_last),
        'month',
        _st,
        _et,
        min(datetime_first),
        max(datetime_last),
        sum(value_count),
        sum(value_sum),
        last(last_value, datetime_last),
        min(minx),
        min(miny),
        max(maxx),
        max(maxy),
        last(last_point, datetime_last)
    FROM rollups
    WHERE
        rollup = 'day' AND
        st>= _st and st <= _et
    GROUP BY 1,2,4,5,6
    ;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.rollups_total()
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
RAISE NOTICE 'Updating total Rollups --- %', clock_timestamp();
    DELETE FROM rollups
    WHERE rollup='total'
    ;
    INSERT INTO rollups (
            groups_id,
            measurands_id,
            sensors_id,
            rollup,
            st,
            et,
            datetime_first,
            datetime_last,
            value_count,
            value_sum,
            last_value,
        minx,
        miny,
        maxx,
        maxy,
            last_point
        ) SELECT
            groups_id,
            measurands_id,
            last(sensors_id, datetime_last),
            'total',
            '1970-01-01'::timestamptz,
            '2999-01-01'::timestamptz,
            min(datetime_first),
            max(datetime_last),
            sum(value_count),
            sum(value_sum),
            last(last_value, datetime_last),
        min(minx),
        min(miny),
        max(maxx),
        max(maxy),
        last(last_point, datetime_last)
        FROM rollups
        WHERE
            rollup = 'year'
        GROUP BY 1,2,4,5,6
        ;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.rollups_yearly(_start timestamp with time zone DEFAULT now())
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
_st timestamptz := date_trunc('year', _start);
_et timestamptz := date_trunc('year', _start) + '1 year'::interval - '1 second'::interval;
BEGIN
RAISE NOTICE 'Updating yearly Rollups  % --- %', _start, clock_timestamp();

RAISE NOTICE '% %', _st, _et;
    DELETE FROM rollups
    WHERE
        rollup='year'
        AND
        st=_st
    ;
    INSERT INTO rollups (
        groups_id,
        measurands_id,
        sensors_id,
        rollup,
        st,
        et,
        datetime_first,
        datetime_last,
        value_count,
        value_sum,
        last_value,
        minx,
        miny,
        maxx,
        maxy,
        last_point
    ) SELECT
        groups_id,
        measurands_id,
        last(sensors_id, datetime_last),
        'year',
        _st,
        _et,
        min(datetime_first),
        max(datetime_last),
        sum(value_count),
        sum(value_sum),
        last(last_value, datetime_last),
        min(minx),
        min(miny),
        max(maxx),
        max(maxy),
        last(last_point, datetime_last)
    FROM rollups
        WHERE
            rollup = 'month' AND
        st>= _st and st <= _et
        GROUP BY 1,2,4,5,6
        ;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.row_count_estimate(ftn text)
 RETURNS bigint
 LANGUAGE sql
AS $function$
SELECT (CASE WHEN c.reltuples < 0 THEN NULL       -- never vacuumed
             WHEN c.relpages = 0 THEN float8 '0'  -- empty table
             ELSE c.reltuples / c.relpages END
     * (pg_catalog.pg_relation_size(c.oid)
      / pg_catalog.current_setting('block_size')::int)
       )::bigint
FROM   pg_catalog.pg_class c
WHERE  c.oid = ftn::regclass;
$function$
;
----------------
CREATE OR REPLACE PROCEDURE public.run_updates(IN job_id integer DEFAULT NULL::integer, IN config jsonb DEFAULT NULL::jsonb)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
_st timestamptz;
_et timestamptz;
t timestamptz;
st timestamptz;
BEGIN

    SELECT current_timestamp INTO st;

    SELECT (config->>'start')::timestamptz INTO STRICT _st;
    SELECT (config->>'end')::timestamptz INTO STRICT _et;

    _st:=date_trunc('day',coalesce(_st, now() - '1 days'::interval));
    _et:=date_trunc('day',coalesce(_et, now()));

    RAISE NOTICE 'updating timezones';
    UPDATE sensor_nodes
    SET timezones_id = get_timezones_id(geom)
    WHERE geom IS NOT NULL
    AND timezones_id IS NULL;

    -- sn_lastpoint pulls directly from measurements at the moment
    UPDATE sensor_nodes
    SET timezones_id = get_timezones_id(sn_lastpoint(sensor_nodes_id))
    WHERE geom IS NULL
    AND ismobile
    AND timezones_id IS NULL;

    SELECT log_performance('update-timezone', st) INTO st;

    RAISE NOTICE 'updating countries';
    update sensor_nodes set country = country(geom)
    where (country is null OR country = '99') and geom is not null;
    update sensor_nodes set country = country(sn_lastpoint(sensor_nodes_id))
    where country is null and geom is null and ismobile;
    COMMIT;
    SELECT log_performance('update-countries', st) INTO st;

    RAISE NOTICE 'Updating sources Tables';
    PERFORM update_sources();
    COMMIT;
    SELECT log_performance('update-sources', st) INTO st;

    RAISE NOTICE 'Updating Groups Tables';
    PERFORM update_groups();
    COMMIT;
    SELECT log_performance('update-groups', st) INTO st;

    FOR t IN
        (SELECT g FROM generate_series(_st, _et, '1 day'::interval) as g)
    LOOP
        --CALL refresh_continuous_aggregate('measurements_daily',_st,_et);
        PERFORM rollups_daily(t);
        COMMIT;
    END LOOP;

    SELECT log_performance('update-daily-rollups', st) INTO st;

    FOR t IN
        (SELECT g FROM generate_series(_st, _et, '1 month'::interval) as g)
    LOOP
        PERFORM rollups_monthly(t);
        COMMIT;
    END LOOP;

    SELECT log_performance('update-monthly-rollups', st) INTO st;

    FOR t IN
        (SELECT g FROM generate_series(_st, _et, '1 year'::interval) as g)
    LOOP
        PERFORM rollups_yearly(t);
        COMMIT;
    END LOOP;

    SELECT log_performance('update-yearly-rollups', st) INTO st;

    PERFORM rollups_total();
    COMMIT;
    SELECT log_performance('update-total-rollups', st) INTO st;

    RAISE NOTICE 'REFRESHING sensors_first_last';
    REFRESH MATERIALIZED VIEW sensors_first_last;
    COMMIT;
    SELECT log_performance('update-sensors-first-last', st) INTO st;

    RAISE NOTICE 'REFRESHING sensor_nodes_json';
    REFRESH MATERIALIZED VIEW sensor_nodes_json;
    COMMIT;
    SELECT log_performance('update-sensor-nodes-json', st) INTO st;

    RAISE NOTICE 'REFRESHING groups_view';
    REFRESH MATERIALIZED VIEW groups_view;
    COMMIT;
    SELECT log_performance('update-groups-view', st) INTO st;

    RAISE NOTICE 'REFRESHING sensor_stats';
    REFRESH MATERIALIZED VIEW sensor_stats;
    COMMIT;
    SELECT log_performance('update-sensor-stats', st) INTO st;

    RAISE NOTICE 'REFRESHING city_stats';
    REFRESH MATERIALIZED VIEW city_stats;
    COMMIT;
    SELECT log_performance('update-city-stats', st) INTO st;

    RAISE NOTICE 'REFRESHING country_stats';
    REFRESH MATERIALIZED VIEW country_stats;
    COMMIT;
    SELECT log_performance('update-country-stats', st) INTO st;

    RAISE NOTICE 'REFRESHING locations_base_v2';
    REFRESH MATERIALIZED VIEW locations_base_v2;
    COMMIT;
    SELECT log_performance('update-locations-base', st) INTO st;

    RAISE NOTICE 'REFRESHING locations';
    REFRESH MATERIALIZED VIEW locations;
    COMMIT;
    SELECT log_performance('update-locations', st) INTO st;

    RAISE NOTICE 'REFRESHING measurements_fastapi_base';
    REFRESH MATERIALIZED VIEW measurements_fastapi_base;
    COMMIT;
    SELECT log_performance('update-measurements-base', st) INTO st;

END;
$procedure$
;
----------------
CREATE OR REPLACE PROCEDURE public.run_updates_full()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
_start timestamptz;
BEGIN
SELECT MIN(datetime) INTO _start FROM measurements;
CALL run_updates(NULL, jsonb_build_object('start', _start));
END;
$procedure$
;
----------------
CREATE OR REPLACE FUNCTION public.sensor_node_changes()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
    INSERT INTO public.sensor_nodes_harrays (
        sensor_nodes_id,
        cities,
        source_names,
        site_names
    ) VALUES (
        NEW.sensor_nodes_id,
        ARRAY[NEW.city],
        ARRAY[NEW.source_name],
        ARRAY[NEW.site_name]
    ) ON CONFLICT (sensor_nodes_id)
    DO UPDATE
        SET
            cities=public.array_distinct(array_cat(sensor_nodes_harrays.cities, EXCLUDED.cities), true),
            source_names=public.array_distinct(array_cat(sensor_nodes_harrays.source_names, EXCLUDED.source_names), true),
            site_names=public.array_distinct(array_cat(sensor_nodes_harrays.site_names, EXCLUDED.site_names), true)
    ;
    INSERT INTO public.sensor_nodes_history
    SELECT
        OLD.sensor_nodes_id,
        OLD.ismobile,
        OLD.geom,
        OLD.site_name,
        OLD.source_name,
        OLD.city,
        OLD.geocoding_result,
        OLD.country,
        OLD.metadata,
        now(),
        OLD.source_id;
    RETURN NEW;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sensor_systems_changes()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
        INSERT INTO public.sensor_systems_history
        (sensor_systems_id, sensor_nodes_id, source_id, metadata, created)
        SELECT
            NEW.sensor_systems_id,
            NEW.sensor_nodes_id,
            NEW.source_id,
            NEW.metadata,
            now();
    RETURN NEW;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sensors_changes()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
    INSERT INTO public.sensors_history
        (
            sensors_id,
            sensor_systems_id,
            measurands_id,
            source_id,
            metadata,
            created
        )
    SELECT
        NEW.sensors_id,
        NEW.sensor_systems_id,
        NEW.measurands_id,
        NEW.source_id,
        NEW.metadata,
        now();
    RETURN NEW;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sig_digits(n anyelement, digits integer)
 RETURNS numeric
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
    SELECT CASE
        WHEN n=0 THEN 0
        ELSE round(n::numeric, digits - 1 - floor(log(abs(n)))::int)
    END
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.slugify(value text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
  -- removes accents (diacritic signs) from a given string --
  WITH "unaccented" AS (
    SELECT unaccent("value") AS "value"
  ),
  -- lowercases the string
  "lowercase" AS (
    SELECT lower("value") AS "value"
    FROM "unaccented"
  ),
  -- remove single and double quotes
  "removed_quotes" AS (
    SELECT regexp_replace("value", '[''"]+', '', 'gi') AS "value"
    FROM "lowercase"
  ),
  -- replaces anything that's not a letter, number, hyphen('-'), or underscore('_') with a hyphen('-')
  "hyphenated" AS (
    SELECT regexp_replace("value", '[^a-z0-9\\-_]+', '-', 'gi') AS "value"
    FROM "removed_quotes"
  ),
  -- trims hyphens('-') if they exist on the head or tail of the string
  "trimmed" AS (
    SELECT regexp_replace(regexp_replace("value", '\-+$', ''), '^\-', '') AS "value"
    FROM "hyphenated"
  )
  SELECT "value" FROM "trimmed";
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sn_lastpoint(_sn_id integer)
 RETURNS geometry
 LANGUAGE sql
 PARALLEL SAFE
AS $function$
SELECT st_setsrid(st_makepoint(lon,lat),4326)
FROM measurements WHERE sensors_id=(
        SELECT sensors_id sa
        FROM sensor_systems
        JOIN sensors
        USING (sensor_systems_id)
        WHERE sensor_nodes_id=_sn_id
        LIMIT 1
)
ORDER BY datetime DESC LIMIT 1
;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.source_in_nodes(nodes integer[], sources text[])
 RETURNS boolean
 LANGUAGE sql
AS $function$
SELECT EXISTS (SELECT 1
FROM
sensor_nodes_sources
LEFT JOIN sources USING (sources_id)
WHERE sensor_nodes_id= ANY($1) AND slug=ANY($2)
);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sources(s integer)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
SELECT jsonb_agg(sources_jsonb(sources))
FROM
sensor_nodes_sources
LEFT JOIN sources USING (sources_id)
WHERE sensor_nodes_id=$1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sources(s integer[])
 RETURNS jsonb
 LANGUAGE sql
AS $function$
SELECT jsonb_agg(distinct sources_jsonb(sources))
FROM
sensor_nodes_sources
LEFT JOIN sources USING (sources_id)
WHERE sensor_nodes_id= ANY($1);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sources_in_city(_city text)
 RETURNS integer
 LANGUAGE sql
 PARALLEL SAFE
AS $function$
SELECT count(distinct sources_id)::int
FROM sensor_nodes
LEFT JOIN sensor_nodes_sources USING (sensor_nodes_id)
WHERE city=_city;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sources_in_country(_country text)
 RETURNS integer
 LANGUAGE sql
 PARALLEL SAFE
AS $function$
SELECT count(distinct sources_id)::int
FROM sensor_nodes
LEFT JOIN sensor_nodes_sources USING (sensor_nodes_id)
WHERE country=_country;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.sources_jsonb(s sources)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
SELECT jsonb_strip_nulls(jsonb_build_object(
    'id', "slug",
    'name', name,
    'readme',
        case when readme is not null then
        '/v2/sources/readme/' || slug
        else null end
) || coalesce(metadata,'{}'::jsonb)) FROM (SELECT s.*) as row;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.split_ingest_id(iid text)
 RETURNS text[]
 LANGUAGE sql
AS $function$
WITH arr AS (
SELECT iid as ingest_id
, string_to_array(iid,'-') as iid)
  SELECT ARRAY[
     iid[1]
    -- deals with case where source_id (from client) has a dash in it
    , CASE WHEN array_length(iid, 1) < 3 THEN 'N/A'
           ELSE array_to_string(iid[2:(array_length(iid, 1)-1)], '-')
           END
    , iid[array_length(iid, 1)]
  ]
  FROM arr;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.split_ingest_id(iid text, pos integer)
 RETURNS text
 LANGUAGE sql
AS $function$
SELECT (split_ingest_id(iid))[pos];
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.table_row_estimator(table_name text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
   plan jsonb;
BEGIN
   EXECUTE 'EXPLAIN (FORMAT JSON) SELECT * FROM ' || table_name INTO plan;
   RETURN (plan->0->'Plan'->>'Plan Rows')::bigint;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.timezone(g geography)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
	SELECT tzid
	FROM timezones
	WHERE st_intersects(g, geog)
	ORDER BY timezones_id ASC
	LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.timezone(g geometry)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
	SELECT tzid
	FROM timezones
	WHERE st_intersects(g::geography, geog)
	ORDER BY timezones_id ASC
	LIMIT 1;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.truncate_timestamp(tstz timestamp with time zone, period text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT date_trunc(period, tstz + '11sec'::interval);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.truncate_timestamp(tstz timestamp with time zone, period text, _offset interval)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT date_trunc(period, tstz + ('-1sec'::interval + _offset));
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.truncate_timestamp(tstz timestamp with time zone, period text, tz text)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT timezone(tz, date_trunc(period, timezone(tz, tstz + '-1sec'::interval)));
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.truncate_timestamp(tstz timestamp with time zone, period text, tz text, _offset interval)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT timezone(tz, date_trunc(period, timezone(tz, tstz + ('-1sec'::interval + _offset))));
$function$
;
----------------
CREATE OR REPLACE PROCEDURE public.update_cached_tables()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
BEGIN
    RAISE NOTICE 'REFRESHING locations_view_cached';
    REFRESH MATERIALIZED VIEW locations_view_cached;
    COMMIT;
    PERFORM log_performance('update-locations-view', CURRENT_TIMESTAMP);
    ----------------------------------------
    RAISE NOTICE 'REFRESHING locations_manufacturers_cached';
    REFRESH MATERIALIZED VIEW locations_manufacturers_cached;
    COMMIT;
    PERFORM log_performance('update-locations-manufacturers', CURRENT_TIMESTAMP);
    ----------------------------------------
    RAISE NOTICE 'REFRESHING locations_latest_measurements_cached';
    REFRESH MATERIALIZED VIEW locations_latest_measurements_cached;
    COMMIT;
    PERFORM log_performance('update-locations-latest-measurements-view', CURRENT_TIMESTAMP);
    ----------------------------------------
    RAISE NOTICE 'REFRESHING countries_view_cached';
    REFRESH MATERIALIZED VIEW countries_view_cached;
    COMMIT;
    PERFORM log_performance('update-countries-view', CURRENT_TIMESTAMP);
    ----------------------------------------
    RAISE NOTICE 'REFRESHING providers_view_cached';
    REFRESH MATERIALIZED VIEW providers_view_cached;
    COMMIT;
    PERFORM log_performance('update-providers-view', CURRENT_TIMESTAMP);
    ----------------------------------------
    RAISE NOTICE 'REFRESHING parameters_view_cached';
    REFRESH MATERIALIZED VIEW parameters_view_cached;
    COMMIT;
    PERFORM log_performance('update-parameters-view', CURRENT_TIMESTAMP);
END;
$procedure$
;
----------------
CREATE OR REPLACE PROCEDURE public.update_daily_cached_tables()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
BEGIN
    RAISE NOTICE 'REFRESHING sensor_node_daily_exceedances';
    REFRESH MATERIALIZED VIEW CONCURRENTLY sensor_node_daily_exceedances;
    COMMIT;
    PERFORM log_performance('update-daily-exceedances', CURRENT_TIMESTAMP);
    ----------------------------------------
    RAISE NOTICE 'REFRESHING sensor_node_range_exceedances';
    REFRESH MATERIALIZED VIEW CONCURRENTLY sensor_node_range_exceedances;
    COMMIT;
    PERFORM log_performance('update-range-exceedances', CURRENT_TIMESTAMP);
END;
$procedure$
;
----------------
CREATE OR REPLACE FUNCTION public.update_daily_data_queue()
 RETURNS bigint
 LANGUAGE sql
AS $function$
 WITH data_min AS (
  SELECT MIN(datetime) as min_date
  , MAX(datetime) as max_date
  FROM measurements
 ), days AS (
  SELECT generate_series(min_date, max_date, '1day'::interval) as datetime
  FROM data_min
 ), daily_inserts AS (
  INSERT INTO daily_data_queue (datetime, tz_offset)
  SELECT datetime, generate_series(-12,14,1) as tz_offset
  FROM days
  ON CONFLICT DO NOTHING
  RETURNING datetime, tz_offset
  ) SELECT COUNT(*)
  FROM daily_inserts;
  $function$
;
----------------
CREATE OR REPLACE FUNCTION public.update_export_log_exported(dy date, id integer, n integer)
 RETURNS interval
 LANGUAGE sql
AS $function$
UPDATE public.open_data_export_logs
SET exported_on = now()
, records = n
, metadata = '{}'::json
WHERE day = dy AND sensor_nodes_id = id
RETURNING exported_on - queued_on;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.update_export_log_modified(dy date, id integer)
 RETURNS interval
 LANGUAGE sql
AS $function$
UPDATE public.open_data_export_logs
SET modified_on = now()
WHERE day = dy AND sensor_nodes_id = id
RETURNING exported_on - queued_on;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.update_groups()
 RETURNS void
 LANGUAGE sql
AS $function$
--OVERALL TOTAL
    INSERT INTO groups (type, name, subtitle)
    SELECT
        'total',
        'total',
        'All Sensors'
    ON CONFLICT (type, name)
    DO NOTHING;

    -- Each sensor_node
    INSERT INTO groups (type, name, subtitle)
    SELECT 'node'
    , sensor_nodes_id::text
    , site_name
    FROM sensor_nodes
    ON CONFLICT (type, name) DO
    UPDATE
    SET
        subtitle=EXCLUDED.subtitle
    ;

    -- Each Country
    INSERT INTO groups (type, name, subtitle)
    SELECT
        'country',
        iso,
        name
    FROM countries
    WHERE iso is not null and name is not null
    ON CONFLICT (type, name)
    DO NOTHING;

    -- Each Source from AQDC sources
    /*INSERT INTO groups (type, name, subtitle)
    SELECT
        'source',
        sn.source_name,
        sn.metadata->>'sensor_node_source_fullname',
        st_union(geom)::geography
    FROM sensors
    LEFT JOIN sensor_systems USING (sensor_systems_id)
    LEFT JOIN sensor_nodes sn USING (sensor_nodes_id)
    WHERE sn.metadata @> '{"origin":"AQDC"}'
    GROUP BY 1,2,3
    ON CONFLICT (type, name)
    DO NOTHING;*/
    INSERT INTO groups (type, name, subtitle, metadata)
    SELECT 'source', slug, name, sources.metadata
    FROM sources
    JOIN sensor_nodes_sources USING (sources_id)
    JOIN sensor_nodes USING (sensor_nodes_id)
    WHERE origin='AQDC'
    ON CONFLICT DO NOTHING
    ;

    -- each aqdc organization
    INSERT INTO groups(type, name, subtitle)
    SELECT DISTINCT 'organization'
    , slugify(sources.metadata->>'organization')
    , sources.metadata->>'organization'
    FROM sources
    JOIN sensor_nodes_sources USING (sources_id)
    JOIN sensor_nodes USING (sensor_nodes_id)
    WHERE origin='AQDC' and sources.metadata ? 'organization'
    ON CONFLICT DO NOTHING
    ;

    --add country sensors
    INSERT INTO groups_sensors (groups_id, sensors_id)
    SELECT
        groups_id,
        s.sensors_id
    FROM sensor_nodes
    JOIN sensor_systems USING (sensor_nodes_id)
    JOIN sensors s USING (sensor_systems_id)
    JOIN groups g ON (country=g.name AND g.type = 'country')
    ON CONFLICT DO NOTHING
    ;

    -- add sensor node sensors
    INSERT INTO groups_sensors (groups_id, sensors_id)
    SELECT
        groups_id,
        s.sensors_id
    FROM sensor_nodes
    JOIN sensor_systems USING (sensor_nodes_id)
    JOIN sensors s USING (sensor_systems_id)
    JOIN groups ON (sensor_nodes_id::text=name)
    ON CONFLICT DO NOTHING;

    -- add total sensors
    INSERT INTO groups_sensors (groups_id, sensors_id)
    SELECT
        groups_id,
        s.sensors_id
    FROM
    sensors s, groups
    WHERE groups.type='total' and groups.name='total'
    ON CONFLICT DO NOTHING
    ;

    -- add sensors for source
    INSERT INTO groups_sensors (groups_id, sensors_id)
    SELECT
        groups_id,
        s.sensors_id
    FROM groups
    JOIN sources ON (groups.name=sources.slug)
    JOIN sensor_nodes_sources USING (sources_id)
    JOIN sensor_systems USING (sensor_nodes_id)
    JOIN sensors s USING (sensor_systems_id)
    ON CONFLICT DO NOTHING
    ;
    -- add sensors for organizations
    INSERT INTO groups_sensors (groups_id, sensors_id)
    SELECT
        groups_id,
        s.sensors_id
    FROM groups
    JOIN sources ON (groups.name=slugify(sources.metadata->>'organization'))
    JOIN sensor_nodes_sources USING (sources_id)
    JOIN sensor_systems USING (sensor_nodes_id)
    JOIN sensors s USING (sensor_systems_id)
    WHERE sources.metadata ? 'organization'
    ON CONFLICT DO NOTHING
    ;
$function$
;
----------------
CREATE OR REPLACE PROCEDURE public.update_hourly_data(IN lag interval, IN lmt integer DEFAULT 1000)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
dt timestamptz;
BEGIN
FOR dt IN (
    SELECT datetime
    FROM hourly_stats
    WHERE datetime < now() - lag
    AND (calculated_on IS NULL
    OR calculated_on < COALESCE(modified_on, added_on))
    ORDER BY datetime ASC
    LIMIT lmt)
LOOP
  RAISE NOTICE 'updating hour: %', dt;
  PERFORM update_hourly_data(dt);
  COMMIT;
END LOOP;
END;
$procedure$
;
----------------
CREATE OR REPLACE PROCEDURE public.update_hourly_data(IN lmt integer DEFAULT 1000)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
dt timestamptz;
BEGIN
FOR dt IN (
    SELECT datetime
    FROM hourly_stats
    WHERE datetime < now()
    AND (calculated_on IS NULL
    OR calculated_on < COALESCE(modified_on, added_on))
    ORDER BY datetime ASC
    LIMIT lmt)
LOOP
  RAISE NOTICE 'updating hour: %', dt;
  PERFORM update_hourly_data(dt);
  COMMIT;
END LOOP;
END;
$procedure$
;
----------------
CREATE OR REPLACE FUNCTION public.update_hourly_data(hr timestamp with time zone DEFAULT (now() - '01:00:00'::interval))
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
nw timestamptz := clock_timestamp();
mc bigint;
BEGIN
WITH inserted AS (
  SELECT COALESCE(measurements_count, 0) as measurements_count
  , COALESCE(sensors_count, 0) as sensors_count
  FROM calculate_hourly_data(hr))
  INSERT INTO hourly_stats (
    datetime
  , calculated_on
  , measurements_count
  , sensors_count
  , calculated_count
  , calculated_seconds)
  SELECT date_trunc('hour', hr)
  , now()
  , measurements_count
  , sensors_count
  , 1
  , EXTRACT(EPOCH FROM clock_timestamp() - nw)
  FROM inserted
  ON CONFLICT (datetime) DO UPDATE
  SET calculated_on = EXCLUDED.calculated_on
  , calculated_count = hourly_stats.calculated_count + 1
  , measurements_count = EXCLUDED.measurements_count
  , sensors_count = EXCLUDED.sensors_count
  , calculated_seconds = EXCLUDED.calculated_seconds
  RETURNING measurements_count INTO mc;
  PERFORM hourly_data_updated_event(hr);
  RETURN mc;
END;
$function$
;
----------------
CREATE OR REPLACE PROCEDURE public.update_hourly_data_latest(IN lag interval, IN lmt integer DEFAULT 1000)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
dt timestamptz;
BEGIN
FOR dt IN (
    SELECT datetime
    FROM hourly_stats
    WHERE datetime < now() - lag
    AND (calculated_on IS NULL
    OR calculated_on < COALESCE(modified_on, added_on))
    ORDER BY datetime DESC
    LIMIT lmt)
LOOP
  RAISE NOTICE 'updating hour: %', dt;
  PERFORM update_hourly_data(dt);
  COMMIT;
END LOOP;
END;
$procedure$
;
----------------
CREATE OR REPLACE PROCEDURE public.update_hourly_data_latest(IN lmt integer DEFAULT 1000)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
dt timestamptz;
BEGIN
FOR dt IN (
    SELECT datetime
    FROM hourly_stats
    WHERE datetime < now()
    AND (calculated_on IS NULL
    OR calculated_on < COALESCE(modified_on, added_on))
    ORDER BY datetime DESC
    LIMIT lmt)
LOOP
  RAISE NOTICE 'updating hour: %', dt;
  PERFORM update_hourly_data(dt);
  COMMIT;
END LOOP;
END;
$procedure$
;
----------------
CREATE OR REPLACE FUNCTION public.update_instruments(sn text, eid integer, iid integer, force boolean DEFAULT false)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
	n bigint := 0;
BEGIN
		UPDATE sensor_nodes
		SET owner_entities_id = eid
		WHERE (lower(origin) = lower(sn) OR lower(source_name) = lower(sn))
		AND (owner_entities_id = 1 OR force);
	  ------------
	  WITH updates AS (
		UPDATE sensor_systems
		SET instruments_id = iid
		WHERE (instruments_id = 1 OR force)
		AND sensor_nodes_id IN (
			SELECT sensor_nodes_id
			FROM sensor_nodes
			WHERE owner_entities_id = eid
	    AND (lower(origin) = lower(sn) OR lower(source_name) = lower(sn))
			)
	  RETURNING 1)
	  SELECT COUNT(1) INTO n FROM updates;
	 RETURN n;
END;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.update_providers_stats(dt date DEFAULT (CURRENT_DATE - 1))
 RETURNS bigint
 LANGUAGE sql
AS $function$
	WITH inserts AS (
	INSERT INTO providers_stats (
	  providers_id
	, day
	, sensor_nodes_count
	, sensors_count
	, measurements_count
	, offset_min
	, offset_avg
	, offset_max
	, datetime_min
	, datetime_max
	, added_on_min
	, added_on_max
	)
	SELECT n.providers_id
	, m.added_on::date as day
	, COUNT(DISTINCT n.sensor_nodes_id) as sensor_nodes_count
	, COUNT(DISTINCT m.sensors_id) as sensors_count
	, COUNT(1) as measurements_count
	, MIN(m.added_on - m.datetime) as offset_min
	, AVG(m.added_on - m.datetime) as offset_avg
	, MAX(m.added_on - m.datetime) as offset_max
	, MIN(datetime) as datetime_min
	, MAX(datetime) as datetime_max
	, MIN(m.added_on) as added_on_min
	, MAX(m.added_on) as added_on_max
	FROM measurements m
	JOIN sensors s USING (sensors_id)
	JOIN sensor_systems y USING (sensor_systems_id)
	JOIN sensor_nodes n USING (sensor_nodes_id)
	WHERE m.added_on > dt
	AND m.added_on < dt + 1
	GROUP BY 1,2
	ON CONFLICT (providers_id, day) DO UPDATE
	SET sensor_nodes_count = EXCLUDED.sensor_nodes_count
	, sensors_count = EXCLUDED.sensors_count
	, measurements_count = EXCLUDED.measurements_count
	, offset_min = EXCLUDED.offset_min
	, offset_avg = EXCLUDED.offset_avg
	, offset_max = EXCLUDED.offset_max
	, datetime_min = EXCLUDED.datetime_min
	, datetime_max = EXCLUDED.datetime_max
	, added_on_min = EXCLUDED.added_on_min
	, added_on_max = EXCLUDED.added_on_max
  RETURNING 1)
	SELECT COUNT(1) FROM inserts;
	$function$
;
----------------
CREATE OR REPLACE FUNCTION public.update_sources()
 RETURNS void
 LANGUAGE sql
AS $function$
    UPDATE sensor_nodes
    SET
    origin=upper(coalesce(metadata->>'origin', source_name))
    WHERE origin IS NULL;

    UPDATE sensor_nodes sn SET
        metadata = jsonb_strip_nulls(coalesce(sn.metadata,'{}'::jsonb) || o.metadata) - '{source_type,origin}'::text[]
    FROM
        origins o WHERE sn.origin is not null and NOT sn.metadata ? 'entity' AND sn.origin=o.origin;

    --AQDC
    INSERT INTO sources (slug, name)
    SELECT DISTINCT
        source_name,
        metadata->>'sensor_node_source_fullname'
    FROM
        sensor_nodes
    WHERE
        origin='AQDC'
    ON CONFLICT DO NOTHING
    ;

    UPDATE sources s set readme = r.readme FROM
    readmes r WHERE s.readme is null and s.slug=r.slug;

    with t as (
        select sensor_nodes_id, sources_id
        from sensor_nodes, sources
        where
        sensor_nodes.source_name = sources.slug
        and sensor_nodes.origin='AQDC'
    )
    insert into sensor_nodes_sources
    select * from t
    ON CONFLICT DO NOTHING
    ;


    -- OpenAQ
    WITH t AS (
        select distinct jsonb_array_elements(metadata->'attribution') as j
        from sensor_nodes
        where
        origin='OPENAQ'
        and
        metadata ? 'attribution'
    )
    INSERT INTO sources (name, metadata)
    SELECT
        j->>'name',
        jsonb_merge_agg(j - '{name}'::text[])
    FROM t
    GROUP BY 1
    ON CONFLICT DO NOTHING
    ;

    with t as (
        select sensor_nodes_id, sources_id
        from sensor_nodes, sources
        WHERE
        sensor_nodes.origin='OPENAQ'
        AND
        sensor_nodes.metadata ? 'attribution'
        AND
        sensor_nodes.metadata @> jsonb_build_object('attribution',jsonb_build_array(jsonb_build_object('name', sources.name)))
    )
    insert into sensor_nodes_sources
    select * from t
    ON CONFLICT DO NOTHING
    ;

    -- Other
    INSERT INTO sources(slug, name)
    SELECT DISTINCT
        slugify(source_name),
        source_name
    FROM
        sensor_nodes
    WHERE
        origin not in ('AQDC')
        AND
        not sensor_nodes.metadata ? 'attribution'
    ON CONFLICT DO NOTHING
    ;

    with t as (
        select sensor_nodes_id, sources_id
        from sensor_nodes, sources
        where
        upper(sensor_nodes.source_name) = upper(sources.name)
        and sensor_nodes.origin not in ('AQDC')
        AND
        not sensor_nodes.metadata ? 'attribution'
    )
    insert into sensor_nodes_sources
    select * from t
    ON CONFLICT DO NOTHING
    ;

$function$
;
----------------
CREATE OR REPLACE FUNCTION public.upsert_daily_stats(dt date)
 RETURNS json
 LANGUAGE sql
AS $function$
	WITH daily_data_summary AS (
	SELECT datetime
	, SUM(value_count) as measurements_count
	, SUM(value_raw_count) as measurements_raw_count
	, COUNT(1) as sensors_count
	, COUNT(DISTINCT sy.sensor_nodes_id) as sensor_nodes_count
	, MAX(calculated_on) as calculated_on
	, MAX(updated_on) as updated_on
	, SUM(calculated_count) as calculated_count
	FROM daily_data d
	JOIN sensors s ON (d.sensors_id = s.sensors_id)
	JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
	WHERE datetime = dt
	GROUP BY 1)
	INSERT INTO daily_stats(
		datetime
	, sensor_nodes_count
	, sensors_count
	, measurements_count
	, measurements_raw_count
	, calculated_on
	, updated_on
	, calculated_count
	, added_on)
	SELECT datetime
	, COALESCE(sensor_nodes_count, 0)
	, COALESCE(sensors_count, 0)
	, COALESCE(measurements_count, 0)
	, COALESCE(measurements_raw_count, 0)
	, calculated_on
	, updated_on
	, COALESCE(calculated_count, 1)
	, now() as added_on
	FROM (SELECT dt as datetime) d
	LEFT JOIN daily_data_summary USING (datetime)
	ON CONFLICT (datetime) DO UPDATE
	SET sensor_nodes_count = EXCLUDED.sensor_nodes_count
	, sensors_count = EXCLUDED.sensors_count
	, measurements_count = EXCLUDED.measurements_count
	, measurements_raw_count = EXCLUDED.measurements_raw_count
	, calculated_on = EXCLUDED.calculated_on
	, calculated_count = EXCLUDED.calculated_count
	, updated_on = EXCLUDED.updated_on
	RETURNING json_build_object(datetime, measurements_raw_count);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.utc_offset(dt timestamp with time zone, sn integer)
 RETURNS interval
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT utc_offset(dt, t.tzid)
FROM sensor_nodes n
	JOIN timezones t ON (t.timezones_id = n.timezones_id)
	WHERE sensor_nodes_id = sn;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.utc_offset(dt timestamp with time zone, tz text)
 RETURNS interval
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT timezone(tz, dt) - timezone('UTC', dt);
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.utc_offset(sn integer)
 RETURNS interval
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
  SELECT utc_offset(t.tzid)
  FROM sensor_nodes n
	JOIN timezones t ON (t.timezones_id = n.timezones_id)
	WHERE sensor_nodes_id = sn;
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.utc_offset(tz text)
 RETURNS interval
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
SELECT timezone(tz, now()) - timezone('UTC', now());
$function$
;
----------------
CREATE OR REPLACE FUNCTION public.verify_email(_email_address text, _verification_code text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
  _users_id int;
  _token text;
BEGIN

  UPDATE users
  SET verified_on = NOW()
  WHERE email_address = _email_address
  AND verification_code = _verification_code
  RETURNING users_id INTO _users_id;

  IF _users_id IS NULL THEN
     RAISE EXCEPTION 'Verification code could not be matched for %', _email_address;
  END IF;
  SELECT get_user_token(_users_id) INTO _token;
  RETURN _token;
END
$function$
;
