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
drop function if exists "public"."measurements_per_hour"(dur interval);
----------------
drop function if exists "public"."measurements_per_hour"(param text, dur interval);
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
drop table "public"."rollups";
----------------
drop table "public"."sensors_latest";
----------------
drop table "public"."sensors_rollup_patch";
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
create or replace view "public"."provider_licenses_view" as  SELECT p.providers_id,
    json_agg(json_build_object('id', p.licenses_id, 'name', l.name, 'date_from', lower(p.active_period), 'date_to', upper(p.active_period))) AS licenses
   FROM (providers_licenses p
     JOIN licenses l ON ((l.licenses_id = p.licenses_id)))
  GROUP BY p.providers_id;

