CREATE OR REPLACE FUNCTION jsonb_array(jsonb)
RETURNS jsonb[] AS $$
SELECT array_agg(j) FROM jsonb_array_elements($1) j;
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_array(anyarray)
RETURNS jsonb[] AS $$
SELECT array_agg(to_jsonb(a)) FROM unnest($1) a;
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_array_query(text, anyarray) RETURNS jsonb[] AS $$
WITH j AS (
SELECT jsonb_agg(jsonb_build_object($1, val)) j
FROM unnest($2) AS val
)
SELECT array_agg(
        j
) FROM j;
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION array_distinct(
      anyarray, -- input array
      boolean DEFAULT false -- flag to ignore nulls
) RETURNS anyarray AS $$
      SELECT array_agg(DISTINCT x)
      FROM unnest($1) t(x)
      WHERE CASE WHEN $2 THEN x IS NOT NULL ELSE true END;
$$ LANGUAGE SQL IMMUTABLE;

-- Aggregate function to return the first not null value
CREATE OR REPLACE FUNCTION public.first_notnull_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
        SELECT coalesce($1, $2);
$$;


-- And then wrap an aggregate around it
DROP AGGREGATE IF EXISTS public.first_notnull (anyelement);
CREATE AGGREGATE public.first_notnull (
        sfunc    = public.first_notnull_agg,
        basetype = anyelement,
        stype    = anyelement
);

-- Aggregate to merge jsonb fields with last one wins
CREATE OR REPLACE FUNCTION public.jsonb_merge( jsonb, jsonb )
RETURNS jsonb LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
        SELECT
            CASE
                WHEN $1 IS NULL THEN $2
                WHEN $2 IS NULL THEN $1
                ELSE $1 || $2
            END;
$$;

DROP AGGREGATE IF EXISTS public.jsonb_merge_agg (jsonb);
-- And then wrap an aggregate around it
CREATE AGGREGATE public.jsonb_merge_agg(
        sfunc    = public.jsonb_merge,
        basetype = jsonb,
        stype    = jsonb
);

CREATE OR REPLACE FUNCTION array_merge( anyarray, anyarray )
RETURNS anyarray LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT
            CASE
                WHEN $1 IS NULL THEN $2
                WHEN $2 IS NULL THEN $1
                ELSE $1 || $2
            END;
$$;

DROP AGGREGATE IF EXISTS public.array_merge_agg (anyarray);
CREATE AGGREGATE array_merge_agg(
    sfunc = array_merge,
    basetype = anyarray,
    stype = anyarray
);


CREATE OR REPLACE FUNCTION get_providers_id(p text)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT providers_id
FROM providers
WHERE source_name = p
LIMIT 1;
$$;


CREATE OR REPLACE FUNCTION timezone(g geography)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT tzid from timezones WHERE st_intersects(g, geog) LIMIT 1;
$$;
CREATE OR REPLACE FUNCTION timezone(g geometry)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT tzid from timezones WHERE st_intersects(g::geography, geog) LIMIT 1;
$$;
CREATE OR REPLACE FUNCTION get_timezones_id(g geometry)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT gid from timezones WHERE st_intersects(g::geography, geog) LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION get_timezones_id(tz text)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT gid
FROM timezones
WHERE lower(tzid) = lower(tz)
LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION get_countries_id(g geography)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT countries_id from countries WHERE st_intersects(g::geometry, geom) LIMIT 1;
$$;
CREATE OR REPLACE FUNCTION get_countries_id(g geometry)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT countries_id from countries WHERE st_intersects(g, geom) LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION country(g geography)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT iso from countries WHERE st_intersects(g::geometry, geom) LIMIT 1;
$$;
CREATE OR REPLACE FUNCTION country(g geometry)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT iso from countries WHERE st_intersects(g, geom) LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION countries(nodes int[])
RETURNS text[] AS
$$
WITH t AS (
        SELECT DISTINCT country
        FROM sensor_nodes WHERE
        sensor_nodes_id = ANY(nodes)
) SELECT array_agg(country) FROM t;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION format_timestamp(tstz timestamptz, tz text DEFAULT 'UTC') returns text AS $$
SELECT replace(format(
                '%sT%s+%s',
                to_char(timezone(COALESCE(tz, 'UTC'), tstz), 'YYYY-MM-DD'),
                --timezone(tz, tstz)::time,
                to_char(timezone(COALESCE(tz, 'UTC'), tstz)::time, 'HH24:MI:SS'),
                to_char(timezone(COALESCE(tz, 'UTC'), tstz) - timezone('UTC',tstz), 'HH24:MI')
            ),'+-','-')
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION truncate_timestamp(tstz timestamptz, period text)
RETURNS timestamptz AS $$
SELECT date_trunc(period, tstz + '11sec'::interval);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

--DROP FUNCTION IF EXISTS truncate_timestamp(timestamptz, text, text);
CREATE OR REPLACE FUNCTION truncate_timestamp(tstz timestamptz, period text, tz text)
RETURNS timestamptz AS $$
SELECT timezone(tz, date_trunc(period, timezone(tz, tstz + '-1sec'::interval)));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

--DROP FUNCTION IF EXISTS truncate_timestamp(timestamptz, text, text, interval);
CREATE OR REPLACE FUNCTION truncate_timestamp(tstz timestamptz, period text, tz text, _offset interval)
RETURNS timestamptz AS $$
SELECT timezone(tz, date_trunc(period, timezone(tz, tstz + ('-1sec'::interval + _offset))));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION truncate_timestamp(tstz timestamptz, period text, _offset interval)
RETURNS timestamptz AS $$
SELECT date_trunc(period, tstz + ('-1sec'::interval + _offset));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION get_datetime_object(tstz timestamptz, tz text DEFAULT 'UTC')
RETURNS json AS $$
SELECT json_build_object(
       'utc', format_timestamp(tstz, 'UTC')
     , 'local', format_timestamp(tstz, tz)
     );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- assume that its in the right timezone but not timestamptz
-- this would happen if we used timezone(tz, timestamptz) to convert something
CREATE OR REPLACE FUNCTION get_datetime_object(tstz timestamp, tz text DEFAULT 'UTC')
RETURNS json AS $$
SELECT json_build_object(
       'utc', format_timestamp(tstz AT TIME ZONE tz, 'UTC')
     , 'local', format_timestamp(tstz AT TIME ZONE tz, tz)
     , 'timezone', tz
     );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE EXTENSION IF NOT EXISTS "unaccent";

CREATE OR REPLACE FUNCTION slugify("value" TEXT)
RETURNS TEXT AS $$
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
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION sources_jsonb(s sources)
RETURNS jsonb AS $$
SELECT jsonb_strip_nulls(jsonb_build_object(
    'id', "slug",
    'name', name,
    'readme',
        case when readme is not null then
        '/v2/sources/readme/' || slug
        else null end
) || coalesce(metadata,'{}'::jsonb)) FROM (SELECT s.*) as row;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sources(s int)
RETURNS jsonb AS $$
SELECT jsonb_agg(sources_jsonb(sources))
FROM
sensor_nodes_sources
LEFT JOIN sources USING (sources_id)
WHERE sensor_nodes_id=$1;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sources(s int[])
RETURNS jsonb AS $$
SELECT jsonb_agg(distinct sources_jsonb(sources))
FROM
sensor_nodes_sources
LEFT JOIN sources USING (sources_id)
WHERE sensor_nodes_id= ANY($1);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION mfr(sensor_systems_metadata jsonb) RETURNS JSONB AS $$
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
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION manufacturers(_sensor_nodes_id int)
RETURNS jsonb AS $$
WITH t AS (
        SELECT
        metadata->>'manufacturer_name' as "manufacturerName",
        metadata->>'model_name' as "modelName"
        FROM
        sensor_systems
        WHERE
        sensor_nodes_id=$1
) SELECT jsonb_strip_nulls(jsonb_agg(to_jsonb(t)))
FROM t;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION node_from_sensor(int) returns int AS $$
WITH ids AS (
    SELECT $1 as sensors_id
)
SELECT sensor_nodes_id FROM
ids
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN sensor_systems USING (sensor_systems_id)
;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION node_from_group(int) returns int AS $$
WITH ids AS (
    SELECT $1 as groups_id
)
SELECT sensor_nodes_id FROM
ids
LEFT JOIN groups_sensors USING (groups_id)
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN sensor_systems USING (sensor_systems_id)
;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION nodes_from_sensors(int[]) returns int[] AS $$
WITH ids AS (
    SELECT unnest($1) as sensors_id
)
SELECT array_agg(sensor_nodes_id) FROM
ids
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN sensor_systems USING (sensor_systems_id)
;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION nodes_from_project(int) returns int[] AS $$
select array_agg( DISTINCT sensor_nodes_id) from groups left join groups_sensors using (groups_id) left join sensors using (sensors_id) left join sensor_systems using (sensor_systems_id) where groups_id=$1;
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION nodes_from_project(text) returns int[] AS $$
select array_agg(DISTINCT sensor_nodes_id) from groups left join groups_sensors using (groups_id) left join sensors using (sensors_id) left join sensor_systems using (sensor_systems_id) where name=$1;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION bounds(float, float, float, float) RETURNS geometry AS $$
SELECT st_setsrid(st_makebox2d(st_makepoint($1,$2),st_makepoint($3,$4)),4326);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bbox(geom geometry) RETURNS double precision[] AS $$
SELECT ARRAY[st_x(geom),st_y(geom),st_x(geom),st_y(geom)];
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION pt3857(float, float) RETURNS geometry AS $$
SELECT st_transform(st_setsrid(st_makepoint($1,$2),4326),3857);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION parameter(p int) RETURNS jsonb AS $$
WITH t AS (
        SELECT
                measurands_id as "parameterId",
                measurand as "parameter",
                units as "unit",
                display as "displayName"
        FROM measurands WHERE measurands_id=$1
) SELECT to_jsonb(t) FROM t;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION source_in_nodes(nodes int[], sources text[]) RETURNS bool AS $$
SELECT EXISTS (SELECT 1
FROM
sensor_nodes_sources
LEFT JOIN sources USING (sources_id)
WHERE sensor_nodes_id= ANY($1) AND slug=ANY($2)
);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sources_in_country(_country text) RETURNS int AS $$
SELECT count(distinct sources_id)::int
FROM sensor_nodes
LEFT JOIN sensor_nodes_sources USING (sensor_nodes_id)
WHERE country=_country;
$$ LANGUAGE SQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sources_in_city(_city text) RETURNS int AS $$
SELECT count(distinct sources_id)::int
FROM sensor_nodes
LEFT JOIN sensor_nodes_sources USING (sensor_nodes_id)
WHERE city=_city;
$$ LANGUAGE SQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION notify(message text) returns void AS $$
DECLARE
BEGIN
RAISE NOTICE '% | %', clock_timestamp(), message;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION project_in_nodes(nodes int[], projectid int[]) RETURNS bool AS $$
SELECT EXISTS (SELECT 1
FROM
groups_sensors
LEFT JOIN sensors USING (sensors_id)
LEFT JOIN sensor_systems USING (sensor_systems_id)
WHERE sensor_nodes_id= ANY($1) AND groups_id=ANY($2)
);
$$ LANGUAGE SQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sn_lastpoint(_sn_id int) returns geometry AS $$
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
$$ LANGUAGE SQL PARALLEL SAFE;


-- From
-- https://stackoverflow.com/questions/48900936/postgresql-rounding-to-significant-figures
CREATE OR REPLACE FUNCTION sig_digits(n anyelement, digits int)
RETURNS numeric
AS $$
    SELECT CASE
        WHEN n=0 THEN 0
        ELSE round(n::numeric, digits - 1 - floor(log(abs(n)))::int)
    END
$$ LANGUAGE sql IMMUTABLE STRICT;

-- need to add the timestamps
CREATE OR REPLACE FUNCTION calculate_coverage(
  obs int
, averaging numeric DEFAULT 3600
, logging numeric DEFAULT 3600
, dur numeric DEFAULT 3600
) RETURNS jsonb AS $$
SELECT jsonb_build_object(
       'observed_count', obs
       , 'observed_interval', make_interval(secs => averaging * obs)
       , 'expected_count', ROUND(dur/logging)
       , 'expected_interval', make_interval(secs => (dur/logging) * averaging)
       , 'percent_complete', ROUND((obs/(dur/logging))*100.0)
       , 'percent_coverage', ROUND((obs/(dur/averaging))*100.0)
       );
$$ LANGUAGE SQL PARALLEL SAFE;

-- A function that will calculate the expected number of hours
-- for a given period, grouping and factor
-- this is used when calculating the coverage for the trends
-- so for two years of data we should expect
-- 2 years * 31 days * 24 hours for january (MM=01)
CREATE OR REPLACE FUNCTION expected_hours(
  sd timestamptz -- start date + hour
  , ed timestamptz -- end date + hour
  , tp text -- type interval (e.g. month, day, hour)
  , gp text -- group/factor value to use in filter
) RETURNS int AS $$
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
$$ LANGUAGE plpgsql;


-- generates 256 bit UUID v4 based token for api key and validation tokens
CREATE OR REPLACE FUNCTION generate_token()
RETURNS text AS $$
SELECT encode(digest(uuid_generate_v4():: text, 'sha256'), 'hex');
$$ LANGUAGE SQL;


-- a function to create a new user account adding a record into the 
-- users table, entities table and user_entities table
CREATE OR REPLACE FUNCTION create_user(
  full_name text
  , email_address text 
  , password_hash text
  , ip_address text
  , entity_type text
) RETURNS text AS $$
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
$$ LANGUAGE plpgsql;

-- a function to verify a user based on users_id and generate
-- an API key in the user_keys table
CREATE OR REPLACE FUNCTION get_user_token(
  _users_id integer
) RETURNS text AS $$
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
    user_keys (users_id, token, added_on)
  VALUES
    (_users_id, generate_token(), NOW())
  RETURNING token AS _token;
  RETURN _token; 
END
$$ LANGUAGE plpgsql;



SELECT expected_hours('2021-01-01 00:00:00', '2023-01-01 00:00:00', 'month', '01'); -- 1488
SELECT expected_hours('2022-01-01 00:00:00', '2023-01-01 00:00:00', 'month', '01'); -- 744
SELECT expected_hours('2022-01-01 00:00:00', '2023-01-01 00:00:00', 'hour', '01'); -- 365
SELECT expected_hours('2022-01-01 00:00:00', '2023-01-01 00:00:00', 'day', '1'); -- 1248

SELECT expected_hours('2021-01-01 00:00:00', '2023-01-01 00:00:00', 'month', '01') * 3600;



