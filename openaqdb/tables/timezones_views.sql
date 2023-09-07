CREATE OR REPLACE FUNCTION timezone(g geography)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
	SELECT tzid
	FROM timezones
	WHERE st_intersects(g, geog)
	ORDER BY gid ASC
	LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION timezone(g geometry)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
	SELECT tzid
	FROM timezones
	WHERE st_intersects(g::geography, geog)
	ORDER BY gid ASC
	LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION get_timezones_id(g geometry)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
	SELECT gid
	FROM timezones
	WHERE st_intersects(g::geography, geog)
	ORDER BY gid ASC
	LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION get_timezones_id(tz text)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
	SELECT gid
	FROM timezones
	WHERE lower(tzid) = lower(tz)
	LIMIT 1;
$$;



WITH nodes AS (
SELECT geom
	, sensor_nodes_id
	, get_timezones_id(geom) as timezones_id
FROM sensor_nodes
WHERE added_on > current_date
LIMIT 2)
	SELECT n.sensor_nodes_id
	, n.geom
	, gid
	, tzid
	, timezones_id
	, st_area(geog::geometry)
	, st_xmin(geog::geometry)
	, st_xmax(geog::geometry)
	, st_ymin(geog::geometry)
	, st_ymax(geog::geometry)
	FROM nodes n, timezones
	WHERE st_intersects(geom::geography, geog);


	SELECT gid
	, tzid
	, substring(st_astext(geog) from 0 for 100)
		, substring(st_astext(geog::geometry) from 0 for 100)
	FROM timezones
	WHERE st_intersects(st_setsrid(st_point(-122,45), 4326), st_geogfromtext(st_astext(geog)))
	LIMIT 5;
