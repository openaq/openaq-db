-- These tables take longer to create and should not change often.
-- The analyses views are used for analyses data that is not ingested as part
-- of the normal ingestion process and should be rerun if any additional analysis
-- data is added.
-- The mobile_generalized view creates a materialized view that snaps
-- mobile data to a 30 meter grid and strips time to be used for maps showing
-- location where mobile data has been. This takes a long time to create, but should be
-- recalculated periodically.

--BEGIN;

TRUNCATE analyses_summary;
INSERT INTO analyses_summary
SELECT sensors_id, min(datetime) as first_datetime, max(datetime) as last_datetime, last(value,datetime)as last_value, count(*) as value_count, sum(value) as value_sum, min(lon) as minx, min(lat) as miny, max(lon) as maxx, max(lat) as maxy, st_makepoint(last(lon, datetime), last(lat, datetime))::geography as last_point from analyses group by sensors_id;

--COMMIT;

create unique index on analyses_summary (sensors_id);

DROP MATERIALIZED VIEW IF EXISTS mobile_generalized CASCADE;
CREATE MATERIALIZED VIEW mobile_generalized AS
(SELECT
    sensor_nodes_id,
    st_snaptogrid(pt3857(lon,lat),30) as geom,
    count(*),
    concat(sensor_nodes_id::text,st_astext(st_snaptogrid(pt3857(lon,lat),30))) AS uniq
FROM
    measurements
    LEFT JOIN sensors USING (sensors_id)
    LEFT JOIN sensor_systems USING (sensor_systems_id)
WHERE
    lat is not null and lon is not null
GROUP BY
    1,2)
UNION ALL
(SELECT
    sensor_nodes_id,
    st_snaptogrid(pt3857(lon,lat),30) as geom,
    count(*),
    concat(sensor_nodes_id::text,st_astext(st_snaptogrid(pt3857(lon,lat),30))) AS uniq
FROM
    analyses
    LEFT JOIN sensors USING (sensors_id)
    LEFT JOIN sensor_systems USING (sensor_systems_id)
WHERE
    lat is not null and lon is not null
GROUP BY
    1,2)
;
CREATE INDEX ON mobile_generalized (sensor_nodes_id);
CREATE INDEX ON mobile_generalized USING GIST (geom, sensor_nodes_id);
CREATE UNIQUE INDEX ON mobile_generalized (uniq);

DROP MATERIALIZED VIEW IF EXISTS mobile_gen_boxes;
CREATE MATERIALIZED VIEW mobile_gen_boxes AS
SELECT
    sensor_nodes_id,
    st_expand(st_extent(geom),20)::geometry as box
FROM
    mobile_generalized
GROUP BY 1;
CREATE INDEX ON mobile_gen_boxes (sensor_nodes_id);
CREATE INDEX ON mobile_gen_boxes USING GIST (box, sensor_nodes_id);

--\i views.sql

--COMMIT;
