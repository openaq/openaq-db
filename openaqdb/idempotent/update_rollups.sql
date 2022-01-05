CREATE OR REPLACE FUNCTION update_sources() RETURNS VOID AS $$
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

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION update_groups() RETURNS VOID AS $$
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
    JOIN groups ON (country=name)
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
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION rollups_daily(
    _start timestamptz = now()
)
RETURNS VOID
LANGUAGE PLPGSQL
SET SEARCH_PATH TO public
AS $$
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
        min(datetime) as first_datetime,
        max(datetime) as last_datetime,
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
        last(sensors_id, last_datetime) as sensors_id,
        rollup,
        st,
        et,
        min(first_datetime) as first_datetime,
        max(last_datetime) as last_datetime,
        sum(value_count) as value_count,
        sum(value_sum) as value_sum,
        last(last_value, last_datetime) as last_value,
        min(minx) as minx,
        min(miny) as miny,
        max(maxx) as maxx,
        max(maxy) as maxy,
        last(last_point, last_datetime) as last_point
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
        first_datetime,
        last_datetime,
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
$$;

-- Added just to make it easier to rebuild the daily rollups during dev
DROP FUNCTION IF EXISTS rollups_daily_full();
CREATE OR REPLACE FUNCTION rollups_daily_full() RETURNS VOID AS $$
WITH days AS (
  SELECT date_trunc('day', datetime - '1sec'::interval) as day
  FROM measurements
  GROUP BY date_trunc('day', datetime - '1sec'::interval)
)
SELECT rollups_daily(day)
FROM days;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION rollups_monthly(
    _start timestamptz = now()
)
RETURNS VOID
LANGUAGE PLPGSQL
SET SEARCH_PATH TO public
AS $$
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
        first_datetime,
        last_datetime,
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
        last(sensors_id, last_datetime),
        'month',
        _st,
        _et,
        min(first_datetime),
        max(last_datetime),
        sum(value_count),
        sum(value_sum),
        last(last_value, last_datetime),
        min(minx),
        min(miny),
        max(maxx),
        max(maxy),
        last(last_point, last_datetime)
    FROM rollups
    WHERE
        rollup = 'day' AND
        st>= _st and st <= _et
    GROUP BY 1,2,4,5,6
    ;
END;
$$;

CREATE OR REPLACE FUNCTION rollups_yearly(
    _start timestamptz = now()
)
RETURNS VOID
LANGUAGE PLPGSQL
SET SEARCH_PATH TO public
AS $$
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
        first_datetime,
        last_datetime,
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
        last(sensors_id, last_datetime),
        'year',
        _st,
        _et,
        min(first_datetime),
        max(last_datetime),
        sum(value_count),
        sum(value_sum),
        last(last_value, last_datetime),
        min(minx),
        min(miny),
        max(maxx),
        max(maxy),
        last(last_point, last_datetime)
    FROM rollups
        WHERE
            rollup = 'month' AND
        st>= _st and st <= _et
        GROUP BY 1,2,4,5,6
        ;
END;
$$;



CREATE OR REPLACE FUNCTION rollups_total()
RETURNS VOID
LANGUAGE PLPGSQL
SET SEARCH_PATH TO public
AS $$
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
            first_datetime,
            last_datetime,
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
            last(sensors_id, last_datetime),
            'total',
            '1970-01-01'::timestamptz,
            '2999-01-01'::timestamptz,
            min(first_datetime),
            max(last_datetime),
            sum(value_count),
            sum(value_sum),
            last(last_value, last_datetime),
        min(minx),
        min(miny),
        max(maxx),
        max(maxy),
        last(last_point, last_datetime)
        FROM rollups
        WHERE
            rollup = 'year'
        GROUP BY 1,2,4,5,6
        ;
END;
$$;

CREATE OR REPLACE PROCEDURE run_updates(job_id int default Null, config jsonb default Null)
LANGUAGE PLPGSQL
AS $$
DECLARE
_st timestamptz;
_et timestamptz;
t timestamptz;
BEGIN
    SELECT (config->>'start')::timestamptz INTO STRICT _st;
    SELECT (config->>'end')::timestamptz INTO STRICT _et;

    _st:=date_trunc('day',coalesce(_st, now() - '1 days'::interval));
    _et:=date_trunc('day',coalesce(_et, now()));

    RAISE NOTICE 'updating timezones';
    update sensor_nodes
    set metadata = metadata || jsonb_build_object('timezone',timezone(geom))
    where not metadata ? 'timezone' and geom is not null;

    update sensor_nodes
    set metadata = metadata || jsonb_build_object('timezone',timezone(sn_lastpoint(sensor_nodes_id)))
    where not metadata ? 'timezone' and geom is null and ismobile;

    RAISE NOTICE 'updating countries';
    update sensor_nodes set country = country(geom)
    where country is null and geom is not null;
    update sensor_nodes set country = country(sn_lastpoint(sensor_nodes_id))
    where country is null and geom is null and ismobile;
    COMMIT;

    RAISE NOTICE 'Updating sources Tables';
    PERFORM update_sources();
    COMMIT;

    RAISE NOTICE 'Updating Groups Tables';
    PERFORM update_groups();
    COMMIT;

    FOR t IN
        (SELECT g FROM generate_series(_st, _et, '1 day'::interval) as g)
    LOOP
        --CALL refresh_continuous_aggregate('measurements_daily',_st,_et);
        PERFORM rollups_daily(t);
        COMMIT;
    END LOOP;

    FOR t IN
        (SELECT g FROM generate_series(_st, _et, '1 month'::interval) as g)
    LOOP
        PERFORM rollups_monthly(t);
        COMMIT;
    END LOOP;

    FOR t IN
        (SELECT g FROM generate_series(_st, _et, '1 year'::interval) as g)
    LOOP
        PERFORM rollups_yearly(t);
        COMMIT;
    END LOOP;

    PERFORM rollups_total();
    COMMIT;

    RAISE NOTICE 'REFRESHING sensors_first_last';
    REFRESH MATERIALIZED VIEW sensors_first_last;
    COMMIT;

    RAISE NOTICE 'REFRESHING sensor_nodes_json';
    REFRESH MATERIALIZED VIEW sensor_nodes_json;
    COMMIT;

    RAISE NOTICE 'REFRESHING groups_view';
    REFRESH MATERIALIZED VIEW groups_view;
    COMMIT;

    RAISE NOTICE 'REFRESHING sensor_stats';
    REFRESH MATERIALIZED VIEW sensor_stats;
    COMMIT;

    RAISE NOTICE 'REFRESHING city_stats';
    REFRESH MATERIALIZED VIEW city_stats;
    COMMIT;

    RAISE NOTICE 'REFRESHING country_stats';
    REFRESH MATERIALIZED VIEW country_stats;
    COMMIT;

    RAISE NOTICE 'REFRESHING locations_base_v2';
    REFRESH MATERIALIZED VIEW locations_base_v2;
    COMMIT;

    RAISE NOTICE 'REFRESHING locations';
    REFRESH MATERIALIZED VIEW locations;
    COMMIT;

    RAISE NOTICE 'REFRESHING measurements_fastapi_base';
    REFRESH MATERIALIZED VIEW measurements_fastapi_base;
    COMMIT;


END;
$$;


DROP PROCEDURE IF EXISTS run_updates_full();
CREATE OR REPLACE PROCEDURE run_updates_full() AS $$
DECLARE
_start timestamptz;
BEGIN
SELECT MIN(datetime) INTO _start FROM measurements;
CALL run_updates(NULL, jsonb_build_object('start', _start));
END;
$$ LANGUAGE plpgsql;
