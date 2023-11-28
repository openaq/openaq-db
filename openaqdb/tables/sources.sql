CREATE TABLE sources(
    sources_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slug text,
    name text not null,
 --   type text not null,
    readme text,
    metadata jsonb
);
CREATE INDEX ON sources (slug);
CREATE UNIQUE INDEX on sources (name);

CREATE TABLE sensor_nodes_sources (
    sensor_nodes_id int references sensor_nodes(sensor_nodes_id),
    sources_id int references sources(sources_id),
    unique(sensor_nodes_id, sources_id)
);

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
