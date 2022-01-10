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
