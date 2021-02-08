CREATE TABLE  IF NOT EXISTS sensor_nodes (
    sensor_nodes_id int generated always as identity primary key,
    ismobile boolean,
    geom geometry,
    site_name text,
    source_name text,
    city text,
    country text,
    metadata jsonb,
    source_id text,
    origin text
);
CREATE INDEX IF NOT EXISTS sensor_nodes_geom_idx ON sensor_nodes USING gist (geom);
CREATE INDEX IF NOT EXISTS sensor_nodes_metadata_idx ON sensor_nodes USING gin (metadata);
CREATE INDEX IF NOT EXISTS sensor_nodes_site_name_source_name_idx ON sensor_nodes USING btree (site_name, source_name);
CREATE UNIQUE INDEX IF NOT EXISTS sensor_nodes_source_name_source_id_idx ON sensor_nodes USING btree (source_name, source_id);


CREATE TABLE  IF NOT EXISTS sensor_nodes_history (
    sensor_nodes_id int,
    ismobile boolean,
    geom geometry,
    site_name text,
    source_name text,
    city text,
    country text,
    metadata jsonb,
    source_id text,
    origin text,
    created timestamptz DEFAULT now()
);

CREATE INDEX ON sensor_nodes_history (sensor_nodes_id);
CREATE INDEX ON sensor_nodes_history (site_name);

CREATE TABLE IF NOT EXISTS sensor_nodes_harrays (
    sensor_nodes_id integer primary key,
    cities text[],
    source_names text[],
    site_names text[]
);


CREATE OR REPLACE FUNCTION sensor_node_changes() RETURNS TRIGGER AS $$
DECLARE
BEGIN
    INSERT INTO sensor_nodes_harrays (
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
            cities=array_distinct(array_cat(sensor_nodes_harrays.cities, EXCLUDED.cities), true),
            source_names=array_distinct(array_cat(sensor_nodes_harrays.source_names, EXCLUDED.source_names), true),
            site_names=array_distinct(array_cat(sensor_nodes_harrays.site_names, EXCLUDED.site_names), true)
    ;
    INSERT INTO sensor_nodes_history
    SELECT
        NEW.sensor_nodes_id,
        NEW.ismobile,
        NEW.geom,
        NEW.site_name,
        NEW.source_name,
        NEW.city,
        NEW.country,
        NEW.metadata,
        now(),
        NEW.source_id;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER sensor_node_change
AFTER INSERT OR UPDATE ON sensor_nodes
FOR EACH ROW EXECUTE PROCEDURE sensor_node_changes();
