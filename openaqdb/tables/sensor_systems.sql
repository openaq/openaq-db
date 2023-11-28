DROP TABLE IF EXISTS sensor_systems CASCADE;
CREATE TABLE  IF NOT EXISTS sensor_systems (
    sensor_systems_id int generated always as identity primary key,
    sensor_nodes_id int not null,
    metadata jsonb,
    source_id text
);

CREATE INDEX IF NOT EXISTS  sensor_systems_sensor_nodes_id_idx ON sensor_systems USING btree (sensor_nodes_id);
CREATE UNIQUE INDEX IF NOT EXISTS  sensor_systems_sensor_nodes_id_source_id_idx ON sensor_systems USING btree (sensor_nodes_id, source_id);

ALTER TABLE sensor_systems ADD CONSTRAINT sn_ss_fkey
    FOREIGN KEY (sensor_nodes_id)
    REFERENCES sensor_nodes (sensor_nodes_id)
    DEFERRABLE INITIALLY IMMEDIATE;

DROP TABLE IF EXISTS sensor_systems_history CASCADE;
CREATE TABLE  IF NOT EXISTS sensor_systems_history (
    sensor_systems_id int,
    sensor_nodes_id int,
    metadata jsonb,
    source_id text,
    created timestamptz DEFAULT now()
);

CREATE OR REPLACE FUNCTION sensor_systems_changes() RETURNS TRIGGER AS $$
DECLARE
BEGIN
        INSERT INTO sensor_systems_history
        (sensor_systems_id, sensor_nodes_id, source_id, metadata, created)
        SELECT
            NEW.sensor_systems_id,
            NEW.sensor_nodes_id,
            NEW.source_id,
            NEW.metadata,
            now();
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER sensor_systems_change
AFTER INSERT OR UPDATE ON sensor_systems
FOR EACH ROW EXECUTE PROCEDURE sensor_systems_changes();
