DROP TABLE IF EXISTS sensors CASCADE;
CREATE TABLE IF NOT EXISTS sensors (
    sensors_id int generated always as identity primary key,
    sensor_systems_id int not null,
    measurands_id int not null,
    source_id text,
    metadata jsonb
);

ALTER TABLE sensors ADD CONSTRAINT ss_s_fkey
    FOREIGN KEY (sensor_systems_id)
    REFERENCES sensor_systems (sensor_systems_id)
    DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE sensors ADD CONSTRAINT m_s_fkey
    FOREIGN KEY (measurands_id)
    REFERENCES measurands (measurands_id)
    DEFERRABLE INITIALLY IMMEDIATE;

CREATE INDEX IF NOT EXISTS sensors_measurands_id_idx ON sensors USING btree (measurands_id);
CREATE INDEX IF NOT EXISTS sensors_sensor_systems_id_idx ON sensors USING btree (sensor_systems_id);
CREATE UNIQUE INDEX IF NOT EXISTS sensors_sensor_systems_id_measurands_id_source_id_idx ON sensors USING btree (sensor_systems_id, measurands_id, source_id);

DROP TABLE IF EXISTS sensors_history CASCADE;
CREATE TABLE  IF NOT EXISTS sensors_history (
    sensors_id int,
    sensor_systems_id int,
    measurands_id int,
    source_id text,
    metadata jsonb,
    created timestamptz DEFAULT now()
);


CREATE OR REPLACE FUNCTION sensors_changes() RETURNS TRIGGER AS $$
DECLARE
BEGIN
    INSERT INTO sensors_history
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
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER sensors_change
AFTER INSERT OR UPDATE ON sensors
FOR EACH ROW EXECUTE PROCEDURE sensors_changes();
