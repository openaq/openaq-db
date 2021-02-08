CREATE TABLE IF NOT EXISTS groups (
    groups_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type text NOT NULL,
    name text NOT NULL,
    subtitle text,
    metadata jsonb,
    geog geography,
    UNIQUE (type, name)
);

CREATE TABLE IF NOT EXISTS groups_sensors (
    groups_id int REFERENCES groups (groups_id) on delete cascade on update cascade,
    sensors_id int REFERENCES sensors (sensors_id) on delete cascade on update cascade,
    PRIMARY KEY (groups_id, sensors_id)
);