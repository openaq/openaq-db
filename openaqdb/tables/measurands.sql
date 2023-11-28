CREATE TYPE parameter_type AS ENUM (
  'pollutant'
, 'meteorological'
);

CREATE TABLE IF NOT EXISTS measurands (
    measurands_id int generated always as identity primary key,
    measurand text not null,
    units text not null,
    display text,
    description text,
    parameter_type parameter_type NOT NULL DEFAULT 'pollutant',
    unique (measurand, units)
);


CREATE TABLE IF NOT EXISTS measurands_map (
  key text NOT NULL
  , measurands_id int NOT NULL REFERENCES measurands ON DELETE CASCADE
  , units text NOT NULL
  , source_name text NOT NULL
  , UNIQUE(key, units)
);

CREATE OR REPLACE VIEW measurands_map_view AS
SELECT measurands_id
, key
FROM measurands_map
UNION ALL
SELECT measurands_id
, concat(measurand, units)
FROM measurands
GROUP BY 1,2;
