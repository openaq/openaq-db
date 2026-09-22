CREATE TABLE IF NOT EXISTS countries(
    countries_id int generated always as identity primary key,
    iso text,
    iso_a3 text,
    name text,
    geog geography
);
CREATE INDEX ON countries USING GIST (geog);
CREATE INDEX ON countries (iso);
CREATE INDEX IF NOT EXISTS countries_geom_idx ON countries USING gist ((geog::geometry));
