CREATE TABLE IF NOT EXISTS countries(
    countries_id int generated always as identity primary key,
    iso text,
    name text,
    geom geometry
);
CREATE INDEX ON countries USING GIST (geom);
CREATE INDEX ON countries (iso);
