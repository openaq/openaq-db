CREATE TABLE countries(
    iso text,
    name text,
    geom geometry
);
CREATE INDEX ON countries USING GIST (geom);
CREATE INDEX ON countries (iso);