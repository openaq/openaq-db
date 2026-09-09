CREATE TABLE IF NOT EXISTS timezones (
    timezones_id int generated always as identity primary key,
    tzid text,
    geog geography
);
CREATE INDEX on timezones USING GIST (geog);
CREATE INDEX ON timezones (tzid);
CREATE INDEX IF NOT EXISTS timezones_geom_idx ON timezones USING gist ((geog::geometry))
