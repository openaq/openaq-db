CREATE TABLE IF NOT EXISTS timezones (
    timezones_id int generated always as identity primary key,
    tzid text,
    geog geography
);
CREATE INDEX on timezones USING GIST (geog);
