CREATE TABLE IF NOT EXISTS timezones (
    gid integer primary key,
    tzid text,
    geog geography
);
CREATE INDEX on timezones USING GIST (geog);
