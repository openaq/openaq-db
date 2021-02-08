CREATE TABLE sources_from_openaq (
    id int primary key,
    data json,
    source_name text
);
CREATE INDEX on sources_from_openaq (source_name);