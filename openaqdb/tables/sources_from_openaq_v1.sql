CREATE TABLE IF NOT EXISTS sources_from_openaq (
    id int primary key,
    data json,
    source_name text
);
CREATE INDEX IF NOT EXISTS sources_from_openaq_source_name_idx on sources_from_openaq (source_name);
