CREATE TABLE IF NOT EXISTS fetchlogs(
    fetchlogs_id int generated always as identity primary key,
    key text UNIQUE NOT NULL,
    init_datetime timestamptz DEFAULT clock_timestamp(),
    loaded_datetime timestamptz,
    completed_datetime timestamptz,
    last_message text,
    last_modified timestamptz
);

CREATE INDEX IF NOT EXISTS fetchlogs_completed_datetime_idx ON fetchlogs(completed_datetime);
CREATE INDEX IF NOT EXISTS fetchlogs_last_modified_idx ON fetchlogs (last_modified);