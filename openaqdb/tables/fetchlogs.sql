CREATE TABLE IF NOT EXISTS fetchlogs(
    fetchlogs_id int generated always as identity primary key,
    key text UNIQUE NOT NULL,
    init_datetime timestamptz DEFAULT clock_timestamp(),
    loaded_datetime timestamptz,
    completed_datetime timestamptz,
    jobs int default 0,
    inserted int default 0,
    records int default 0,
    batch_uuid text,
    first_recorded_datetime timestamptz,
    last_recorded_datetime timestamptz,
    first_inserted_datetime timestamptz,
    last_inserted_datetime timestamptz,
    file_size int,
    last_message text,
    last_modified timestamptz
);

CREATE INDEX IF NOT EXISTS fetchlogs_completed_datetime_idx ON fetchlogs(completed_datetime);
CREATE INDEX IF NOT EXISTS fetchlogs_last_modified_idx ON fetchlogs (last_modified);
