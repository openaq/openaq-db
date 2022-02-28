-- a simple table to track the rejects

CREATE TABLE rejects (
    t timestamptz DEFAULT current_timestamp
    , tbl text
    , r jsonb
    , fetchlogs_id int NOT NULL REFERENCES fetchlogs ON DELETE CASCADE
);
