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
    last_message text
    , last_modified timestamptz
    , has_error bool NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS fetchlogs_completed_datetime_idx ON fetchlogs(completed_datetime);
CREATE INDEX IF NOT EXISTS fetchlogs_last_modified_idx ON fetchlogs (last_modified);


DROP TABLE IF EXISTS ingest_stats;
CREATE TABLE IF NOT EXISTS ingest_stats (
    ingest_method text NOT NULL PRIMARY KEY -- lcs vs realtime vs stations
  -- Total counts
  , total_measurements_processed bigint DEFAULT 0
  , total_measurements_inserted bigint DEFAULT 0
  , total_measurements_rejected bigint DEFAULT 0
  , total_nodes_processed bigint DEFAULT 0
  , total_nodes_inserted bigint DEFAULT 0
  , total_nodes_updated bigint DEFAULT 0
  , total_nodes_rejected bigint DEFAULT 0
  -- total Times
  , total_process_time_ms int DEFAULT 0
  , total_insert_time_ms int DEFAULT 0
  , total_cache_time_ms int DEFAULT 0
  -- Latest counts
  , latest_measurements_processed bigint DEFAULT 0
  , latest_measurements_inserted bigint DEFAULT 0
  , latest_measurements_rejected bigint DEFAULT 0
  , latest_nodes_processed bigint DEFAULT 0
  , latest_nodes_inserted bigint DEFAULT 0
  , latest_nodes_updated bigint DEFAULT 0
  , latest_nodes_rejected bigint DEFAULT 0
  -- latest Times
  , latest_process_time_ms int DEFAULT 0
  , latest_insert_time_ms int DEFAULT 0
  , latest_cache_time_ms int DEFAULT 0
  , ingest_count int DEFAULT 1
  , started_on timestamptz DEFAULT now() -- start time for stats
  , ingested_on timestamptz DEFAULT now()  -- last update for stats
);
