CREATE TABLE IF NOT EXISTS fetchlogs(
    fetchlogs_id int generated always as identity primary key,
    key text UNIQUE NOT NULL, -- name of the fetchlog file
    init_datetime timestamptz,
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
    , scheduled_datetime timestamptz
    , queue_name text
    , queued_datetime timestamptz
    , deployment_config jsonb
);

CREATE INDEX IF NOT EXISTS fetchlogs_completed_datetime_idx ON fetchlogs(completed_datetime);
CREATE INDEX IF NOT EXISTS fetchlogs_last_modified_idx ON fetchlogs (last_modified);


-- we ingest many fetchlog files at a time so this
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


-- A table to store responses sent from the fetchers
CREATE TABLE IF NOT EXISTS fetcher_responses (
    source_name text NOT NULL
  , datetime timestamptz NOT NULL DEFAULT now()
  , message text NOT NULL
  , records int NOT NULL DEFAULT 0
  , locations int
  , datetime_from timestamptz
  , datetime_to timestamptz
  , duration_seconds real
  , errors json
  , parameters json
);



CREATE OR REPLACE FUNCTION fetcher_source_summary(st timestamptz, et timestamptz DEFAULT now()) RETURNS TABLE (
   source_name text
  , n int
  , zeros int
  , pct_success double precision
  , min double precision
  , p02 double precision
  , p25 double precision
  , p50 double precision
  , avg double precision
  , sd double precision
  , p75 double precision
  , p98 double precision
  , max double precision
  , skew double precision
) AS $$
WITH fetcher_agg AS (
SELECT source_name
  , COUNT(1) as n
  , SUM((records=0)::int) AS zeros
  , MIN(records) as min
  , PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY records) as p02
  , PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY records) as p25
  , PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY records) as p50
  , AVG(records) as avg
  , STDDEV(records) as sd
  , PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY records) as p75
  , PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY records) as p98
  , MAX(records) as max
  FROM fetcher_responses
  WHERE datetime > st
  AND datetime < et
  GROUP BY source_name)
  SELECT source_name
  , n
  , zeros
  , CASE WHEN zeros = 0 THEN 100 ELSE ROUND(((1-(zeros::numeric/n::numeric)) * 100.0),1) END as pct_success
  , min
  , ROUND(p02::numeric,1) as p02
  , ROUND(p25::numeric,1) as p25
  , ROUND(p50::numeric,1) as p50
  , ROUND(avg::numeric,1) as avg
  , ROUND(sd::numeric,1) as sd
  , ROUND(p75::numeric,1) as p75
  , ROUND(p98::numeric,1) as p98
  , max
  , CASE WHEN sd>0 THEN ROUND(((3*(avg-p50))/sd)::numeric,2) ELSE 0 END AS skew
  FROM fetcher_agg;
  $$ LANGUAGE SQL;
