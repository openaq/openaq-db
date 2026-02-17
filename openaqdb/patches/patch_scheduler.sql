
BEGIN;

  -- add a field to the fetchlogs
ALTER TABLE fetchlogs
    -- first the time that the deployment was scheduled
    ADD COLUMN IF NOT EXISTS scheduled_datetime timestamptz
   -- the name of the message queue to post to
  , ADD COLUMN IF NOT EXISTS queue_name text
  -- and then once the poller picks it up and queues it
  , ADD COLUMN IF NOT EXISTS queued_datetime timestamptz
  -- and the payload to pass to the queue
  , ADD COLUMN IF NOT EXISTS fetcher_config jsonb;


CREATE INDEX IF NOT EXISTS fetchlogs_unqueued_scheduled_idx
ON fetchlogs(scheduled_datetime)
WHERE scheduled_datetime IS NOT NULL AND queued_datetime IS NULL;


\i ../fetcher/scheduler.sql
\i ../fetcher/deployments.sql
\i ../fetcher/deployment_data.sql


GRANT USAGE ON SCHEMA fetcher TO postgresread, postgreswrite;
GRANT SELECT ON ALL TABLES IN SCHEMA fetcher TO postgresread;
GRANT ALL ON ALL TABLES IN SCHEMA fetcher TO postgreswrite;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA fetcher TO postgreswrite;

COMMIT;
