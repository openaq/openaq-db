
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
  , ADD COLUMN IF NOT EXISTS deployment_config jsonb;


\i ../scheduler.sql
\i ../deployments.sql


COMMIT;
