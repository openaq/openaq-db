-- ============================================================================
-- Deployment Configuration Schema
-- ============================================================================
-- Defines the structure for scheduling and configuring data adapter deployments.
-- Adapters fetch data from external sources, and deployments group adapters with
-- schedules, temporal offsets, and execution handlers.

--DROP SCHEMA IF EXISTS fetcher CASCADE;
CREATE SCHEMA IF NOT EXISTS fetcher;
SET search_path = fetcher, public;


-- ============================================================================
-- Core Tables
-- ============================================================================

-- Handlers: Define execution queues (SQS) for routing deployment jobs
-- Each deployment references a handler that determines where the job is sent
CREATE TABLE IF NOT EXISTS handlers (
    handlers_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , label text NOT NULL
  , description text
  , queue_name text NOT NULL UNIQUE
  , output_format text NOT NULL DEFAULT 'json.gz'
  , added_on timestamp with time zone DEFAULT now()
);


-- Fetcher Clients: Available adapter implementations
-- Each represents a specific data source client (air4thai, clarity, etc.)
-- Multiple adapters can use the same client with different configurations
CREATE SEQUENCE IF NOT EXISTS fetcher_clients_sq START 10;
CREATE TABLE IF NOT EXISTS fetcher_clients (
  fetcher_clients_id int PRIMARY KEY DEFAULT nextval('fetcher_clients_sq')
  , name text NOT NULL UNIQUE -- name of adapter/provider
  , handler text   -- lcs or fetcher
  , description text
  , authorization_method text -- NULL means none
  , accepts_start_date boolean DEFAULT 'f'
  , accepts_end_date boolean DEFAULT 'f'
  , accepts_offset boolean DEFAULT 'f'
  -- , output_format text NOT NULL DEFAULT 'json.gz'
  , added_on timestamp with time zone DEFAULT now()
);


-- Adapters: Links providers to fetcher clients with provider-specific configuration
-- Allows the same fetcher client to be reused across multiple providers
-- The config field stores provider-specific parameters (URLs, field mappings, etc.)
CREATE TABLE IF NOT EXISTS adapters (
    adapters_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , providers_id int NOT NULL REFERENCES public.providers ON DELETE CASCADE
  , fetcher_clients_id int NOT NULL REFERENCES fetcher_clients ON DELETE CASCADE
  , config jsonb NOT NULL DEFAULT '{}' -- this is to make the unique constraint better, avoids nulls
  , added_on timestamp with time zone DEFAULT now()
  , UNIQUE (providers_id, fetcher_clients_id, config) -- should not allow multiples of the same thing
);


-- ============================================================================
-- Domain Types
-- ============================================================================

-- Cron Expression: Custom domain type that validates cron syntax on insert/update
-- Ensures only valid cron expressions (5 fields: minute hour day month weekday) are stored
-- Examples: "*/15 * * * *" (every 15 min), "0 9 * * 1-5" (weekdays at 9am)
CREATE DOMAIN cronexpr AS TEXT
  CONSTRAINT is_valid_cron_expression CHECK (cron_validate_expr(VALUE));


-- Deployments: Scheduling definitions for groups of adapters
-- Each deployment runs on a cron schedule and may include temporal offset for delayed data
-- last_deployed_datetime tracks most recent queue time for monitoring
CREATE TABLE IF NOT EXISTS deployments (
    deployments_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , handlers_id int NOT NULL REFERENCES handlers DEFAULT 1
  , label text NOT NULL UNIQUE
  , description text
  , temporal_offset int
  , filename_prefix text NOT NULL
  , is_active boolean NOT NULL DEFAULT 't'
  , schedule cronexpr NOT NULL
  , last_deployed_datetime timestamptz
  , added_on timestamp with time zone DEFAULT now()
);


-- Deployment Adapters: Junction table linking deployments to adapters (many-to-many)
-- Allows a single deployment to run multiple adapters, or an adapter to run in multiple deployments
CREATE TABLE IF NOT EXISTS deployment_adapters (
    deployments_id int NOT NULL REFERENCES deployments
  , adapters_id int NOT NULL REFERENCES adapters
  , added_on timestamp with time zone DEFAULT now()
  , PRIMARY KEY (deployments_id, adapters_id)
);

-- ============================================================================
-- Query Function: Get Ready Deployments
-- ============================================================================
-- Read-only function that identifies which deployments should run at given time
-- Includes adapter count to distinguish between configured and empty deployments
-- Use this for testing, monitoring, or "dry run" inspection without queueing jobs
--
-- Returns:
--   - deployments_id: Deployment identifier
--   - key: Unique fetchlog key (format: YYYY-MM-DD/prefix/prefix-YYYYMMDDHH24MI)
--   - deployment_config: Full JSON payload with adapters, offsets, queue info
--   - queue_name: SQS queue for routing execution
--   - adapters_count: Number of adapters assigned (0 = not ready for queueing)
--
-- Example: SELECT * FROM get_ready_deployments('2026-01-27 14:30:00');
CREATE OR REPLACE FUNCTION get_ready_deployments(
  check_time timestamptz DEFAULT now()
)
RETURNS TABLE (
  deployments_id int
  , key text
  , deployment_config jsonb
  , queue_name text
  , adapters_count bigint
) AS $$
BEGIN
  SET search_path = fetcher, public;
  RETURN QUERY
  WITH deployment_adapters_config AS (
          SELECT da.deployments_id
          , COUNT(1) as adapters_count
          , jsonb_agg(
           jsonb_build_object(
            'adapters_id', a.adapters_id
            , 'fetcher_client', ac.name
            , 'providers_id', p.providers_id
            , 'provider_name', p.source_name
            , 'config', a.config
          )
        ) as adapters_config
        FROM deployment_adapters da
        JOIN adapters a ON da.adapters_id = a.adapters_id
        JOIN fetcher_clients ac ON a.fetcher_clients_id = ac.fetcher_clients_id
        JOIN public.providers p ON a.providers_id = p.providers_id
        GROUP BY da.deployments_id
  )
  SELECT d.deployments_id
    , format('%1$s/%2$s/%2$s-%3$s.%4$s'
        , to_char(check_time, 'YYYY-MM-DD')
        , d.filename_prefix
        , to_char(check_time, 'YYYYMMDDHH24MI')
        , h.output_format
     ) as key
    , jsonb_build_object(
      'deployments_id', d.deployments_id
      , 'label', d.label
      , 'temporal_offset', d.temporal_offset
      , 'scheduled_time', check_time
      , 'queue_name', h.queue_name
      , 'adapters', COALESCE(c.adapters_config, '[]'::jsonb)
  ) as deployment_config
  , h.queue_name
  , COALESCE(c.adapters_count, 0) as adapters_count
  FROM deployments d
  LEFT JOIN deployment_adapters_config c ON (c.deployments_id = d.deployments_id)
  JOIN handlers h ON d.handlers_id = h.handlers_id
  WHERE d.is_active
  AND is_cron_ready(d.schedule, check_time);
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- Action Function: Queue Deployments
-- ============================================================================
-- Queues ready deployments to fetchlogs table for execution by adapter application
-- Only queues deployments with adapters assigned (adapters_count > 0)
-- Uses ON CONFLICT to ensure idempotency - same deployment+time only queued once
-- Updates last_deployed_datetime for successfully queued deployments
--
-- Typically called by pg_cron every minute: SELECT queue_deployments();
--
-- Returns: Count of deployments successfully queued (0 if all already queued or conflicted)
CREATE OR REPLACE FUNCTION queue_deployments(
  check_time timestamptz DEFAULT now()
)
RETURNS int AS $$
DECLARE
  rows_count int;
BEGIN
  SET search_path = fetcher, public;
  WITH ready_deployments AS (
    SELECT * FROM get_ready_deployments(check_time)
  ),
  inserted_rows AS (
    INSERT INTO public.fetchlogs (
      key
      , scheduled_datetime
      , deployment_config
      , queue_name
    )
    SELECT
      key
      , check_time
      , deployment_config
      , queue_name
    FROM ready_deployments
    WHERE adapters_count > 0
    ON CONFLICT (key) DO NOTHING
    RETURNING (deployment_config->>'deployments_id')::int as deployments_id
  )
  UPDATE deployments d
  SET last_deployed_datetime = check_time
  FROM inserted_rows ir
  WHERE d.deployments_id = ir.deployments_id;
  GET DIAGNOSTICS rows_count = ROW_COUNT;
  RETURN rows_count;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- Polling Function: Get and Mark Queued Jobs
-- ============================================================================
-- Atomically retrieves scheduled jobs from fetchlogs and marks them as queued
-- Used by adapter application to poll for work
-- Only returns jobs scheduled for now or earlier (excludes future jobs)
--
-- Parameters:
--   - job_limit: Maximum number of jobs to return (NULL = no limit)
--
-- Returns: Table of fetchlog records with deployment config
-- Side effect: Sets queued_datetime = now() for returned records
--
-- Example: SELECT * FROM get_and_mark_queued_jobs(10);
CREATE OR REPLACE FUNCTION get_and_mark_queued_jobs(
  job_limit int DEFAULT NULL
)
RETURNS TABLE (
  fetchlogs_id int
  , key text
  , scheduled_datetime timestamptz
  , queued_datetime timestamptz
  , queue_name text
  , deployment_config jsonb
) AS $$
BEGIN
  SET search_path = fetcher, public;
  RETURN QUERY
  WITH jobs_to_queue AS (
    SELECT f.fetchlogs_id
    FROM public.fetchlogs f
    WHERE f.scheduled_datetime IS NOT NULL
    AND f.scheduled_datetime <= now()
    AND f.queued_datetime IS NULL
    ORDER BY f.scheduled_datetime ASC
    LIMIT job_limit
    FOR UPDATE SKIP LOCKED
  ),
  updated_jobs AS (
    UPDATE public.fetchlogs f
    SET queued_datetime = now()
    FROM jobs_to_queue j
    WHERE f.fetchlogs_id = j.fetchlogs_id
    RETURNING f.fetchlogs_id
      , f.key
      , f.scheduled_datetime
      , f.queued_datetime
      , f.queue_name
      , f.deployment_config
  )
  SELECT * FROM updated_jobs;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- Monitoring Views
-- ============================================================================

-- Stuck Jobs: Identifies jobs that were queued but never progressed
-- These may indicate application crashes, network issues, or other failures
-- Default threshold: 1 hour, but can be adjusted in the view definition
CREATE OR REPLACE VIEW stuck_jobs AS
SELECT
  f.fetchlogs_id
  , f.key
  , f.scheduled_datetime
  , f.queued_datetime
  , f.queue_name
  , f.deployment_config
  , now() - f.queued_datetime as stuck_duration
FROM public.fetchlogs f
WHERE f.queued_datetime IS NOT NULL
AND f.queued_datetime < now() - interval '1 hour'
AND f.loaded_datetime IS NULL
ORDER BY f.queued_datetime ASC;


-- Deployment Health: Summary of deployment execution history and status
-- Shows last run time, recent success/failure counts, and staleness indicators
CREATE OR REPLACE VIEW deployment_health AS
SELECT
  d.deployments_id
  , d.label
  , d.schedule
  , d.is_active
  , d.last_deployed_datetime
  , d.temporal_offset
  , h.queue_name
  , now() - d.last_deployed_datetime as time_since_last_run
  , (SELECT COUNT(*)
     FROM public.fetchlogs f
     WHERE f.deployment_config->>'deployments_id' = d.deployments_id::text
     AND f.scheduled_datetime > now() - interval '24 hours') as runs_last_24h
  , (SELECT COUNT(*)
     FROM public.fetchlogs f
     WHERE f.deployment_config->>'deployments_id' = d.deployments_id::text
     AND f.completed_datetime IS NOT NULL
     AND f.scheduled_datetime > now() - interval '24 hours') as completed_last_24h
  , (SELECT COUNT(*)
     FROM public.fetchlogs f
     WHERE f.deployment_config->>'deployments_id' = d.deployments_id::text
     AND f.has_error = true
     AND f.scheduled_datetime > now() - interval '24 hours') as errors_last_24h
  , (SELECT COUNT(*)
     FROM deployment_adapters da
     WHERE da.deployments_id = d.deployments_id) as adapters_count
FROM deployments d
JOIN handlers h ON d.handlers_id = h.handlers_id
ORDER BY d.is_active DESC, d.last_deployed_datetime DESC NULLS LAST;



  SET search_path = public;

  COMMIT;
