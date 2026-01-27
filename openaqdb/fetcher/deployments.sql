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
  , added_on timestamp with time zone DEFAULT now()
);


-- Adapter Clients: Available adapter implementations
-- Each represents a specific data source client (air4thai, clarity, etc.)
-- Multiple adapters can use the same client with different configurations
CREATE SEQUENCE IF NOT EXISTS adapter_clients_sq START 10;
CREATE TABLE IF NOT EXISTS adapter_clients (
  adapter_clients_id int PRIMARY KEY DEFAULT nextval('adapter_clients_sq')
  , name text NOT NULL UNIQUE -- name of adapter/provider
  , handler text   -- lcs or fetcher
  , description text
  , authorization_method text -- NULL means none
  , accepts_start_date boolean DEFAULT 'f'
  , accepts_end_date boolean DEFAULT 'f'
  , accepts_offset boolean DEFAULT 'f'
  , added_on timestamp with time zone DEFAULT now()
);


-- Adapters: Links providers to adapter clients with provider-specific configuration
-- Allows the same adapter client to be reused across multiple providers
-- The config field stores provider-specific parameters (URLs, field mappings, etc.)
CREATE TABLE IF NOT EXISTS adapters (
    adapters_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , providers_id int NOT NULL REFERENCES public.providers ON DELETE CASCADE
  , adapter_clients_id int NOT NULL REFERENCES adapter_clients ON DELETE CASCADE
  , config jsonb
  , added_on timestamp with time zone DEFAULT now()
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
  deployments_id int,
  key text,
  deployment_config jsonb,
  queue_name text,
  adapters_count bigint
) AS $$
BEGIN
  SET search_path = fetcher, public;
  RETURN QUERY
  WITH deployment_adapters_config AS (
          SELECT da.deployments_id
          , COUNT(1) as adapters_count
          , jsonb_agg(
           jsonb_build_object(
            'adapters_id', a.adapters_id,
            'adapter_client', ac.name,
            'providers_id', p.providers_id,
            'provider_name', p.source_name,
            'config', a.config
          )
        ) as adapters_config
        FROM deployment_adapters da
        JOIN adapters a ON da.adapters_id = a.adapters_id
        JOIN adapter_clients ac ON a.adapter_clients_id = ac.adapter_clients_id
        JOIN public.providers p ON a.providers_id = p.providers_id
        GROUP BY da.deployments_id
  )
  SELECT d.deployments_id,
    to_char(check_time, 'YYYY-MM-DD') || '/' || d.filename_prefix || '/' || d.filename_prefix || '-' || to_char(check_time, 'YYYYMMDDHH24MI') as key,
    jsonb_build_object(
      'deployments_id', d.deployments_id,
      'label', d.label,
      'temporal_offset', d.temporal_offset,
      'scheduled_time', check_time,
      'queue_name', h.queue_name,
      'adapters', COALESCE(c.adapters_config, '[]'::jsonb)
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
      key,
      scheduled_datetime,
      deployment_config,
      queue_name
    )
    SELECT
      key,
      check_time,
      deployment_config,
      queue_name
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



  SET search_path = public;

  COMMIT;
