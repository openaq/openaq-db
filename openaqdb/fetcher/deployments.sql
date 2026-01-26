--DROP SCHEMA IF EXISTS fetcher CASCADE;
CREATE SCHEMA IF NOT EXISTS fetcher;
SET search_path = fetcher, public;


  -- an adapter_client table that keeps track of the custom clients we have made
  -- and then an adapter table that lists those clients and any config that goes with them,
  -- -- this is where any upload tool configs will go
  -- finally the deployments table taht will combind the adapters with a schedule and offset

-- create a list of queues that will accept a fetch job
  -- the queue name will will need to be referenced in the deployments
  -- and is currently an SQS queue name that is set to trigger a lmabda
CREATE TABLE IF NOT EXISTS handlers (
    handlers_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , label text NOT NULL
  , description text
  , queue_name text NOT NULL UNIQUE
  , added_on timestamp with time zone DEFAULT now()
);


-- this will list all our clients we use for adapters
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

  -- instead of building off the providers table we create this link table
  -- which allows us to create more than one adapter for a given provider
  -- if we need to, which mostly we should not
CREATE TABLE IF NOT EXISTS adapters (
    adapters_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , providers_id int NOT NULL REFERENCES public.providers ON DELETE CASCADE
  , adapter_clients_id int NOT NULL REFERENCES adapter_clients ON DELETE CASCADE
  , config jsonb
  , added_on timestamp with time zone DEFAULT now()
);


 -- * 1,2,3 */5 1-3
CREATE DOMAIN cronexpr AS TEXT
  CONSTRAINT is_valid_cron_expression CHECK (cron_validate_expr(VALUE));


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


  CREATE TABLE IF NOT EXISTS deployment_adapters (
    deployments_id int NOT NULL REFERENCES deployments
  , adapters_id int NOT NULL REFERENCES adapters
  , added_on timestamp with time zone DEFAULT now()
  , PRIMARY KEY (deployments_id, adapters_id)
);


  -- instead of adding it here
--ALTER TABLE providers
--ADD COLUMN adapters_id int NOT NULL DEFAULT 1
--, ADD COLUMN is_active boolean DEFAULT 't'





-- -- If provider is provided than we use that and ignore the active flag
-- -- otherwise check for adapter and also use the active flag
-- -- otherwise just assume all active providers
-- CREATE OR REPLACE FUNCTION deployment_sources(pid int, aid int) RETURNS jsonb AS $$
-- SELECT --jsonb_build_object('count', COUNT(1))
-- json_agg(metadata)
-- FROM public.providers p
-- JOIN deployments.adapters a ON (p.adapters_id = a.adapters_id)
-- WHERE (pid IS NULL AND aid IS NULL AND p.is_active)
-- OR (aid IS NULL AND p.providers_id = pid)
-- OR (pid IS NULL AND a.adapters_id = aid AND p.is_active);
-- $$ LANGUAGE SQL;


-- -- should be one
-- SELECT deployment_sources(:airnow, NULL);
-- -- should be about 29
-- SELECT deployment_sources(NULL, 340);
-- -- should be about 150
-- SELECT deployment_sources(NULL, NULL);



-- -- Now we can update the providers with that data
-- SELECT name
-- , temporal_offset as offset
-- , jsonb_array_length(deployment_sources(providers_id, adapters_id)) as sources_count
-- FROM deployments
-- WHERE is_active;


-- SELECT name
-- , temporal_offset as offset
-- , deployment_sources(providers_id, adapters_id)
-- FROM deployments
-- WHERE is_active
-- AND name ~* 'airnow'
-- ;


-- Function 1: Get deployments that are ready to run (read-only, no side effects)
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


-- Function 2: Queue the ready deployments to fetchlogs table
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
