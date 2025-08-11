
  -- create log/metrics schema

--CREATE SCHEMA IF NOT EXISTS logging;
--SET search_path = logging, public;

-- simple table to hold the raw logs until we process them
CREATE TABLE IF NOT EXISTS api_logs (
    api_key text
  , status_code int
  , endpoint text
  , agent text
  , params jsonb
  , added_on timestamptz DEFAULT now()
);

-- how many requests per node/day
CREATE TABLE IF NOT EXISTS daily_requests (
    day date NOT NULL UNIQUE
  , requests_count int NOT NULL
);

-- a table to list the clients we care about
-- also reduces the size of the log table
CREATE TABLE IF NOT EXISTS agents (
  agents_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , label text NOT NULL UNIQUE
  , key text NOT NULL UNIQUE
  , description text
);

-- where are the request comeing from each day
CREATE TABLE IF NOT EXISTS agent_daily_requests (
    agents_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , UNIQUE(agents_id, day)
);

-- how many measurement requests per sensor/day
CREATE TABLE IF NOT EXISTS sensor_measurements_daily_requests (
   sensors_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , UNIQUE(sensors_id, day)
);

-- how many requests per node/day
CREATE TABLE IF NOT EXISTS sensor_nodes_daily_requests (
   sensor_nodes_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , UNIQUE(sensor_nodes_id, day)
);

-- how many requests per group/day
CREATE TABLE IF NOT EXISTS groups_daily_requests (
   groups_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , UNIQUE(groups_id, day)
);

-- a table to list the clients we care about
-- also reduces the size of the log table
CREATE TABLE IF NOT EXISTS api_clients (
  api_clients_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , label text NOT NULL UNIQUE
  , key text NOT NULL UNIQUE
  , description text
);

CREATE TABLE IF NOT EXISTS client_daily_requests (
   api_clients_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , UNIQUE(api_clients_id, day)
);

-- run this vie pg_cron
CREATE OR REPLACE FUNCTION process_daily_logs(
    process_date date DEFAULT CURRENT_DATE - INTERVAL '1 day',
    delete_processed_logs boolean DEFAULT false
)
RETURNS void AS $$
DECLARE
    processed_count integer := 0;
BEGIN
    -- Insert/update agent daily requests
    INSERT INTO agent_daily_requests (agents_id, day, requests_count)
    SELECT
        a.agents_id
        , process_date
        , COUNT(1)
    FROM api_logs l
    JOIN agents a ON a.key = l.api_key
    WHERE l.added_on::date = process_date
    GROUP BY a.agents_id
    ON CONFLICT (agents_id, day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count;

    -- Insert/update client daily requests
    INSERT INTO client_daily_requests (api_clients_id, day, requests_count)
    SELECT
        c.api_clients_id
        , process_date
        , COUNT(1)
    FROM api_logs l
    JOIN api_clients c ON c.key = l.api_key
    WHERE l.added_on::date = process_date
    GROUP BY c.api_clients_id
    ON CONFLICT (api_clients_id, day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count;

    -- Insert/update sensor measurements daily requests
    -- Assumes params contains sensors_id
    INSERT INTO sensor_measurements_daily_requests (sensors_id, day, requests_count)
    SELECT
        (l.params->>'sensors_id')::int
        , process_date
        , COUNT(1)
    FROM api_logs l
    WHERE l.added_on::date = process_date
      AND l.params ? 'sensors_id'
      AND l.params->>'sensors_id' ~ '^\d+$'
    GROUP BY (l.params->>'sensors_id')::int
    ON CONFLICT (sensors_id, day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count;

    -- Insert/update sensor nodes daily requests
    -- Assumes params contains sensor_nodes_id
    INSERT INTO sensor_nodes_daily_requests (sensor_nodes_id, day, requests_count)
    SELECT
        (l.params->>'sensor_nodes_id')::int
        , process_date
        , COUNT(1)
    FROM api_logs l
    WHERE l.added_on::date = process_date
      AND l.params ? 'sensor_nodes_id'
      AND l.params->>'sensor_nodes_id' ~ '^\d+$'
    GROUP BY (l.params->>'sensor_nodes_id')::int
    ON CONFLICT (sensor_nodes_id, day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count;

    -- Insert/update groups daily requests
    -- Assumes params contains groups_id
    INSERT INTO groups_daily_requests (groups_id, day, requests_count)
    SELECT
        (l.params->>'groups_id')::int
        , process_date
        , COUNT(1)
    FROM api_logs l
    WHERE l.added_on::date = process_date
      AND l.params ? 'groups_id'
      AND l.params->>'groups_id' ~ '^\d+$'
    GROUP BY (l.params->>'groups_id')::int
    ON CONFLICT (groups_id, day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count;

    -- Get count of processed records for logging
    INSERT INTO daily_requests (day, requests_count)
    SELECT
          process_date
        , COUNT(1)
    FROM api_logs l
    WHERE l.added_on::date = process_date
    --GROUP BY 1
    ON CONFLICT (day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count
    RETURNING requests_count INTO processed_count;

    -- Optionally delete processed logs
    IF delete_processed_logs THEN
        DELETE FROM api_logs WHERE added_on::date = process_date;
        RAISE NOTICE 'Processed and deleted % log records for date %', processed_count, process_date;
    ELSE
        RAISE NOTICE 'Processed % log records for date % (logs retained)', processed_count, process_date;
    END IF;

END;
$$ LANGUAGE plpgsql;
