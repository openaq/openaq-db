
  -- create log/metrics schema

--CREATE SCHEMA IF NOT EXISTS logging;
--SET search_path = logs, public;

-- simple table to hold the raw logs until we process them
CREATE TABLE IF NOT EXISTS api_logs (
    api_key text
  , status_code int
  , endpoint text
  , agent text
  , params jsonb
  , timing double precision
  , counter int
  , rate_limiter text
  , ip_address text
  , added_on timestamptz DEFAULT now()
);

-- a table to list the agents we care about
-- also reduces the size of the log table
CREATE TABLE IF NOT EXISTS agents (
    agents_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , label text NOT NULL UNIQUE
  , pattern text NOT NULL UNIQUE
  , description text
);


-- how many requests per node/day
CREATE TABLE IF NOT EXISTS daily_requests (
    day date NOT NULL UNIQUE
  , requests_count int NOT NULL
  , requests_time int NOT NULL
);


-- where are the request comeing from each day
CREATE TABLE IF NOT EXISTS agent_daily_requests (
    agents_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , requests_time int NOT NULL
  , UNIQUE(agents_id, day)
);

-- how many measurement requests per sensor/day
CREATE TABLE IF NOT EXISTS sensor_measurements_daily_requests (
   sensors_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , requests_time int NOT NULL
  , UNIQUE(sensors_id, day)
);

-- how many requests per node/day
CREATE TABLE IF NOT EXISTS sensor_nodes_daily_requests (
   sensor_nodes_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , requests_time int NOT NULL
  , UNIQUE(sensor_nodes_id, day)
);

-- how many requests per group/day
CREATE TABLE IF NOT EXISTS groups_daily_requests (
   groups_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , requests_time int NOT NULL
  , UNIQUE(groups_id, day)
);

-- how many requests per group/day
CREATE TABLE IF NOT EXISTS user_daily_requests (
   users_id int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , requests_time int NOT NULL
  , UNIQUE(users_id, day)
);

CREATE TABLE IF NOT EXISTS status_code_daily_requests (
    status_code int NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , requests_time int NOT NULL
  , UNIQUE(status_code, day)
);

CREATE TABLE IF NOT EXISTS resource_daily_requests (
    resource text NOT NULL
  , day date NOT NULL
  , requests_count int NOT NULL
  , requests_time int NOT NULL
  , UNIQUE(resource, day)
);


CREATE VIEW uncategorized_requests AS
SELECT l.agent
  , COUNT(1) as n
  , ROUND(SUM(timing)) as time
  , MIN(added_on) as first_requested
  , MAX(added_on) as last_requested
  , COUNT(DISTINCT api_key) as keys
FROM api_logs l
LEFT JOIN LATERAL (SELECT agents_id FROM agents a WHERE l.agent ~ a.pattern LIMIT 1) rp ON TRUE
WHERE rp.agents_id IS NULL
GROUP BY 1;


-- run this vie pg_cron
CREATE OR REPLACE FUNCTION process_daily_logs(
    process_date date DEFAULT CURRENT_DATE - INTERVAL '1 day',
    delete_processed_logs boolean DEFAULT false
)
RETURNS int AS $$
DECLARE
    processed_count integer := 0;
BEGIN
    -- Insert/update agent daily requests
    INSERT INTO agent_daily_requests (agents_id, requests_count, requests_time, day)
    SELECT rp.agents_id, COUNT(1), ROUND(SUM(timing)), process_date
    FROM api_logs l
    JOIN LATERAL (SELECT agents_id FROM agents a WHERE l.agent ~ a.pattern LIMIT 1) rp ON TRUE
    WHERE l.added_on::date = process_date
    GROUP BY 1
    ON CONFLICT (agents_id, day)
    DO UPDATE SET
    requests_count = EXCLUDED.requests_count
    , requests_time = EXCLUDED.requests_time;

  -- http codes
    INSERT INTO status_code_daily_requests (status_code, requests_count, requests_time, day)
    SELECT status_code, COUNT(1), ROUND(SUM(timing)), process_date
    FROM api_logs l
    WHERE l.added_on::date = process_date
    GROUP BY status_code
    ON CONFLICT (status_code, day)
    DO UPDATE SET
    requests_count = EXCLUDED.requests_count
    , requests_time = EXCLUDED.requests_time;


    INSERT INTO resource_daily_requests (resource, requests_count, requests_time, day)
    SELECT endpoint, COUNT(1), ROUND(SUM(timing)), process_date
    FROM api_logs l
    WHERE l.added_on::date = process_date
    GROUP BY endpoint
    ON CONFLICT (resource, day)
    DO UPDATE SET
    requests_count = EXCLUDED.requests_count
    , requests_time = EXCLUDED.requests_time;

  -- use the api keys to match to user
  WITH grouped_data AS (
    SELECT api_key, COUNT(1) as n, ROUND(SUM(timing)) as t
    FROM api_logs l
    WHERE l.added_on::date = current_date - 1
    GROUP BY 1
  )
  INSERT INTO user_daily_requests (users_id, requests_count, requests_time, day)
  SELECT u.users_id, n, t, current_date - 1
  FROM grouped_data d
  JOIN user_keys u ON (d.api_key = u.token)
    ON CONFLICT (users_id, day)
    DO UPDATE SET
    requests_count = EXCLUDED.requests_count
    , requests_time = EXCLUDED.requests_time;

    -- Insert/update sensor measurements daily requests
    -- Assumes params contains sensors_id
    INSERT INTO sensor_measurements_daily_requests (sensors_id, day, requests_count, requests_time)
    SELECT
        (l.params->>'sensors_id')::int
        , process_date
        , COUNT(1)
        , ROUND(SUM(timing))
    FROM api_logs l
    WHERE l.added_on::date = process_date
      AND l.params ? 'sensors_id'
      AND l.params->>'sensors_id' ~ '^\d+$'
    GROUP BY (l.params->>'sensors_id')::int
    ON CONFLICT (sensors_id, day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count
    , requests_time = EXCLUDED.requests_time;

    -- Insert/update sensor nodes daily requests
    -- Assumes params contains sensor_nodes_id
    INSERT INTO sensor_nodes_daily_requests (sensor_nodes_id, day, requests_count, requests_time)
    SELECT
         COALESCE((l.params->>'locations_id')::int, sy.sensor_nodes_id)
        , process_date
        , COUNT(1)
        , ROUND(SUM(timing))
    FROM api_logs l
    JOIN sensors s ON (s.sensors_id = (l.params->>'sensors_id')::int)
    JOIN sensor_systems sy ON (sy.sensor_systems_id = s.sensor_systems_id)
    WHERE l.added_on::date = process_date
        AND ((l.params ? 'locations_id' AND l.params->>'locations_id' ~ '^\d+$')
        OR (l.params ? 'sensors_id' AND l.params->>'sensors_id' ~ '^\d+$'))
      GROUP BY 1
    ON CONFLICT (sensor_nodes_id, day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count
    , requests_time = EXCLUDED.requests_time;

    -- Insert/update groups daily requests
    -- Assumes params contains groups_id
    -- INSERT INTO groups_daily_requests (groups_id, day, requests_count, requests_time)
    -- SELECT
    --     (l.params->>'groups_id')::int
    --     , process_date
    --     , COUNT(1)
    --     , ROUND(SUM(timing))
    -- FROM api_logs l
    -- WHERE l.added_on::date = process_date
    --   AND l.params ? 'groups_id'
    --   AND l.params->>'groups_id' ~ '^\d+$'
    -- GROUP BY (l.params->>'groups_id')::int
    -- ON CONFLICT (groups_id, day)
    -- DO UPDATE SET requests_count = EXCLUDED.requests_count
    -- , requests_time = EXCLUDED.requests_time;

    -- Get count of processed records for logging
    INSERT INTO daily_requests (day, requests_count, requests_time)
    SELECT
          process_date
        , COUNT(1)
        , ROUND(SUM(timing))
    FROM api_logs l
    WHERE l.added_on::date = process_date
    --GROUP BY 1
    ON CONFLICT (day)
    DO UPDATE SET requests_count = EXCLUDED.requests_count
    , requests_time = EXCLUDED.requests_time
    RETURNING requests_count INTO processed_count;

    -- Optionally delete processed logs
    IF delete_processed_logs THEN
        DELETE FROM api_logs WHERE added_on::date = process_date;
        RAISE NOTICE 'Processed and deleted % log records for date %', processed_count, process_date;
    ELSE
        RAISE NOTICE 'Processed % log records for date % (logs retained)', processed_count, process_date;
    END IF;
  RETURN processed_count;
END;
$$ LANGUAGE plpgsql SET search_path = logs, public;


CREATE OR REPLACE FUNCTION clear_daily_requests(
    clear_date date DEFAULT current_date+1
)
RETURNS void AS $$
BEGIN
    -- Clear daily_requests
    DELETE FROM daily_requests WHERE day < clear_date;
    -- Clear agent_daily_requests
    DELETE FROM agent_daily_requests WHERE day < clear_date;
    -- Clear client_daily_requests
    DELETE FROM client_daily_requests WHERE day < clear_date;
    -- Clear sensor_measurements_daily_requests
    DELETE FROM sensor_measurements_daily_requests WHERE day < clear_date;
    -- Clear sensor_nodes_daily_requests
    DELETE FROM sensor_nodes_daily_requests WHERE day < clear_date;
    -- Clear groups_daily_requests
    DELETE FROM groups_daily_requests WHERE day < clear_date;
    DELETE FROM status_code_daily_requests WHERE day < clear_date;
    DELETE FROM resource_daily_requests WHERE day < clear_date;
    DELETE FROM user_daily_requests WHERE day < clear_date;
END;
$$ LANGUAGE plpgsql;
