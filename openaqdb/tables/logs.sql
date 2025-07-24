CREATE TABLE IF NOT EXISTS api_logs (
    api_key text
  , status_code int
  , endpoint text
  , params jsonb
  , added_on timestamptz DEFAULT now()
);
