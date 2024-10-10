CREATE TABLE IF NOT EXISTS performance_log (
  process_name text
, start_datetime timestamptz
, end_datetime timestamptz DEFAULT current_timestamp
);

CREATE OR REPLACE VIEW performance_log_view AS
SELECT process_name
, start_datetime
, end_datetime
, age(end_datetime, start_datetime) as process_time
FROM performance_log;

CREATE OR REPLACE FUNCTION log_performance(text, timestamptz) RETURNS timestamptz AS $$
  INSERT INTO performance_log (process_name, start_datetime, end_datetime)
  VALUES (pg_backend_pid()||'-'||$1, $2, current_timestamp)
  RETURNING end_datetime;
$$ LANGUAGE SQL;
