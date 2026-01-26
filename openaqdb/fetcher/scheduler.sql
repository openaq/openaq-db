CREATE SCHEMA IF NOT EXISTS fetcher;
SET search_path = fetcher, public;

-- Helper function to validate individual cron fields
CREATE OR REPLACE FUNCTION cron_validate_part(field_value TEXT, min_val INTEGER, max_val INTEGER)
RETURNS BOOLEAN AS $$
DECLARE
    parts TEXT[];
    part TEXT;
    range_parts TEXT[];
    step_parts TEXT[];
    num INTEGER;
BEGIN
    -- Handle asterisk
    IF field_value = '*' THEN
        RETURN TRUE;
    END IF;

    -- Handle step values (*/n or number/n)
    IF position('/' in field_value) > 0 THEN
        step_parts := string_to_array(field_value, '/');
        IF array_length(step_parts, 1) != 2 THEN
            RETURN FALSE;
        END IF;

        -- Validate step number
        BEGIN
            num := step_parts[2]::INTEGER;
            IF num <= 0 OR num > max_val THEN
                RETURN FALSE;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RETURN FALSE;
        END;

        -- If first part is *, it's valid
        IF step_parts[1] = '*' THEN
            RETURN TRUE;
        END IF;

        -- Otherwise validate the base number
        BEGIN
            num := step_parts[1]::INTEGER;
            RETURN num >= min_val AND num <= max_val;
        EXCEPTION WHEN OTHERS THEN
            RETURN FALSE;
        END;
    END IF;

    -- Handle comma-separated values
    IF position(',' in field_value) > 0 THEN
        parts := string_to_array(field_value, ',');
        FOREACH part IN ARRAY parts
        LOOP
            IF NOT cron_validate_part(part, min_val, max_val) THEN
                RETURN FALSE;
            END IF;
        END LOOP;
        RETURN TRUE;
    END IF;

    -- Handle ranges (n-m)
    IF position('-' in field_value) > 0 THEN
        range_parts := string_to_array(field_value, '-');
        IF array_length(range_parts, 1) != 2 THEN
            RETURN FALSE;
        END IF;

        BEGIN
            IF range_parts[1]::INTEGER < min_val OR
               range_parts[1]::INTEGER > max_val OR
               range_parts[2]::INTEGER < min_val OR
               range_parts[2]::INTEGER > max_val OR
               range_parts[1]::INTEGER > range_parts[2]::INTEGER THEN
                RETURN FALSE;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RETURN FALSE;
        END;

        RETURN TRUE;
    END IF;

    -- Handle single number
    BEGIN
        num := field_value::INTEGER;
        RETURN num >= min_val AND num <= max_val;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION cron_validate_expr(cron_expr TEXT) RETURNS BOOLEAN AS $$
DECLARE
    fields TEXT[];
    minute_field TEXT;
    hour_field TEXT;
    day_field TEXT;
    month_field TEXT;
    weekday_field TEXT;
BEGIN
    -- Remove extra whitespace and split into fields
    cron_expr := trim(regexp_replace(cron_expr, '\s+', ' ', 'g'));
    fields := string_to_array(cron_expr, ' ');

    -- Must have exactly 5 fields
    IF array_length(fields, 1) != 5 THEN
        RETURN FALSE;
    END IF;

    minute_field := fields[1];
    hour_field := fields[2];
    day_field := fields[3];
    month_field := fields[4];
    weekday_field := fields[5];

    -- Validate minute field (0-59)
    IF NOT cron_validate_part(minute_field, 0, 59) THEN
        RETURN FALSE;
    END IF;

    -- Validate hour field (0-23)
    IF NOT cron_validate_part(hour_field, 0, 23) THEN
        RETURN FALSE;
    END IF;

    -- Validate day field (1-31)
    IF NOT cron_validate_part(day_field, 1, 31) THEN
        RETURN FALSE;
    END IF;

    -- Validate month field (1-12)
    IF NOT cron_validate_part(month_field, 1, 12) THEN
        RETURN FALSE;
    END IF;

    -- Validate weekday field (0-6)
    IF NOT cron_validate_part(weekday_field, 0, 6) THEN
        RETURN FALSE;
    END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION cron_evaluate_part(
    field_expr TEXT,
    current_value INTEGER,
    min_val INTEGER,
    max_val INTEGER
) RETURNS BOOLEAN AS $$
DECLARE
    parts TEXT[];
    part TEXT;
    range_parts TEXT[];
    step_parts TEXT[];
    range_start INTEGER;
    range_end INTEGER;
    step_val INTEGER;
    i INTEGER;
BEGIN
    -- Handle comma-separated values
    parts := string_to_array(field_expr, ',');

    FOREACH part IN ARRAY parts LOOP
        part := trim(part);

        -- Handle step values (e.g., */5, 0-30/5)
        IF position('/' in part) > 0 THEN
            step_parts := string_to_array(part, '/');
            step_val := step_parts[2]::INTEGER;
            part := step_parts[1];

            -- Check step
            IF part = '*' THEN
                IF (current_value - min_val) % step_val = 0 THEN
                    RETURN TRUE;
                END IF;
            ELSE
                -- Range with step
                range_parts := string_to_array(part, '-');
                range_start := range_parts[1]::INTEGER;
                range_end := COALESCE(range_parts[2]::INTEGER, range_start);

                IF current_value >= range_start AND current_value <= range_end THEN
                    IF (current_value - range_start) % step_val = 0 THEN
                        RETURN TRUE;
                    END IF;
                END IF;
            END IF;

        -- Handle ranges (e.g., 1-5)
        ELSIF position('-' in part) > 0 THEN
            range_parts := string_to_array(part, '-');
            range_start := range_parts[1]::INTEGER;
            range_end := range_parts[2]::INTEGER;

            IF current_value >= range_start AND current_value <= range_end THEN
                RETURN TRUE;
            END IF;

        -- Handle single values
        ELSE
            IF current_value = part::INTEGER THEN
                RETURN TRUE;
            END IF;
        END IF;
    END LOOP;

    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION is_cron_ready(
    cron_expr TEXT,
    check_time TIMESTAMPTZ DEFAULT NOW()
) RETURNS BOOLEAN AS $$
DECLARE
    parts TEXT[];
    minute_part TEXT;
    hour_part TEXT;
    day_part TEXT;
    month_part TEXT;
    dow_part TEXT;
    check_minute INTEGER;
    check_hour INTEGER;
    check_day INTEGER;
    check_month INTEGER;
    check_dow INTEGER;
BEGIN
    -- Parse cron expression (minute hour day month dow)
    parts := string_to_array(trim(cron_expr), ' ');

    IF array_length(parts, 1) != 5 THEN
        RAISE EXCEPTION 'Invalid cron expression: %. Expected format: "minute hour day month dow"', cron_expr;
    END IF;

    minute_part := parts[1];
    hour_part := parts[2];
    day_part := parts[3];
    month_part := parts[4];
    dow_part := parts[5];

    -- Extract time components from check_time
    check_minute := EXTRACT(MINUTE FROM check_time)::INTEGER;
    check_hour := EXTRACT(HOUR FROM check_time)::INTEGER;
    check_day := EXTRACT(DAY FROM check_time)::INTEGER;
    check_month := EXTRACT(MONTH FROM check_time)::INTEGER;
    check_dow := EXTRACT(DOW FROM check_time)::INTEGER; -- 0 = Sunday

    -- Check minute
    IF minute_part != '*' THEN
        IF NOT cron_evaluate_part(minute_part, check_minute, 0, 59) THEN
            RETURN FALSE;
        END IF;
    END IF;

    -- Check hour
    IF hour_part != '*' THEN
        IF NOT cron_evaluate_part(hour_part, check_hour, 0, 23) THEN
            RETURN FALSE;
        END IF;
    END IF;

    -- Check month
    IF month_part != '*' THEN
        IF NOT cron_evaluate_part(month_part, check_month, 1, 12) THEN
            RETURN FALSE;
        END IF;
    END IF;

    -- Check day of month OR day of week (cron OR logic)
    IF day_part != '*' OR dow_part != '*' THEN
        IF day_part != '*' AND cron_evaluate_part(day_part, check_day, 1, 31) THEN
            -- Day of month matches
            RETURN TRUE;
        ELSIF dow_part != '*' AND cron_evaluate_part(dow_part, check_dow, 0, 6) THEN
            -- Day of week matches
            RETURN TRUE;
        ELSIF day_part = '*' AND dow_part != '*' THEN
            -- Only dow specified
            RETURN cron_evaluate_part(dow_part, check_dow, 0, 6);
        ELSIF day_part != '*' AND dow_part = '*' THEN
            -- Only day specified
            RETURN cron_evaluate_part(day_part, check_day, 1, 31);
        ELSE
            -- Neither matches
            RETURN FALSE;
        END IF;
    END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

  -- a simple method for checking a potential cron expression
  CREATE OR REPLACE FUNCTION check_scheduler(ce text, sd timestamptz DEFAULT now(), fr interval DEFAULT '1day') RETURNS jsonb AS $$
    SELECT jsonb_build_object(
      'runs', COUNT(1)
    , 'runtimes', array_agg(tm::timestamp(0))
    )
    FROM generate_series(sd, sd + fr, '1min'::interval) as tm
    WHERE is_cron_ready(ce, tm);
  $$ LANGUAGE SQL;



-- a set of tests that we can run if/when we update the method
CREATE OR REPLACE FUNCTION run_scheduler_tests() RETURNS boolean AS $$
DECLARE r bool;
BEGIN
  -- runs every minute
   -- Every minute: TRUE
  SET search_path = fetcher;
  IF ((NOT is_cron_ready('* * * * *')) OR (SELECT (
  SELECT SUM(is_cron_ready('* * * * *', tm)::int)
  FROM generate_series(
      '2025-01-01 00:00:00'::timestamp
    , '2025-01-02 00:00:00'::timestamp
    , '1min'::interval
  ) as tm) != 1441)) THEN
  RAISE EXCEPTION 'does not run every minute';
  END IF;

    -- runs every hour
  IF (SELECT (
  SELECT SUM(is_cron_ready('0 * * * *', tm)::int)
  FROM generate_series(
      '2025-01-01 00:00:00'::timestamp
    , '2025-01-02 00:00:00'::timestamp
    , '1min'::interval
  ) as tm) != 25) THEN
  RAISE EXCEPTION 'does not run every hour';
  END IF;


  IF (NOT is_cron_ready('0 9 * * *', '2025-01-01 09:00:00')) THEN
    RAISE EXCEPTION 'does not run every day at 9am';
  END IF ;  -- Every day at 9 AM: depends on current time

  -- Every day at 9 AM: runs exactly 3 times in 3 days
  IF (SELECT (
  SELECT SUM(is_cron_ready('0 9 * * *', tm)::int)
  FROM generate_series(
      '2025-01-01 00:00:00'::timestamp
    , '2025-01-04 00:00:00'::timestamp
    , '1min'::interval
  ) as tm) != 3) THEN
    RAISE EXCEPTION 'does not run exactly 3 times in 3 days for 0 9 * * *';
  END IF;

  -- Every 5 minutes: test specific time
  IF (NOT is_cron_ready('*/5 * * * *', '2025-01-01 09:00:00')) THEN
    RAISE EXCEPTION 'does not run at expected time for */5 * * * *';
  END IF;

  -- Every 5 minutes: runs exactly 13 times in 1 hour
  IF (SELECT (
  SELECT SUM(is_cron_ready('*/5 * * * *', tm)::int)
  FROM generate_series(
      '2025-01-01 00:00:00'::timestamp
    , '2025-01-01 01:00:00'::timestamp
    , '1min'::interval
  ) as tm) != 13) THEN
    RAISE EXCEPTION 'does not run exactly 13 times in 1 hour for */5 * * * *';
  END IF;

  -- Every Sunday at midnight: test specific time
  IF (NOT is_cron_ready('0 0 * * 0', '2025-12-07 00:00:00')) THEN
    RAISE EXCEPTION 'does not run at expected time for 0 0 * * 0';
  END IF;

  -- Every Sunday at midnight: runs exactly 2 times in ~7 days
  IF (SELECT (
  SELECT SUM(is_cron_ready('0 0 * * 0', tm)::int)
  FROM generate_series(
      '2025-12-07 00:00:00'::timestamp
    , '2025-12-14 02:0:00'::timestamp
    , '1min'::interval
  ) as tm) != 2) THEN
    RAISE EXCEPTION 'does not run exactly 2 times in ~7 days for 0 0 * * 0';
  END IF;

  -- First day of month at 9 AM: test specific time
  IF (NOT is_cron_ready('0 9 1 * *', '2025-12-01 09:00:00')) THEN
    RAISE EXCEPTION 'does not run at expected time for 0 9 1 * *';
  END IF;

  -- First day of month at 9 AM: runs exactly 12 times in 1 year
  IF (SELECT (
  SELECT SUM(is_cron_ready('0 9 1 * *', tm)::int)
  FROM generate_series(
      '2025-01-01 00:00:00'::timestamp
    , '2026-01-01 00:0:00'::timestamp
    , '1hour'::interval
  ) as tm) != 12) THEN
    RAISE EXCEPTION 'does not run exactly 12 times in 1 year for 0 9 1 * *';
  END IF;

  -- Weekdays (Mon-Fri) at midnight: test specific time
  IF (NOT is_cron_ready('0 0 * * 1-5', '2025-12-08 00:00:00')) THEN
    RAISE EXCEPTION 'does not run at expected time for 0 0 * * 1-5';
  END IF;

  -- Weekdays (Mon-Fri) at midnight: runs exactly 15 times from Mon to Sat
  IF (SELECT (
  SELECT SUM(is_cron_ready('0 0 * * 1-5', tm)::int)
  FROM generate_series(
      '2025-12-01 00:00:00'::timestamp
    , '2025-12-20 00:0:00'::timestamp
    , '1hour'::interval
  ) as tm) != 15) THEN
    RAISE EXCEPTION 'does not run exactly 15 times from Mon to Sat for 0 0 * * 1-5';
  END IF;
  RETURN TRUE;
 END;
  $$ LANGUAGE plpgsql;


  SELECT cron_validate_expr('*/61 * * * *');
  SELECT cron_validate_expr('* */24 * * *');
  SELECT cron_validate_expr('* * */32 * *');
  SELECT cron_validate_expr('* * * */13 *');
  SELECT cron_validate_expr('* * * * */8');


  SELECT cron_validate_expr('* * * * *');
  SELECT cron_validate_expr('*/5 * * * *');
  SELECT cron_validate_expr('0 1,2 * * 2-4,6');

  -- run it right away
  SELECT run_scheduler_tests();

  -- should run about 8 times in a week
  SELECT check_scheduler('0 1,2 * * 2-4,6', current_date, '1week');

  -- every monday @ 1:15 am
  SELECT check_scheduler('5 1 * * 1', '2025-12-01'::timestamptz, '1month');

  -- every 10min for the first 2 hours of every day
  SELECT check_scheduler('*/10 0,1 * * *', current_date, '1week');


SET search_path = public;
