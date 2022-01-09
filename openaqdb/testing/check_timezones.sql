\set origin '''dev-station'''

\i testing_schema.sql

SELECT generate_fake_data(24, :origin, '1hours', '1month');

SELECT generate_canary_data(:origin, '1hours', '1month');

SELECT generate_version_data(2, :origin, '2hours', '1month');

CALL run_updates_full();

SELECT *
FROM testing.canary_rollup_days
WHERE subtitle ~* 'tz:0';

SELECT *
FROM testing.canary_days_compare
WHERE site_name ~* 'tz:-8'
AND value = 5;

SELECT *
FROM testing.canary_days_utc
WHERE site_name ~* 'tz:5'
LIMIT 10;

SELECT *
FROM testing.canary_days_local
WHERE site_name ~* 'tz:5'
LIMIT 10;

-- SELECT remove_testing_data();
