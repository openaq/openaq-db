-- views
  -- groups_view_pre
  -- groups_sources_classify
  -- groups_view
DROP MATERIALIZED VIEW IF EXISTS groups_view;
DROP VIEW IF EXISTS groups_view_pre;
DROP MATERIALIZED VIEW IF EXISTS groups_sources_classify;

DROP TABLE IF EXISTS groups_sensors;
DROP TABLE IF EXISTS groups;
-- functions
  -- nodes_from_group
  -- nodes_from_project (2)
DROP FUNCTION IF EXISTS public.node_from_group(int);
DROP FUNCTION IF EXISTS public.nodes_from_project(int);
DROP FUNCTION IF EXISTS public.nodes_from_project(text);
DROP FUNCTION IF EXISTS public.project_in_nodes(int[], int[]);


-- update_rollups
-- Drop functions
DROP FUNCTION IF EXISTS update_groups();
DROP FUNCTION IF EXISTS rollups_daily(timestamptz); --- yes
DROP FUNCTION IF EXISTS rollups_daily_full();
DROP FUNCTION IF EXISTS rollups_monthly(timestamptz);
DROP FUNCTION IF EXISTS rollups_yearly(timestamptz);
DROP FUNCTION IF EXISTS rollups_total();
DROP PROCEDURE IF EXISTS run_updates(int, jsonb);
DROP PROCEDURE IF EXISTS run_updates_full();

-- Drop materialized views
DROP MATERIALIZED VIEW IF EXISTS sensors_first_last;
DROP MATERIALIZED VIEW IF EXISTS sensor_nodes_json;
DROP MATERIALIZED VIEW IF EXISTS groups_view;
DROP MATERIALIZED VIEW IF EXISTS sensor_stats;
DROP MATERIALIZED VIEW IF EXISTS city_stats;
DROP MATERIALIZED VIEW IF EXISTS country_stats;
DROP MATERIALIZED VIEW IF EXISTS locations_base_v2;
DROP MATERIALIZED VIEW IF EXISTS locations;
DROP MATERIALIZED VIEW IF EXISTS measurements_fastapi_base;
