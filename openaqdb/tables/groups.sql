DROP TABLE IF EXISTS groups CASCADE;
DROP TABLE IF EXISTS groups_sensors CASCADE;
-- functions
  -- nodes_from_group
  -- nodes_from_project (2)
DROP FUNCTION IF EXISTS public.node_from_group(int);
DROP FUNCTION IF EXISTS public.nodes_from_project(int);
DROP FUNCTION IF EXISTS public.nodes_from_project(text);
DROP FUNCTION IF EXISTS public.project_in_nodes(int[], int[]);

-- views
  -- groups_view_pre
  -- groups_sources_classify
  -- groups_view
DROP VIEW IF EXISTS groups_view_pre CASCADE;
DROP MATERIALIZED VIEW IF EXISTS groups_sources_classify CASCADE;
DROP MATERIALIZED VIEW IF EXISTS groups_view CASCADE;

-- update_rollups
-- Drop functions
DROP FUNCTION IF EXISTS update_groups();
DROP FUNCTION IF EXISTS rollups_daily(timestamptz);
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


 CREATE TABLE IF NOT EXISTS groups (
    groups_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    , label text NOT NULL
    , description text -- optional
    , group_path ltree -- trigger will make sure its not null
    , users_id int NOT NULL REFERENCES users ON DELETE CASCADE  -- users_id represents the list owner
    , visibility public.visibility_type NOT NULL DEFAULT 'private'
    , added_on timestamptz NOT NULL DEFAULT clock_timestamp()
    , modified_on timestamptz
    , UNIQUE (label)
);

  -- a trigger to make sure that we get something in the path
CREATE OR REPLACE FUNCTION check_group_path() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.group_path IS NOT NULL THEN
    IF index(NEW.group_path, NEW.groups_id::text::ltree, 0)<0 THEN
      NEW.group_path = NEW.group_path||NEW.groups_id::text::ltree;
    END IF;
  ELSE
      NEW.group_path = NEW.groups_id::text::ltree;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER group_path_tgr
BEFORE INSERT OR UPDATE ON groups
FOR EACH ROW EXECUTE PROCEDURE check_group_path();

-- A standard set of rules that we are able to string together (AND)
  -- to create a set of location ides for a specific group
CREATE TABLE IF NOT EXISTS rules (
  rules_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10)
  , label text NOT NULL UNIQUE
  , description text
  -- a statement like `providers_id = :providers_id
  -- or `locations_by_country(:countries_ids)`
  , where_statement text NOT NULL UNIQUE
  , params_config jsonb NOT NULL -- a variable name and the type for checking
);

  -- A set of rules to apply to one group
 CREATE TABLE IF NOT EXISTS group_rules (
  groups_id int NOT NULL REFERENCES groups ON DELETE CASCADE
  , rules_id int NOT NULL REFERENCES rules -- dont allow rules that are used to be deleted
  , args jsonb NOT NULL -- the values to be used in the rules
  , UNIQUE(groups_id, rules_id)
);


CREATE TABLE IF NOT EXISTS sensor_nodes_groups (
      groups_id int REFERENCES groups (groups_id) on delete cascade on update cascade
    , sensor_nodes_id int REFERENCES sensor_nodes (sensor_nodes_id) on delete cascade on update cascade
    , added_on timestamptz DEFAULT clock_timestamp()
    , PRIMARY KEY (groups_id, sensor_nodes_id)
);


  -- rules
  INSERT INTO rules (rules_id, label, where_statement, params_config)
  OVERRIDING SYSTEM VALUE
  VALUES
    (1, 'Countries', 'countries_id=ANY(:countries_id)', jsonb_build_object('countries_id', 'int[]'))
  , (2, 'Providers', 'providers_id=ANY(:providers_id)', jsonb_build_object('providers_id', 'int[]'))
  , (3, 'Owners', 'users_id=ANY(:users_id)', jsonb_build_object('_id', 'int[]'))
  , (4, 'Locations', 'sensor_nodes_id=ANY(:sensor_nodes_id)', jsonb_build_object('sensor_nodes_id', 'int[]'))
  , (5, 'Geom', 'geom @> :geom', jsonb_build_object('geom', 'geometry'));


-- some test examples
  INSERT INTO groups (groups_id, label, users_id)
  OVERRIDING SYSTEM VALUE
  VALUES
  (1, 'testing', 1);

  INSERT INTO group_rules (groups_id, rules_id, args)
  VALUES
  (1, 1, jsonb_build_object('countries_id', '{1}'));


-- CREATE OR REPLACE FUNCTION get_group_sensor_nodes2(gid int) RETURNS text AS $$ --RETURNS SETOF int AS $$
-- DECLARE
--     qry text;
-- BEGIN
--     -- set the query up
--     SELECT format('SELECT id FROM locations_view_cached WHERE %s'
--     , array_to_string(array_agg(where_statement), ' AND '))
--     INTO qry
--     FROM group_rules g
--     JOIN rules r USING (rules_id)
--     WHERE groups_id = gid;
--     -- Prepare and execute via using
--     RETURN QUERY EXECUTE query USING (
--       SELECT jsonb_each_text((SELECT args FROM group_rules WHERE groups_id = gid))
--     );
-- END;
-- $$ LANGUAGE plpgsql;
