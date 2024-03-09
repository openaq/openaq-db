-- these tables are storing data for the following purposes
-- Adding a gate keeper to api usage
-- Allowing users to have more more than one key if needed
-- Storing information about the api users
-- we will be using ltree for the entity relationships
CREATE EXTENSION IF NOT EXISTS ltree;

-- A user is someone that uses the api and/or
-- will log into the platform
CREATE SEQUENCE IF NOT EXISTS users_sq START 10;
CREATE TABLE IF NOT EXISTS users (
  users_id int PRIMARY KEY DEFAULT nextval('users_sq')
  , email_address text NOT NULL UNIQUE
  -- these are included if we are going to allow logins
  , password_hash text
  , password_salt text
  -- verification code is used to verify the email address
  -- could also be used instead of a password, just force
  -- reverification for each new key or edit
  , verification_code text
  , added_on timestamptz DEFAULT now()
  , modified_on timestamptz
  , verified_on timestamptz
  , expires_on timestamptz
  -- we cant delete users so we must inactivate them
  , is_active boolean NOT NULL DEFAULT 't'::boolean
  , ip_address cidr NOT NULL
);

-- An optional key type table to hold some defaults
-- For example, we could have a public good type which
-- provides a higher rate vs a commercial key
-- but its hard to come up with a scenario where this needed
-- CREATE SEQUENCE IF NOT EXISTS key_types_sq START 10;
-- CREATE TABLE IF NOT EXISTS key_types (
--   key_types_id int PRIMARY KEY DEFAULT nextval('key_types_sq')
--   , label text NOT NULL UNIQUE
--   , description text
--   , uses_per_minute int NOT NULL DEFAULT 60
--   ...
-- );

-- A list of keys that a user owns
-- these records are not meant to be edited
-- if we want to save keys forever than we may
-- want to add an is_active field, though its likely
-- just better to remove them
CREATE SEQUENCE IF NOT EXISTS user_keys_sq START 10;
CREATE TABLE IF NOT EXISTS user_keys (
  user_keys_id int PRIMARY KEY DEFAULT nextval('user_keys_sq')
  , users_id int NOT NULL REFERENCES users
  -- uncomment if using the key_types method
  --, key_types_id int NOT NULL REFERENCES key_types
  , label varchar(100) NOT NULL
  , token text NOT NULL UNIQUE
  -- if we want to allow a key by key rate
  -- another option would be to have a key type and
  -- set the rate there. But we could still keep this
  -- as an override, it was just need to be nullable
  , uses_per_minute int NOT NULL DEFAULT 60
  -- attributes can store things like refresh tokes (if used)
  , attributes jsonb
  , added_on timestamptz DEFAULT now()
  , expires_on timestamptz
  , last_used_on timestamptz
  , UNIQUE(users_id, label)
);


-- If we are going to be using a reference to a person/organization
-- anywhere else in the platform we will likely want to separate users
-- from entities.
-- For example, if we want to include entities in the metadata for instruments
-- (e.g. owner, entity person)
-- And if we are expecting that either a person OR an organization can be
-- the owner/entity person we are likely going to want to use the following
-- pattern where we have a entities list and each entity is of a type
--

-- One way to create the type is via an enumerated type
CREATE TYPE entity_type AS ENUM (
  'Person'
, 'Organization'
, 'Governmental Organization'
, 'Non-Governmental Organization'
, 'Research Organization'
, 'Community Organization'
, 'Private Organization'
);

-- If we want to add other information to the types,
-- e.g. is_multiple to say that a type is group type
-- we would either hard code that in or we could create
-- a lookup table instead of the enum

-- A entity is typically a person that we want to collect
-- information on and reference in the system somewhere
CREATE SEQUENCE IF NOT EXISTS entities_sq START 10;
CREATE TABLE IF NOT EXISTS entities (
  entities_id int PRIMARY KEY DEFAULT nextval('entities_sq')
  , entity_type entity_type NOT NULL
  , full_name text NOT NULL
  -- add any details that we want to track about a person
  -- some tracking tables that we may want to include
  , added_on timestamptz NOT NULL DEFAULT now()
  , added_by int NOT NULL REFERENCES users DEFAULT 1
  , modified_on timestamptz
  , modified_by int REFERENCES users
);


-- And then we link the entity to a specific user
CREATE SEQUENCE IF NOT EXISTS users_entities_sq START 10;
CREATE TABLE IF NOT EXISTS users_entities (
  users_entities_id int PRIMARY KEY DEFAULT nextval('users_entities_sq')
  , users_id int NOT NULL REFERENCES users ON DELETE CASCADE
  , entities_id int NOT NULL REFERENCES entities ON DELETE CASCADE
  , UNIQUE(users_id, entities_id)
);

-- There are a few ways that we could model the relationships
-- using a parent/child table and then recursive views
-- or by using the ltree extension (not installed)
-- https://www.postgresql.org/docs/current/ltree.html

-- Tree method
CREATE SEQUENCE IF NOT EXISTS entity_paths_sq START 10;
CREATE TABLE IF NOT EXISTS entity_paths (
  entity_paths_id int PRIMARY KEY DEFAULT nextval('entity_paths_sq')
  , entities_id int NOT NULL REFERENCES entities
  , entities_path ltree NOT NULL
  -- add other details here
);

-- add a trigger to make sure that things are done as needed
-- when adding a new relationship we need to make sure that
-- the current entities_id is included in the path
CREATE OR REPLACE FUNCTION check_entities_path() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.entities_path IS NOT NULL THEN
    IF index(NEW.entities_path, NEW.entities_id::text::ltree, 0)<0 THEN
      NEW.entities_path = NEW.entities_path||NEW.entities_id::text::ltree;
    END IF;
  ELSE
      NEW.entities_path = NEW.entities_id::text::ltree;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS entities_path_tgr ON entity_path;
CREATE TRIGGER entities_path_tgr
BEFORE INSERT OR UPDATE ON entity_paths
FOR EACH ROW EXECUTE PROCEDURE check_entities_path();

-- Grant access to the api user
GRANT SELECT ON public.users TO apiuser;
GRANT SELECT ON public.entities TO apiuser;
GRANT SELECT ON public.user_keys TO apiuser;
GRANT SELECT ON public.users_entities TO apiuser;
