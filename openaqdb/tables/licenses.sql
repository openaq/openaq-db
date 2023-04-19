-- adopted from the license tag of
-- https://github.com/openaddresses/openaddresses/blob/master/CONTRIBUTING.md#optional-address-tags
DROP TABLE IF EXISTS sources_licenses;
DROP TABLE IF EXISTS providers_licenses;
DROP TABLE IF EXISTS licenses;

DROP SEQUENCE IF EXISTS sources_licenses_sq;
DROP SEQUENCE IF EXISTS providers_licenses_sq;
DROP SEQUENCE IF EXISTS licenses_sq;

CREATE SEQUENCE IF NOT EXISTS licenses_sq START 10;
CREATE TABLE IF NOT EXISTS licenses (
  licenses_id int PRIMARY KEY DEFAULT nextval('licenses_sq')
  , attribution_entities_id int NOT NULL REFERENCES entities
  , description text -- short descriptive summary
  , url text -- link to the actual text/pdf/copy of license
  , attribution_required boolean DEFAULT 't'
  , share_alike_required boolean DEFAULT 't'
  , commercial_use_allowed boolean DEFAULT 'f'
  , redistribution_allowed boolean DEFAULT 'f'
  , modification_allowed boolean DEFAULT 'f'
  , metadata jsonb
);

-- need to be able to track any changes to the licenses
-- and a given source cant have more than one license at a time
CREATE SEQUENCE IF NOT EXISTS sources_licenses_sq START 10;
CREATE TABLE IF NOT EXISTS sources_licenses (
  sources_licenses_id int PRIMARY KEY DEFAULT nextval('sources_licenses_sq')
  , licenses_id int NOT NULL REFERENCES licenses
  , sources_id int NOT NULL REFERENCES sources
  , active_period daterange NOT NULL DEFAULT daterange(current_date, 'infinity')
  , EXCLUDE USING gist (sources_id WITH = , active_period WITH &&)
);

-- do the same for providers
CREATE SEQUENCE IF NOT EXISTS providers_licenses_sq START 10;
CREATE TABLE IF NOT EXISTS providers_licenses (
  providers_licenses_id int PRIMARY KEY DEFAULT nextval('providers_licenses_sq')
  , licenses_id int NOT NULL REFERENCES licenses
  , providers_id int NOT NULL REFERENCES providers
  , active_period daterange NOT NULL DEFAULT daterange(current_date, 'infinity')
  , EXCLUDE USING gist (providers_id WITH = , active_period WITH &&)
);

-- if we had a projects table we could do the same
-- CREATE SEQUENCE IF NOT EXISTS projects_licenses_sq START 10;
-- CREATE TABLE IF NOT EXISTS projects_licenses (
--   projects_licenses_id int PRIMARY KEY DEFAULT nextval('projects_licenses_sq')
--   , licenses_id int NOT NULL REFERENCES licenses
--   , projects_id int NOT NULL REFERENCES projects
--   , active_period daterange NOT NULL DEFAULT daterange(current_date, 'infinity')
--   , EXCLUDE USING gist (projects_id WITH = , active_period WITH &&)
-- );


-- And then we would need some queries to help us figure out what is the current license
