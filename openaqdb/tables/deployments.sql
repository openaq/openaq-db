
CREATE SEQUENCE IF NOT EXISTS adapters_sq START 10;
CREATE TABLE IF NOT EXISTS adapters (
  adapters_id int PRIMARY KEY DEFAULT nextval('adapters_sq')
  , name text NOT NULL UNIQUE -- name of adapter/provider
  , handler text   -- lcs or fetcher
  , description text
);


CREATE SEQUENCE IF NOT EXISTS deployments_sq START 10;
CREATE TABLE IF NOT EXISTS deployments (
  deployments_id int PRIMARY KEY DEFAULT nextval('deployments_sq')
  , name text NOT NULL UNIQUE
  , description text
  , temporal_offset int
  , providers_id int REFERENCES providers
  , adapters_id int REFERENCES adapters
  , is_active boolean NOT NULL DEFAULT 't'
);


ALTER TABLE providers
ADD COLUMN adapters_id int NOT NULL DEFAULT 1
--, ADD COLUMN is_active boolean DEFAULT 't'
;


INSERT INTO adapters (adapters_id, name, description) VALUES
(1, 'openaq-adapter', 'Default adapter for OpenAQ. Does not current exist but will be used as default when one is not specified.');

-- If provider is provided than we use that and ignore the active flag
-- otherwise check for adapter and also use the active flag
-- otherwise just assume all active providers
CREATE OR REPLACE FUNCTION deployment_sources(pid int, aid int) RETURNS jsonb AS $$
SELECT --jsonb_build_object('count', COUNT(1))
json_agg(metadata)
FROM providers p
JOIN adapters a ON (p.adapters_id = a.adapters_id)
WHERE (pid IS NULL AND aid IS NULL AND p.is_active)
OR (aid IS NULL AND p.providers_id = pid)
OR (pid IS NULL AND a.adapters_id = aid AND p.is_active);
$$ LANGUAGE SQL;


-- -- should be one
-- SELECT deployment_sources(:airnow, NULL);
-- -- should be about 29
-- SELECT deployment_sources(NULL, 340);
-- -- should be about 150
-- SELECT deployment_sources(NULL, NULL);



-- -- Now we can update the providers with that data
-- SELECT name
-- , temporal_offset as offset
-- , jsonb_array_length(deployment_sources(providers_id, adapters_id)) as sources_count
-- FROM deployments
-- WHERE is_active;


-- SELECT name
-- , temporal_offset as offset
-- , deployment_sources(providers_id, adapters_id)
-- FROM deployments
-- WHERE is_active
-- AND name ~* 'airnow'
-- ;
