-- use is_public and maybe we can keep it
	ALTER TABLE sensor_nodes
	ADD COLUMN is_public boolean DEFAULT 't';

	ALTER TABLE sensors
	ADD COLUMN is_public boolean DEFAULT 't';

-- create an index in the hopes of speeding things up
CREATE INDEX IF NOT EXISTS sensor_nodes_public_idx ON sensor_nodes USING btree (is_public);
CREATE INDEX IF NOT EXISTS sensors_public_idx ON sensors USING btree (is_public);


-- Mark all purple air nodes and sensors as inactive

	UPDATE sensor_nodes
	SET is_public = false
	WHERE is_public
	AND source_name ~* 'purpleair';


	UPDATE sensors
	SET is_public = false
	WHERE is_public
	AND sensors_id IN (
		SELECT sensors_id
		FROM sensors s
		JOIN sensor_systems sy USING (sensor_systems_id)
		JOIN sensor_nodes sn USING (sensor_nodes_id)
		WHERE sn.source_name ~* 'purpleair');


	-- this will take care of the easy stuff
\i ../locations/locations.sql
\i ../tables/countries_views.sql
\i ../tables/providers_views.sql


-- v2
-- averages - INNER JOIN on locations
	-- cities - sensors is public
	-- countries - nodes is public
	-- locations -- locations_view
	-- latest --locations view
	-- measurements -- locations_view
	-- parameters  -- no need
	-- projects -- doesnt seem to work
	-- sources -- sensors is public
	-- summary -- no need

	-- v3
	-- instruments -- no need
	-- manufacturers -- no need
	-- locations -- locations_view
	-- parameters -- no need
	-- countries -- countries_view
	-- manufacturers -- nodes is public
	-- locations/measurements -- locations_view, is_public
	-- owners -- is_public
	-- locations/trends -- locations_view
	-- providers -- providers_view
	-- sensors/measurements -- locations_view, is_public
	-- locations/sensors -- is_public, locations_view
	-- sensors -- is_public, locations_view
