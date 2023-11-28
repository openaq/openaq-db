TRUNCATE measurements
	, sensors
	, versions
	, sensor_systems
	, sensor_nodes
	, rejects
	CASCADE;

--DELETE FROM fetchlogs WHERE key ~* '/cac/';
	INSERT INTO fetchlogs (key) VALUES
	  ('cac-pipeline/measures/cac/2023-08-24_measurements_5min_000000000000.csv.gz')
	, ('cac-pipeline/stations/cac/Bicholi Habsi.json.gz')
	, ('cac-pipeline/stations/cac/Malav.json.gz')
	, ('cac-pipeline/stations/cac/Musakhedi.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-as-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-bc_375-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-bc_470-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-bc_528-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-bc_625-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-bc_880-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-ca-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-cl-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-co-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-ec-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-fe-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-k-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-ni-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-no2-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-no3-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-o3-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-oc-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-pb-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-pm10-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-pm25-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-pressure-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-relativehumidity-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-so2-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-so4-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-temperature-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-wind_direction-u-20230824.json.gz')
	, ('cac-pipeline/versions/cac/versioning-Malav-wind_speed-u-20230824.json.gz')
	ON CONFLICT DO NOTHING
	;

	INSERT INTO measurands (measurand, units, display, description, parameter_type) VALUES
	('bc_375', 'µg/m³', 'BC (375nm)', 'Black carbon mass concentration measured at 375 nm', 'pollutant')
,	('bc_470', 'µg/m³', 'BC (470nm)', 'Black carbon mass concentration measured at 470 nm', 'pollutant')
,	('bc_528', 'µg/m³', 'BC (528nm)', 'Black carbon mass concentration measured at 528 nm', 'pollutant')
,	('bc_625', 'µg/m³', 'BC (625nm)', 'Black carbon mass concentration measured at 625 nm', 'pollutant')
,	('bc_880', 'µg/m³', 'BC (880nm)', 'Black carbon mass concentration measured at 880 nm', 'pollutant')
ON CONFLICT DO NOTHING;

SELECT *
	FROM fetchlogs
	WHERE key ~* 'station';


	SELECT r->>'measurand' as measurand
	, r->>'units' as units
	FROM rejects
	WHERE tbl = 'ms_sensors-missing-measurands-id';

	SELECT measurand
	, units
	FROM measurands
	ORDER BY 1;
