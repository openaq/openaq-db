
-- create some test sites
  -- using airgradient provider just to get a license
WITH locations AS (
  SELECT * FROM
  (VALUES
  ('testing/site1', 'testing', 'testing site 1', ST_SetSRID(ST_Point( -71.104, 42.315),4326), 'airnow', 'pm25')
  , ('testing/site2', 'testing', 'testing site 2', ST_SetSRID(ST_Point( -72.104, 42.415),4326), 'airnow', 'pm25')
   ) as t (source_id, source_name, site_name, geom, provider, measurand)
  ), inserted_nodes AS (
  INSERT INTO sensor_nodes (
    source_id
  , source_name
  , site_name
  , geom
  , countries_id
  , timezones_id
  , providers_id
  , owner_entities_id
  , ismobile             -- currently not defaulted in the table
  )
  SELECT source_id
  , source_name
  , site_name
  , geom
  , get_countries_id(geom)
  , get_timezones_id(geom)
  , get_providers_id(provider)
  , 1
  , 'f'
  FROM locations
  ON CONFLICT (source_name, source_id) DO UPDATE
  SET geom = EXCLUDED.geom
  , timezones_id = EXCLUDED.timezones_id
  , countries_id = EXCLUDED.countries_id
  , owner_entities_id = EXCLUDED.owner_entities_id
  RETURNING source_id, sensor_nodes_id
  ), inserted_systems AS (
    INSERT INTO sensor_systems (sensor_nodes_id, source_id, instruments_id)
    SELECT sensor_nodes_id
    , source_id
    , 1 as instruments_id
    FROM locations l
    JOIN inserted_nodes n USING (source_id)
    ON CONFLICT (sensor_nodes_id, source_id) DO UPDATE
    SET instruments_id = EXCLUDED.instruments_id
    RETURNING sensor_systems_id, source_id
  ), inserted_sensors AS (
    INSERT INTO sensors (
        sensor_systems_id
        , source_id
        , measurands_id
        , data_averaging_period_seconds
        , data_logging_period_seconds
        )
    SELECT sensor_systems_id
    , source_id||'/'||measurand
    , get_measurands_id(l.measurand)
    , 60*30
    , 60*30
    FROM locations l
    JOIN inserted_systems n USING (source_id)
    ON CONFLICT (sensor_systems_id, measurands_id) DO UPDATE
    SET source_id = EXCLUDED.source_id
    RETURNING sensor_systems_id, source_id
  ) SELECT * FROM inserted_sensors;




WITH fake_times AS (
SELECT generate_series(current_date - 3, current_date, '30min'::interval) as datetime
  ) INSERT INTO measurements (datetime, sensors_id, value)
    SELECT datetime, 1 as sensors_id, 1 as value FROM fake_times
    ON CONFLICT (sensors_id, datetime) DO UPDATE
    SET value = EXCLUDED.value;
