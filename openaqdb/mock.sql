


INSERT INTO public.instruments (instruments_id
, label
, description
, manufacturer_entities_id
, is_monitor) VALUES
(1, 'Unknown Sensor', 'Instrument details not available', 1, 'f')
, (2, 'Government Monitor', 'Instrument details are not available', 1, 't')
ON CONFLICT DO NOTHING;

-- create some test sites
  -- using airgradient provider just to get a license
WITH locations AS (
  SELECT * FROM
  (VALUES
    ('testing/site1', 'testing', 'testing site 1', ST_SetSRID(ST_Point( -151.76306, -16.51516), 4326), 'airnow', 'pm25') -- Kirbati
  , ('testing/site2', 'testing', 'testing site 2', ST_SetSRID(ST_Point( -121.8418, 44.75228), 4326), 'airnow', 'pm25') -- America/Los_Angeles
  , ('testing/site3', 'testing', 'testing site 3', ST_SetSRID(ST_Point( -71.104, 42.315),4326), 'airnow', 'pm25') -- America/New_York
  , ('testing/site4', 'testing', 'testing site 4', ST_SetSRID(ST_Point( -0.107389, 51.487236), 4326), 'airnow', 'pm25') -- Europe/London
  , ('testing/site5', 'testing', 'testing site 5', ST_SetSRID(ST_Point( 185.199922, -20.248716), 4326), 'airnow', 'pm25')
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
    , 2 as instruments_id -- 2 is a monitor and not a sensor
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
SELECT generate_series('2023-03-01'::date, '2023-04-01'::date, '30min'::interval) as datetime
  ) INSERT INTO measurements (datetime, sensors_id, value)
  --SELECT f.datetime, s.sensors_id, date_part('day', as_local(datetime - interval '1sec', t.tzid))
  SELECT as_utc(datetime, t.tzid), s.sensors_id, date_part('day', datetime - interval '1sec')
  FROM fake_times f
  JOIN sensors s ON (TRUE)
  JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
  JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
  JOIN timezones t ON (sn.timezones_id = t.timezones_id)
  ON CONFLICT DO NOTHING;


 -- make sure we have something to test the moy with
WITH fake_times AS (
--SELECT generate_series(current_date - (365 * 2), current_date, '1d'::interval) as datetime
SELECT generate_series('2021-12-25'::date, '2023-01-05'::date, '1d'::interval) as datetime
  ) INSERT INTO measurements (datetime, sensors_id, value)
  --SELECT f.datetime, s.sensors_id, date_part('day', as_utc(datetime - interval '1sec', t.tzid))
  SELECT as_utc(datetime, t.tzid), s.sensors_id, date_part('day', datetime - interval '1sec')
  FROM fake_times xf
  JOIN sensors s ON (TRUE)
  JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
  JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
  JOIN timezones t ON (sn.timezones_id = t.timezones_id)
  WHERE sensors_id = 1
  ON CONFLICT DO NOTHING;


WITH fake_times AS (
SELECT generate_series(current_date - 7, current_timestamp, '30min'::interval) as datetime
  ) INSERT INTO measurements (datetime, sensors_id, value)
  --SELECT f.datetime, s.sensors_id, date_part('day', as_utc(datetime - interval '1sec', t.tzid))
  SELECT as_utc(datetime, t.tzid), s.sensors_id, date_part('day', datetime - interval '1sec')
  FROM fake_times f
  JOIN sensors s ON (TRUE)
  JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
  JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
  JOIN timezones t ON (sn.timezones_id = t.timezones_id)
  WHERE sensors_id = 1
  ON CONFLICT DO NOTHING;
