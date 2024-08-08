WITH map AS (
SELECT d.*
, m.measurands_id
FROM (VALUES
-- ('PurpleAir', 'pm1.0', 'pm1', 'µg/m³'),
-- ('PurpleAir', 'pm2.5','pm25', 'µg/m³'),
-- ('PurpleAir', 'pm10.0','pm10', 'µg/m³'),
-- ('PurpleAir', '0.3_um_count','um003', 'particles/cm³'),
-- ('PurpleAir', '0.5_um_count','um005', 'particles/cm³'),
-- ('PurpleAir', '1.0_um_count','um010', 'particles/cm³'),
-- ('PurpleAir', '2.5_um_count','um025', 'particles/cm³'),
-- ('PurpleAir', '5.0_um_count','um050', 'particles/cm³'),
-- ('PurpleAir', '10.0_um_count','um100', 'particles/cm³'),
-- ('PurpleAir', 'humidity','humidity', '%'),
-- ('PurpleAir', 'temperature','temperature', 'f'),
-- ('PurpleAir', 'pressure','pressure', 'mb'),
-- ('PurpleAir', 'voc','voc', 'iaq'),
-- ('PurpleAir', 'ozone1','ozone', 'ppb')
  ('clarity', 'relHumid','relativehumidity', '%') -- RelativeHumidity
, ('clarity', 'temperature','temperature', 'c') -- Temperature
, ('clarity', 'pm2_5ConcMass','pm25', 'μg/m3') --	PM2.5 mass concentration
, ('clarity', 'pm1ConcMass','pm1', 'μg/m3') --	PM1 mass concentration
, ('clarity', 'pm10ConcMass','pm10', 'μg/m3') --	PM10 mass concentration
, ('clarity', 'no2Conc','no2', 'ppb') -- NO2 volume concentration
, ('clarity', 'windSpeed','windspeed', 'm/s') --	Wind speed
, ('clarity', 'windDirection','winddirection', 'degrees') --	Wind direction, compass degrees (0°=North, then clockwise)
) as d(source_name, key, db, units)
JOIN measurands m ON (m.measurand = d.db AND m.units = d.units)
ORDER BY m.measurands_id, 1)
INSERT INTO measurands_map (source_name, key, units, measurands_id)
SELECT source_name, key, units, measurands_id
FROM map
ON CONFLICT DO NOTHING;

INSERT INTO measurands_map (key, measurands_id, units, source_name) VALUES
  ('pm1', 19, '', 'clarity')
, ('no2', 15, '', 'clarity')
, ('pm10', 1, '', 'clarity')
, ('pm25', 2, '', 'clarity')
, ('o3', 3, '', 'cmu')
, ('so2', 101, '', 'cmu')
, ('co', 102, '', 'cmu')
, ('no', 24, '', 'cmu')
, ('pm4', 19844, '', 'senstate')
, ('co2', 21, '', 'houston')
, ('bc', 11, '', 'houston')
, ('um025', 130, '', 'houston')
ON CONFLICT DO NOTHING
;
