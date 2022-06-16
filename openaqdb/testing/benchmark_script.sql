-- The open data query

-- select a random sensor
SELECT ROUND(random() * 10000) as row;
\gset

SELECT sensor_nodes_id FROM sensor_nodes OFFSET :row LIMIT 1;
\gset

\set day '''2016-05-03'''

SELECT sn.sensor_nodes_id
, datetime
, m.sensors_id
, value
, lon
, lat
FROM measurements m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
WHERE sn.sensor_nodes_id = :sensor_nodes_id
AND datetime > (:day)::timestamp
AND datetime <= (:day)::timestamp + '1day'::interval;
