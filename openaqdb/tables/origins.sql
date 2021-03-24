CREATE TABLE origins(
    origin text primary key,
    metadata jsonb default '{}'::jsonb
);

INSERT INTO origins (origin, metadata) VALUES
('OPENAQ','{"entity": "government", "sensorType": "reference grade"}'),
('AQDC','{"entity": "research", "sensorType": "low-cost sensor"}'),
('CMU','{"entity": "research", "sensorType": "low-cost sensor"}'),
('HABITATMAP','{"entity": "community", "sensorType": "low-cost sensor"}'),
('PURPLEAIR','{"entity": "community", "sensorType": "low-cost sensor"}')
;