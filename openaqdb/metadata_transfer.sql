

-- pull the averaging information from the sensor metadata
-- and add to the sensors table

UPDATE sensors
SET data_averaging_period_seconds = (metadata->>'data_averaging_period_seconds')::int
WHERE metadata->>'data_averaging_period_seconds' IS NOT NULL;

-- this field is a bit of a mess and some data cant be transferred reliably
UPDATE sensors
SET data_averaging_period_seconds = 3600
WHERE metadata->>'data_avg_dur' IS NOT NULL
AND metadata->>'data_avg_dur' ~* '^hourly';


UPDATE sensors
SET data_logging_period_seconds = (metadata->>'logging_interval_seconds')::numeric::int
WHERE metadata->>'logging_interval_seconds' IS NOT NULL
AND metadata->>'logging_interval_seconds' != 'true';

------------------------------------------------
-- Adding the manufacturers                   --
-- They exist at both system and sensor level --
------------------------------------------------

INSERT INTO contacts (full_name, contact_type, added_by)
SELECT metadata->>'manufacturer_name'
, 'Organization'::contact_type
, 1
FROM sensors
WHERE metadata->>'manufacturer_name' IS NOT NULL
AND metadata->>'manufacturer_name' NOT IN (SELECT full_name FROM contacts)
GROUP BY 1,2;

INSERT INTO contacts (full_name, contact_type, added_by)
SELECT metadata->>'manufacturer_name'
, 'Organization'::contact_type
, 1
FROM sensor_systems
WHERE metadata->>'manufacturer_name' IS NOT NULL
AND metadata->>'manufacturer_name' NOT IN (SELECT full_name FROM contacts)
GROUP BY 1,2;

INSERT INTO contacts (full_name, contact_type, added_by)
SELECT metadata->>'deployed_by'
, 'Organization'::contact_type
, 1
FROM sensor_systems
WHERE metadata->>'deployed_by' IS NOT NULL
AND metadata->>'deployed_by' NOT IN (SELECT full_name FROM contacts)
GROUP BY 1,2;

----------------------------
-- Adding the instruments --
----------------------------

INSERT INTO instruments (label, description, manufacturer_contacts_id)
SELECT metadata->>'model_name' as label
, 'Added on initial metadata transfer'
, MIN(c.contacts_id)
FROM sensor_systems s
LEFT JOIN contacts c ON (s.metadata->>'manufacturer_name' = c.full_name)
WHERE metadata->>'model_name' IS NOT NULL
GROUP BY 1,2
ON CONFLICT DO NOTHING;

INSERT INTO instruments (label, description, manufacturer_contacts_id)
SELECT metadata->>'aqdc:usr_prov_snsr_sys_id' as label
, 'Added on initial metadata transfer'
, MIN(c.contacts_id)
FROM sensor_systems s
LEFT JOIN contacts c ON (s.metadata->>'manufacturer_name' = c.full_name)
WHERE metadata->>'aqdc:usr_prov_snsr_sys_id' IS NOT NULL
GROUP BY 1,2
ON CONFLICT DO NOTHING;




\set mfield '''sensor_node_source_fullname'''
SELECT metadata
FROM sensors
WHERE metadata->>(:mfield) IS NOT NULL
LIMIT 10;
SELECT metadata->>(:mfield),
COUNT(1)
FROM sensors
WHERE metadata->>(:mfield) IS NOT NULL
GROUP BY 1;
SELECT metadata
FROM sensor_systems
WHERE metadata->>(:mfield) IS NOT NULL
LIMIT 10;
SELECT metadata->>(:mfield),
COUNT(1)
FROM sensor_nodes
WHERE metadata->>(:mfield) IS NOT NULL
GROUP BY 1;

SELECT metadata->>'sampling_duration',
COUNT(1)
FROM sensors
WHERE metadata->>'sampling_duration' IS NOT NULL
GROUP BY 1
LIMIT 10;
