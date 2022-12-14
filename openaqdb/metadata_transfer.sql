

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

-- move the timezone data from the metadata to the timezones_id field
UPDATE sensor_nodes
SET timezones_id = t.gid
FROM timezones t
WHERE sensor_nodes.metadata->>'timezone' IS NOT NULL
AND sensor_nodes.metadata->>'timezone' = t.tzid
--AND timezones_id IS NULL
;



UPDATE sensor_nodes
SET timezones_id = get_timezones_id(geom)
WHERE geom IS NOT NULL
AND timezones_id IS NULL
AND added_on > current_date - 3;


UPDATE sensor_nodes
SET providers_id = p.providers_id
FROM providers p
WHERE sensor_nodes.source_name = p.source_name
AND sensor_nodes.providers_id IS NULL
OR sensor_nodes.providers_id = 1;

INSERT INTO providers (label, source_name, description, export_prefix)
SELECT source_name
, source_name
, 'added from ingest process'
, lower(source_name)
FROM sensor_nodes
GROUP BY 1,2,3,4
ON CONFLICT(source_name) DO NOTHING;


UPDATE sensor_nodes
SET countries_id = c.countries_id
FROM countries c
WHERE sensor_nodes.country = c.iso
AND sensor_nodes.countries_id IS NULL;




SELECT sensor_nodes_id
, source_name
, geom
, st_x(geom)
, st_y(geom)
, added_on
, ismobile
FROM sensor_nodes
WHERE origin = 'HABITATMAP'
AND geom IS NOT NULL
AND added_on > current_date - 30;

SELECT *
FROM sensor_nodes_check
WHERE origin = 'HABITATMAP'
AND has_coordinates;

SELECT *
FROM measurements
WHERE lat IS NOT NULL
AND lat = 200
LIMIT 10;
