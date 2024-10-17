
  INSERT INTO entities (entity_type, full_name, metadata) VALUES
    ('Sensor Manufacturer', 'MetOne', '{}'::jsonb)
  , ('Sensor Manufacturer', 'AethLabs', '{}'::jsonb)
  , ('Sensor Manufacturer', 'Ecotech', '{}'::jsonb);

  --INSERT INTO int
  WITH manufacturer AS (
    SELECT entities_id
    FROM entities
    WHERE full_name = 'MetOne'
  )
  INSERT INTO instruments (manufacturer_entities_id, label, description, is_monitor, ingest_id)
  SELECT entities_id, v.model_name, v.description, v.is_monitor, v.ingest_id
  FROM manufacturer, (VALUES
    ('AIO 2','Sonic Weather Sensor/Station', true, 'metone:aio2')
  , ('BAM 1020','Beta Attenuation Mass Monitor', true, 'metone:bam_1020')
  ) as v(model_name, description, is_monitor, ingest_id);


  WITH manufacturer AS (
    SELECT entities_id
    FROM entities
    WHERE full_name = 'Ecotech'
  )
  INSERT INTO instruments (manufacturer_entities_id, label, description, is_monitor, ingest_id)
  SELECT entities_id, v.model_name, v.description, v.is_monitor, v.ingest_id
  FROM manufacturer, (VALUES
    ('Serinus 30','Carbon Monoxide (CO) analyser', true, 'ecotech:serinus_30')
  ) as v(model_name, description, is_monitor, ingest_id);

  WITH manufacturer AS (
    SELECT entities_id
    FROM entities
    WHERE full_name = 'AethLabs'
  )
  INSERT INTO instruments (manufacturer_entities_id, label, description, is_monitor, ingest_id)
  SELECT entities_id, v.model_name, v.description, v.is_monitor, v.ingest_id
  FROM manufacturer, (VALUES
    ('MA350','5-wavelength UV-VIS-IR Black Carbon monitor', true, 'aethlabs:ma350')
  ) as v(model_name, description, is_monitor, ingest_id);
