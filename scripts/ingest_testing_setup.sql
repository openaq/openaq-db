
  -- add some fetchlogs for testing
TRUNCATE fetchlogs CASCADE;
INSERT INTO fetchlogs (fetchlogs_id, key, last_modified)
  OVERRIDING SYSTEM VALUE
  VALUES
  (1, '/home/christian/Downloads/1713977723-sxb7b.json', now())
, (2, '/home/christian/Downloads/openaq-fetches/lcs-etl-pipeline/measures/airgradient/airgradient-1710004041-s61p5.csv', now())
, (3, '/home/christian/Downloads/habitatmap-1714036497-h84j.csv', now())
, (4, '/home/christian/Downloads/airgradient-1714003639-h32tu.csv', now())
, (5, '/home/christian/Downloads/senstate-1714007461-ivz5g.csv', now())
, (6, 'lcs-etl-pipeline/measures/purpleair/1664911958-z2atn.csv.gz', now())
, (7, 'realtime-gzipped/2022-10-04/1664912239.ndjson.gz', now())
, (8, 'lcs-etl-pipeline/measures/airgradient/airgradient-1689428589-h5th8.csv.gz', now())
ON CONFLICT DO NOTHING;


  -- add some partitions
  SELECT create_measurements_partition('2021-01-01'::date);
  SELECT create_measurements_partition('2024-01-01'::date);
  SELECT create_measurements_partition('2024-02-01'::date);
  SELECT create_measurements_partition('2024-03-01'::date);
  SELECT create_measurements_partition('2024-04-01'::date);
  SELECT create_measurements_partition('2024-05-01'::date);

  -- add some parameter maps
  -- make sure we have the mapper
  INSERT INTO measurands_map VALUES
  ('pm25', 2, 'ug/m3', 'clarity')
  ON CONFLICT DO NOTHING;
