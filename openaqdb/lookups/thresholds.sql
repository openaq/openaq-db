DELETE FROM thresholds;
INSERT INTO thresholds (measurands_id, value) VALUES
  (2, 5)
, (2, 10)
, (2, 250)
ON CONFLICT DO NOTHING;
