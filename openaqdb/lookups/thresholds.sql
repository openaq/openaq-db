INSERT INTO thresholds (measurands_id, value) VALUES
  (2, 5)
, (2, 10)
, (2, 250)
, (1, 15)
, (1, 20)
, (3, 100)
, (5, 10)
, (5, 40)
, (6, 20)
, (6, 40)
ON CONFLICT DO NOTHING;
