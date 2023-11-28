CREATE TABLE rejects (
  t timestamptz DEFAULT now()
, tbl text
, r jsonb
, fetchlogs_id int REFERENCES fetchlogs ON DELETE CASCADE
, reason text
);
