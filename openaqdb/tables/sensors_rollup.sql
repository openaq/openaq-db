-- Sensors rollups will store the summary for the sensors
-- entire lifespan
--DROP TABLE IF EXISTS sensors_rollup;
CREATE SEQUENCE IF NOT EXISTS sensors_rollup_sq START 10;
CREATE TABLE IF NOT EXISTS sensors_rollup (
    sensors_id int PRIMARY KEY REFERENCES sensors
  , datetime_first timestamptz NOT NULL             -- first recorded measument datetime (@ingest)
  , datetime_last timestamptz NOT NULL              -- last recorded measurement time (@ingest)
  , geom_latest geometry                            -- last recorded point (@ingest)
  , value_latest double precision NOT NULL          -- last recorded measurement (@ingest)
  , value_count int NOT NULL NOT NULL               -- total count of measurements (@ingest, @rollup)
  , value_avg double precision NOT NULL             -- average of all measurements (@ingest, @rollup)
  , value_sd double precision                       -- sd of all measurements (@ingest, @rollup)
  , value_min double precision NOT NULL             -- lowest measurement value (@ingest, @rollup)
  , value_max double precision NOT NULL             -- highest value measured (@ingest, @rollup)
  , added_on timestamptz NOT NULL DEFAULT now()     -- first time measurements were added (@ingest)
  , modified_on timestamptz NOT NULL DEFAULT now()  -- last time we measurements were added (@ingest)
);
