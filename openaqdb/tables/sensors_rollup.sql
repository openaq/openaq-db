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


CREATE OR REPLACE VIEW sensor_rollups_summary AS
SELECT m.measurand
  , m.units
  , COUNT(1) as sensors_count
  , ROUND(AVG(value_avg::numeric), 2) as value_avg
  , ROUND(MIN(value_min::numeric), 2) as value_min
  , ROUND((PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY value_min))::numeric, 2) as value_min_p01
  , ROUND((PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY value_min))::numeric, 2) as value_min_p05
  , ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value_max))::numeric, 2) as value_max_p95
  , ROUND((PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY value_max))::numeric, 2) as value_max_p99
  , ROUND(MAX(value_max::numeric), 2) as value_max
  , MAX(r.added_on) as last_added
  , MAX(r.modified_on) as last_modified
FROM sensors_rollup r
JOIN sensors s USING (sensors_id)
JOIN measurands m USING (measurands_id)
GROUP BY 1, 2
ORDER BY 3 DESC;
