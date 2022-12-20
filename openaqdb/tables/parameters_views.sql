DROP VIEW IF EXISTS parameters_view CASCADE;
CREATE OR REPLACE VIEW parameters_view AS
-----------------------------------
WITH locations_measurands AS (
-----------------------------------
    SELECT m.measurands_id
    , COUNT(1) as locations_count
    FROM sensor_nodes sn
    JOIN sensor_systems ss USING (sensor_nodes_id)
    JOIN sensors s USING (sensor_systems_id)
    JOIN measurands m USING (measurands_id)
    GROUP BY m.measurands_id
-----------------------------------
), measurements_measurands AS (
-----------------------------------
    SELECT s.measurands_id
    , SUM(sl.value_count) AS measurements_count
    , MIN(sl.datetime_first) as datetime_first
    , MAX(sl.datetime_last) as datetime_last
    FROM sensor_nodes sn
    JOIN sensor_systems ss USING (sensor_nodes_id)
    JOIN sensors s USING (sensor_systems_id)
    JOIN sensors_rollup sl USING (sensors_id)
    GROUP BY s.measurands_id)
-----------------------------------
SELECT measurands.measurands_id AS id
, measurands.measurand AS name
, measurands.display AS display_name
, measurands.units
, measurands.description
, lm.locations_count
, mm.measurements_count
, datetime_first
, datetime_last
FROM measurands
JOIN locations_measurands lm USING (measurands_id)
JOIN measurements_measurands mm USING (measurands_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS parameters_view_cached AS
SELECT *
FROM parameters_view;
CREATE INDEX ON parameters_view_cached (id);
