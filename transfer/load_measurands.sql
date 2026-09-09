-- Stage table matches the SOURCE schema (columns present in the CSV).
CREATE TEMP TABLE stage_measurands (
    measurands_id  int,
    measurand      text,
    units          text,
    display        text,
    description    text,
    parameter_type text,
    upper_limit    double precision,
    lower_limit    double precision
);

\COPY stage_measurands FROM PSTDIN WITH (FORMAT CSV, HEADER)

INSERT INTO public.measurands AS m (
    measurands_id,
    measurand,
    units,
    display,
    description,
    parameter_type,
    upper_limit,
    lower_limit
)
OVERRIDING SYSTEM VALUE
SELECT
    measurands_id,
    measurand,
    units,
    display,
    description,
    parameter_type::parameter_type,
    upper_limit,
    lower_limit
FROM stage_measurands
ON CONFLICT (measurands_id) DO UPDATE
SET
    measurand      = EXCLUDED.measurand,
    units          = EXCLUDED.units,
    display        = EXCLUDED.display,
    description    = EXCLUDED.description,
    parameter_type = EXCLUDED.parameter_type,
    upper_limit    = EXCLUDED.upper_limit,
    lower_limit    = EXCLUDED.lower_limit;

\echo Measurands loaded:
SELECT
    (SELECT count(*) FROM stage_measurands)  AS staged,
    (SELECT count(*) FROM public.measurands) AS total;


-- allow all measurands and use the current units
-- moving forward we are migrating mb to hpa
UPDATE measurands
  SET units_id = get_units_id(units)
  WHERE units != 'mb';

\i ../openaqdb/lookups/measurands_map.sql
