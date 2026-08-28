CREATE TYPE parameter_type AS ENUM (
  'pollutant'
, 'meteorological'
);

CREATE TABLE units (
    units_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- canonical ingest-friendly form (what we store internally)
    units text NOT NULL UNIQUE,
    -- human-friendly display form (µg/m³, °C, etc.)
    display text NOT NULL,
    -- what kind of thing this measures (dimension)
    dimension text NOT NULL,  -- 'mass_concentration', 'mixing_ratio', 'temperature', ...
    description text,
    is_active boolean DEFAULT true
);

CREATE TABLE IF NOT EXISTS measurands (
    measurands_id int generated always as identity primary key,
    measurand text not null,
    units text not null,
    display text,
    description text
    , units_id int REFERENCES units
    , ingest_key text UNIQUE
    , parameter_type parameter_type NOT NULL DEFAULT 'pollutant'
    , upper_limit double precision
    , lower_limit double precision
    , is_active boolean DEFAULT true
    , UNIQUE (measurand, units)
    , UNIQUE (measurand, units_id)
    , CHECK (upper_limit > lower_limit)
);


CREATE TABLE IF NOT EXISTS measurands_map (
  key text NOT NULL
  , measurands_id int NOT NULL REFERENCES measurands ON DELETE CASCADE
  , units text NOT NULL
  , source_name text NOT NULL
  , UNIQUE(key, units)
);

-- Alias table for the "µg/m³ vs ug/m3" problem
CREATE TABLE unit_aliases (
    alias text PRIMARY KEY,
    units_id int NOT NULL REFERENCES units
);

CREATE TABLE unit_conversions (
    from_units_id int NOT NULL REFERENCES units,
    to_units_id int NOT NULL REFERENCES units,
    factor double precision NOT NULL,
    intercept double precision NOT NULL DEFAULT 0,
    measurand text,
    CONSTRAINT unit_conversions_uniq
      UNIQUE NULLS NOT DISTINCT (from_units_id, to_units_id, measurand)
);


CREATE OR REPLACE VIEW measurands_map_view AS
WITH all_measurands AS (
SELECT measurands_id
, key
FROM measurands_map
JOIN measurands USING (measurands_id)
  WHERE is_active
UNION ALL
SELECT measurands_id
, CASE WHEN ingest_key IS NOT NULL THEN ingest_key ELSE concat(measurand, units) END
FROM measurands
  WHERE is_active
  )
  SELECT key
  , MIN(measurands_id) as measurands_id
  , COUNT(DISTINCT measurands_id) as measurand_duplicates
  FROM all_measurands
GROUP BY 1;


CREATE OR REPLACE FUNCTION get_measurands_id(m text)
RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT measurands_id
FROM measurands
WHERE lower(measurand) = lower(m)
LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION get_units_id(u text) RETURNS int
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT units_id FROM units WHERE units = lower(u)
    UNION ALL
    SELECT units_id FROM unit_aliases WHERE alias = u
    UNION ALL
    SELECT units_id FROM unit_aliases WHERE lower(alias) = lower(u)
    LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION transform_units(
    value double precision,
    from_units text,
    to_units text,
    measurand text DEFAULT NULL
) RETURNS double precision AS $$
DECLARE
    _from_id int;
    _to_id int;
    _factor double precision;
    _intercept double precision;
BEGIN
    -- Null-safe short circuit
    IF value IS NULL OR from_units IS NULL OR to_units IS NULL THEN
        RETURN value;
    END IF;
    -- Resolve unit strings (handles aliases + case)
    _from_id := get_units_id(from_units);
    _to_id := get_units_id(to_units);

    IF _from_id IS NULL THEN
        RAISE EXCEPTION 'Unknown from_units: %', from_units;
    END IF;
    IF _to_id IS NULL THEN
        RAISE EXCEPTION 'Unknown to_units: %', to_units;
    END IF;

    -- Identity conversion (same canonical unit, even if aliased differently)
    IF _from_id = _to_id THEN
        RETURN value;
    END IF;

    -- Look up conversion: prefer measurand-specific, fall back to general
    SELECT factor, intercept
      INTO _factor, _intercept
      FROM unit_conversions uc
     WHERE from_units_id = _from_id
       AND to_units_id = _to_id
       AND (uc.measurand IS NULL OR uc.measurand = transform_units.measurand)
     ORDER BY (uc.measurand IS NOT NULL) DESC  -- prefer specific match
     LIMIT 1;

    IF _factor IS NULL THEN
      IF measurand IS NULL THEN
        RAISE EXCEPTION 'No conversion from % to %', from_units, to_units;
      ELSE
        RAISE EXCEPTION 'No conversion from % to % for %', from_units, to_units, measurand;
      END IF;
    END IF;
    RETURN value * _factor + _intercept;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION transform_units(
    value double precision,
    from_units_id int,
    to_units_id int,
    measurand text DEFAULT NULL
) RETURNS double precision
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
DECLARE
    _factor double precision;
    _intercept double precision;
BEGIN
    IF value IS NULL OR from_units_id IS NULL OR to_units_id IS NULL THEN
        RETURN value;
    END IF;

    IF from_units_id = to_units_id THEN
        RETURN value;
    END IF;

    SELECT factor, intercept
      INTO _factor, _intercept
      FROM unit_conversions uc
     WHERE from_units_id = transform_units.from_units_id
       AND to_units_id = transform_units.to_units_id
       AND (uc.measurand IS NULL OR uc.measurand = transform_units.measurand)
     ORDER BY (uc.measurand IS NOT NULL) DESC
     LIMIT 1;

    IF _factor IS NULL THEN
      IF measurand IS NULL THEN
        RAISE EXCEPTION 'No conversion from % to %', from_units, to_units;
      ELSE
        RAISE EXCEPTION 'No conversion from % to % for %', from_units, to_units, measurand;
      END IF;
    END IF;

    RETURN value * _factor + _intercept;
END;
$$;

CREATE OR REPLACE VIEW unit_conversions_reference AS
WITH aliases AS (
    SELECT units_id,
           array_agg(alias ORDER BY alias) AS aliases
      FROM unit_aliases
     GROUP BY units_id
)
SELECT
    fu.units          AS from_units,
    fu.display        AS from_display,
    fa.aliases        AS from_aliases,
    tu.units          AS to_units,
    tu.display        AS to_display,
    fu.dimension      AS dimension,
    uc.measurand,
    uc.factor,
    uc.intercept,
    CASE
        WHEN uc.intercept = 0 THEN
            format('y = x * %s', uc.factor)
        ELSE
            format('y = x * %s + %s', uc.factor, uc.intercept)
    END AS formula
  FROM unit_conversions uc
  JOIN units fu       ON fu.units_id = uc.from_units_id
  JOIN units tu       ON tu.units_id = uc.to_units_id
  LEFT JOIN aliases fa ON fa.units_id = fu.units_id
 ORDER BY fu.dimension,
          fu.units,
          tu.units,
          uc.measurand NULLS FIRST;

CREATE OR REPLACE VIEW units_reference AS
SELECT u.units_id,
       u.units,
       u.display,
       u.dimension,
       u.description,
       u.is_active,
       COALESCE(a.aliases, '{}') AS aliases
  FROM units u
  LEFT JOIN (
      SELECT units_id, array_agg(alias ORDER BY alias) AS aliases
        FROM unit_aliases
       GROUP BY units_id
  ) a ON a.units_id = u.units_id
 ORDER BY u.dimension, u.units;

CREATE OR REPLACE VIEW measurands_reference AS
WITH specific_conversions AS (
    SELECT uc.measurand,
           array_agg(
               format('%s → %s (×%s)',
                      fu.units, tu.units, uc.factor)
               ORDER BY fu.units, tu.units
           ) AS conversions
      FROM unit_conversions uc
      JOIN units fu ON fu.units_id = uc.from_units_id
      JOIN units tu ON tu.units_id = uc.to_units_id
     WHERE uc.measurand IS NOT NULL
     GROUP BY uc.measurand
)
SELECT
    m.measurands_id,
    m.measurand,
    m.units          AS units,
    u.display        AS units_display,
    u.dimension      AS units_dimension,
    m.units_id,
    m.display        AS measurand_display,
    m.description,
    m.lower_limit,
    m.upper_limit,
    m.is_active,
    COALESCE(sc.conversions, '{}') AS specific_conversions
  FROM measurands m
  LEFT JOIN units u              ON u.units_id = m.units_id
  LEFT JOIN specific_conversions sc ON sc.measurand = m.measurand
 ORDER BY m.measurand;
