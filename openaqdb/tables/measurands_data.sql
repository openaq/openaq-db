INSERT INTO units (units, display, dimension) VALUES
  ('ug/m3', 'µg/m³', 'mass_concentration'),
  ('mg/m3', 'mg/m³', 'mass_concentration'),
  ('ng/m3', 'ng/m³', 'mass_concentration'),
  ('ppb',   'ppb',   'mixing_ratio'),
  ('ppm',   'ppm',   'mixing_ratio'),
  ('c',     '°C',    'temperature'),
  ('f',     '°F',    'temperature'),
  ('hpa',   'hPa',   'pressure'),
  ('kpa',   'kPa',   'pressure'),
  ('%',     '%',     'relative_humidity'),
  ('m/s',   'm/s',   'wind_speed'),
  ('km/h',  'km/h',  'wind_speed'),
  ('deg',   '°',     'wind_direction'),
  ('particles/cm3', 'particles/cm³', 'particle_concentration'),
  ('umol/mol', 'µmol/mol', 'mixing_ratio'),
  ('nmol/mol', 'nmol/mol', 'mixing_ratio'),
  ('pmol/mol', 'pmol/mol', 'mixing_ratio')
  ON CONFLICT DO NOTHING;

INSERT INTO unit_aliases (alias, units_id) VALUES
  ('µg/m³', (SELECT units_id FROM units WHERE units = 'ug/m3')),
  ('ugm3',  (SELECT units_id FROM units WHERE units = 'ug/m3')),
  ('mb',    (SELECT units_id FROM units WHERE units = 'hpa')),
  ('mbar',  (SELECT units_id FROM units WHERE units = 'hpa')),
  ('C',     (SELECT units_id FROM units WHERE units = 'c')),
  ('ug/m2',     (SELECT units_id FROM units WHERE units = 'ug/m3')),
  ('µmol/mol', (SELECT units_id FROM units WHERE units = 'umol/mol')),
  ('μmol/mol', (SELECT units_id FROM units WHERE units = 'umol/mol')),  -- note: different μ character!
  ('deg_c',     (SELECT units_id FROM units WHERE units = 'c')),
  ('particles/cm³',     (SELECT units_id FROM units WHERE units = 'particles/cm3')),
  ('°C',    (SELECT units_id FROM units WHERE units = 'c'))
  ON CONFLICT DO NOTHING;

-- =============================================================================
-- UNIT CONVERSIONS
-- Uses units.units_id via subqueries for clarity and referential safety.
-- Formula: converted_value = value * factor + offset
-- =============================================================================
INSERT INTO unit_conversions (from_units_id, to_units_id, factor, intercept) VALUES
-- ---------------------------------------------------------------------------
-- Mass concentration: ng/m3 ↔ ug/m3 ↔ mg/m3
-- ---------------------------------------------------------------------------
((SELECT units_id FROM units WHERE units = 'ug/m3'),
 (SELECT units_id FROM units WHERE units = 'mg/m3'), 0.001, 0),
((SELECT units_id FROM units WHERE units = 'mg/m3'),
 (SELECT units_id FROM units WHERE units = 'ug/m3'), 1000, 0),
((SELECT units_id FROM units WHERE units = 'ng/m3'),
 (SELECT units_id FROM units WHERE units = 'ug/m3'), 0.001, 0),
((SELECT units_id FROM units WHERE units = 'ug/m3'),
 (SELECT units_id FROM units WHERE units = 'ng/m3'), 1000, 0),
((SELECT units_id FROM units WHERE units = 'ng/m3'),
 (SELECT units_id FROM units WHERE units = 'mg/m3'), 0.000001, 0),
((SELECT units_id FROM units WHERE units = 'mg/m3'),
 (SELECT units_id FROM units WHERE units = 'ng/m3'), 1000000, 0),
-- ---------------------------------------------------------------------------
-- Mixing ratio: ppb ↔ ppm
-- ---------------------------------------------------------------------------
((SELECT units_id FROM units WHERE units = 'ppb'),
 (SELECT units_id FROM units WHERE units = 'ppm'), 0.001, 0),
((SELECT units_id FROM units WHERE units = 'ppm'),
 (SELECT units_id FROM units WHERE units = 'ppb'), 1000, 0),
-- ---------------------------------------------------------------------------
-- Mixing ratio: mol-fraction forms (1:1 with ppm/ppb/ppt)
-- µmol/mol = ppm, nmol/mol = ppb, pmol/mol = ppt
-- ---------------------------------------------------------------------------
((SELECT units_id FROM units WHERE units = 'umol/mol'),
 (SELECT units_id FROM units WHERE units = 'ppm'), 1, 0),
((SELECT units_id FROM units WHERE units = 'ppm'),
 (SELECT units_id FROM units WHERE units = 'umol/mol'), 1, 0),
((SELECT units_id FROM units WHERE units = 'nmol/mol'),
 (SELECT units_id FROM units WHERE units = 'ppb'), 1, 0),
((SELECT units_id FROM units WHERE units = 'ppb'),
 (SELECT units_id FROM units WHERE units = 'nmol/mol'), 1, 0),
-- Cross conversions among mol-fraction forms
((SELECT units_id FROM units WHERE units = 'umol/mol'),
 (SELECT units_id FROM units WHERE units = 'nmol/mol'), 1000, 0),
((SELECT units_id FROM units WHERE units = 'nmol/mol'),
 (SELECT units_id FROM units WHERE units = 'umol/mol'), 0.001, 0),
((SELECT units_id FROM units WHERE units = 'pmol/mol'),
 (SELECT units_id FROM units WHERE units = 'nmol/mol'), 0.001, 0),
((SELECT units_id FROM units WHERE units = 'nmol/mol'),
 (SELECT units_id FROM units WHERE units = 'pmol/mol'), 1000, 0),
((SELECT units_id FROM units WHERE units = 'pmol/mol'),
 (SELECT units_id FROM units WHERE units = 'umol/mol'), 0.000001, 0),
((SELECT units_id FROM units WHERE units = 'umol/mol'),
 (SELECT units_id FROM units WHERE units = 'pmol/mol'), 1000000, 0),
-- ---------------------------------------------------------------------------
-- Temperature: c ↔ f
-- ---------------------------------------------------------------------------
((SELECT units_id FROM units WHERE units = 'f'),
 (SELECT units_id FROM units WHERE units = 'c'), 0.5555555555555556, -17.77777777777778),
((SELECT units_id FROM units WHERE units = 'c'),
 (SELECT units_id FROM units WHERE units = 'f'), 1.8, 32),
-- ---------------------------------------------------------------------------
-- Pressure: hpa ↔ kpa
-- (mb, mbar are aliases of hpa in unit_aliases; no conversion row needed)
-- ---------------------------------------------------------------------------
((SELECT units_id FROM units WHERE units = 'kpa'),
 (SELECT units_id FROM units WHERE units = 'hpa'), 10, 0),
((SELECT units_id FROM units WHERE units = 'hpa'),
 (SELECT units_id FROM units WHERE units = 'kpa'), 0.1, 0),
-- ---------------------------------------------------------------------------
-- Wind speed: km/h ↔ m/s
-- ---------------------------------------------------------------------------
((SELECT units_id FROM units WHERE units = 'km/h'),
 (SELECT units_id FROM units WHERE units = 'm/s'), 0.2777777777777778, 0),
((SELECT units_id FROM units WHERE units = 'm/s'),
 (SELECT units_id FROM units WHERE units = 'km/h'), 3.6, 0)
ON CONFLICT DO NOTHING;


--   -- converting between the mixing ratios and mass concentrations
--   -- these are currently just for testing our process
-- -- =============================================================================
-- -- Measurand-specific mixing-ratio ↔ mass-concentration conversions
-- -- Reference conditions: 25°C, 1 atm (US EPA standard)
-- -- Formula: factor = MW / 24.45 (L/mol at STP)
-- -- =============================================================================

-- -- ppb → ug/m3
-- INSERT INTO unit_conversions (from_units_id, to_units_id, factor, intercept, measurand) VALUES
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 1.2270, 0, 'no'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 1.8814, 0, 'no2'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 1.9632, 0, 'o3'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 2.6204, 0, 'so2'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 1.1456, 0, 'co'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 0.6560, 0, 'ch4'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 0.6965, 0, 'nh3'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 1.3939, 0, 'h2s'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 1.2282, 0, 'hcho'),
-- ((SELECT units_id FROM units WHERE units = 'ppb'),
--  (SELECT units_id FROM units WHERE units = 'ug/m3'), 3.1947, 0, 'bc6h6');

-- -- ug/m3 → ppb (inverse)
-- INSERT INTO unit_conversions (from_units_id, to_units_id, factor, intercept, measurand) VALUES
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/1.2270, 0, 'no'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/1.8814, 0, 'no2'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/1.9632, 0, 'o3'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/2.6204, 0, 'so2'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/1.1456, 0, 'co'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/0.6560, 0, 'ch4'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/0.6965, 0, 'nh3'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/1.3939, 0, 'h2s'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/1.2282, 0, 'hcho'),
-- ((SELECT units_id FROM units WHERE units = 'ug/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppb'), 1.0/3.1947, 0, 'bc6h6');

-- -- ppm → mg/m3 (same factor as ppb → ug/m3, just scaled)
-- INSERT INTO unit_conversions (from_units_id, to_units_id, factor, intercept, measurand) VALUES
-- ((SELECT units_id FROM units WHERE units = 'ppm'),
--  (SELECT units_id FROM units WHERE units = 'mg/m3'), 1.1456, 0, 'co'),
-- ((SELECT units_id FROM units WHERE units = 'ppm'),
--  (SELECT units_id FROM units WHERE units = 'mg/m3'), 1.8004, 0, 'co2'),
-- ((SELECT units_id FROM units WHERE units = 'ppm'),
--  (SELECT units_id FROM units WHERE units = 'mg/m3'), 0.6560, 0, 'ch4');

-- -- mg/m3 → ppm (inverse)
-- INSERT INTO unit_conversions (from_units_id, to_units_id, factor, intercept, measurand) VALUES
-- ((SELECT units_id FROM units WHERE units = 'mg/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppm'), 1.0/1.1456, 0, 'co'),
-- ((SELECT units_id FROM units WHERE units = 'mg/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppm'), 1.0/1.8004, 0, 'co2'),
-- ((SELECT units_id FROM units WHERE units = 'mg/m3'),
--  (SELECT units_id FROM units WHERE units = 'ppm'), 1.0/0.6560, 0, 'ch4');

-- -- umol/mol → mg/m3 (same as ppm → mg/m3, for CO2 research networks)
-- INSERT INTO unit_conversions (from_units_id, to_units_id, factor, intercept, measurand) VALUES
-- ((SELECT units_id FROM units WHERE units = 'umol/mol'),
--  (SELECT units_id FROM units WHERE units = 'mg/m3'), 1.8004, 0, 'co2'),
-- ((SELECT units_id FROM units WHERE units = 'mg/m3'),
--  (SELECT units_id FROM units WHERE units = 'umol/mol'), 1.0/1.8004, 0, 'co2');
