-- Stage table matches the SOURCE schema (what's actually in the CSV).
-- List only the columns present in the dump; local-only columns
-- (spatial_match_tolerance, etc.) stay out of stage.
CREATE TEMP TABLE stage_providers (
    providers_id  int,
    label text,
  description text,
  is_public boolean,
    source_name   text,
    export_prefix text,
  license text,
    metadata      jsonb,
  owner_entities_id int,
    is_active     boolean,
  spatial_match_tolerance double precision
);

\COPY stage_providers FROM PSTDIN WITH (FORMAT CSV, HEADER)

INSERT INTO public.providers AS p (
    providers_id,
    label,
  description,
  is_public,
  license,
  owner_entities_id,
    source_name,
    export_prefix,
    metadata,
    is_active,
  spatial_match_tolerance
)
SELECT
    providers_id,
  label, description,is_public,license,owner_entities_id,
    source_name,
    export_prefix,
    metadata,
    is_active, spatial_match_tolerance
FROM stage_providers
ON CONFLICT (providers_id) DO UPDATE
SET
    source_name   = EXCLUDED.source_name,
    export_prefix = EXCLUDED.export_prefix,
    metadata      = EXCLUDED.metadata,
    is_active     = EXCLUDED.is_active;
-- spatial_match_tolerance is untouched -- keeps its local value on updates,
-- and takes the column default on new inserts.
