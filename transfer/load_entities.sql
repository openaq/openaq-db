-- Stage table matches the SOURCE schema (columns present in the CSV).
CREATE TEMP TABLE stage_entities (
    entities_id  int,
    entity_type  text,
    full_name    text,
    added_on     timestamptz,
    added_by     int,
    modified_on  timestamptz,
    modified_by  int,
  ingest_id text,
    metadata     jsonb
);

\COPY stage_entities FROM PSTDIN WITH (FORMAT CSV, HEADER)

INSERT INTO public.entities AS e (
    entities_id,
    entity_type,
    full_name,
    added_on,
    added_by,
    modified_on,
    modified_by,
    metadata
)
SELECT
    entities_id,
    entity_type::entity_type,
    full_name,
    added_on,
    added_by,
    modified_on,
    modified_by,
    metadata
FROM stage_entities
ON CONFLICT (entities_id) DO UPDATE
SET
    entity_type = EXCLUDED.entity_type,
    full_name   = EXCLUDED.full_name,
    metadata    = EXCLUDED.metadata,
    modified_on = now(),
    modified_by = EXCLUDED.modified_by;

\echo Entities loaded:
SELECT
    (SELECT count(*) FROM stage_entities)  AS staged,
    (SELECT count(*) FROM public.entities) AS total;
