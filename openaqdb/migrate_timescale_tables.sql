CREATE OR REPLACE FUNCTION migrate_timescale_chunk(nme text) RETURNS text AS $$
DECLARE
table_name text;
sd date;
ed date;
stm text;
BEGIN

  SELECT range_start::date, range_end::date INTO sd, ed
  FROM timescaledb_information.chunks c
  WHERE chunk_name = nme;

  table_name := 'measurements_'||to_char(sd, 'YYYYMMDD')||to_char(ed, '_YYYYMMDD');

  stm := format('
-- update to allow us to alter
UPDATE _timescaledb_catalog.chunk
SET dropped = TRUE
WHERE table_name = ''%s'';

-- detach
ALTER TABLE _timescaledb_internal.%1$s
NO INHERIT public.measurements;

-- rename
ALTER TABLE _timescaledb_internal.%1$s
RENAME TO %2$s;

-- move to new schema
ALTER TABLE _timescaledb_internal.%2$s
SET SCHEMA _measurements_internal;

-- Attach to the native table with from/to values that correspond to check
ALTER TABLE public.measurements
ATTACH PARTITION _measurements_internal.%2$s
FOR VALUES FROM (''%3$s'') TO (''%4$s'');
', nme, table_name, sd, ed);
  EXECUTE stm;
  RETURN table_name;
  EXCEPTION WHEN OTHERS THEN
    --RAISE NOTICE 'ERROR:'||SQLERRM;
    RETURN 'ERROR ('||nme||'): '||SQLERRM;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migrate_timescale_chunk(
  chunk text
, table_name text
, new_table_name text
, new_partition_prefix text
, new_schema_name text
) RETURNS text AS $$
DECLARE
partition_name text;
sd date;
ed date;
stm text;
BEGIN

  SELECT range_start::date, range_end::date INTO sd, ed
  FROM timescaledb_information.chunks c
  WHERE chunk_name = chunk;

  partition_name := new_partition_prefix||to_char(sd, 'YYYYMMDD')||to_char(ed, '_YYYYMMDD');

  stm := format('
-- update to allow us to alter
UPDATE _timescaledb_catalog.chunk
SET dropped = TRUE
WHERE table_name = ''%s'';

-- detach
ALTER TABLE _timescaledb_internal.%1$s
NO INHERIT %5$s;

-- rename
ALTER TABLE _timescaledb_internal.%1$s
RENAME TO %2$s;

-- move to new schema
ALTER TABLE _timescaledb_internal.%2$s
SET SCHEMA %6$s;

-- Attach to the native table with from/to values that correspond to check
ALTER TABLE %7$s
ATTACH PARTITION %6$s.%2$s
FOR VALUES FROM (''%3$s'') TO (''%4$s'');
', chunk, partition_name, sd, ed, table_name, new_schema_name, new_table_name);
  --EXECUTE stm;
  RETURN stm;
  --RETURN table_name;
  --EXCEPTION WHEN OTHERS THEN
    --RAISE NOTICE 'ERROR:'||SQLERRM;
    --RETURN 'ERROR ('||nme||'): '||SQLERRM;
END;
$$ LANGUAGE plpgsql;

SELECT migrate_timescale_chunk(
'_hyper_41_2152_chunk'
, 'public.hourly_rollups'
, 'public.hourly_rollups_native'
, 'hourly_rollups_'
, '_measurements_internal'
);

BEGIN;
SELECT migrate_timescale_chunk(table_name)
FROM _timescaledb_catalog.chunk
WHERE NOT dropped
AND table_name ~*'_1_';
COMMIT;
