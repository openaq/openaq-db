BEGIN;
\i idempotent/systeminfo.sql
\i idempotent/util_functions.sql
\i idempotent/views.sql
\i idempotent/update_rollups.sql
\i idempotent/static_summaries.sql
COMMIT;