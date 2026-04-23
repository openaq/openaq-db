# Fetcher Schema

Database-driven scheduling and orchestration system for OpenAQ data adapter deployments.

## Purpose

The fetcher schema manages scheduling and configuration for data adapter deployments. Schedule definitions, adapter assignments, and execution metadata live in the database, enabling runtime configuration changes without application redeployment.

## Design Goals

1. **Database-driven configuration**: All scheduling and deployment settings stored in database tables
2. **Flexible scheduling**: Cron expressions provide fine-grained control over execution timing
3. **Observability**: Track deployment history, adapter assignments, and execution metadata
4. **Queue-based execution**: fetchlogs serves as job queue for polling-based adapter execution

## Architecture

### Core Tables

- **handlers**: SQS queue definitions for routing deployment execution
- **adapter_clients**: Available adapter implementations (air4thai, clarity, etc.)
- **adapters**: Links providers to adapter clients with provider-specific configuration
- **deployments**: Scheduling definitions with cron expressions and temporal offsets
- **deployment_adapters**: Many-to-many junction linking deployments to adapters

### Execution Flow

1. **pg_cron** runs `queue_deployments()` every minute
2. Function evaluates cron schedules for active deployments
3. Ready deployments (with adapters assigned) are inserted into `public.fetchlogs` as scheduled jobs
4. **Adapter application** polls fetchlogs for new jobs
5. Application executes adapters, creates file, uploads to S3
6. **S3 trigger** invokes Lambda to upsert file metadata to fetchlogs
7. **Ingest application** processes files and updates fetchlogs metadata

### Key Design Choices

**Cron expressions**: Standard 5-field format (minute hour day month weekday) with custom validation functions to ensure correctness before storage.

**fetchlogs as queue**: The `public.fetchlogs` table serves as both execution queue and audit log. The `scheduled_datetime` field marks queued jobs; `loaded_datetime` and `completed_datetime` track progress through the pipeline.

**Unique keys for idempotency**: Fetchlog keys use format `YYYY-MM-DD/prefix/prefix-YYYYMMDDHH24MI`. Each deployment+time combination produces exactly one job via ON CONFLICT DO NOTHING constraint.

**Temporal offsets**: The `temporal_offset` field (in hours) tells adapters to fetch data from N hours in the past, accommodating data sources that publish with delay.

**Two-function design**: `get_ready_deployments()` queries which deployments should run (read-only); `queue_deployments()` inserts them into fetchlogs. The separation enables inspection without side effects.

**Adapter filtering**: Deployments without assigned adapters appear in `get_ready_deployments()` for visibility but are not queued by `queue_deployments()`.

## Usage

### Query ready deployments (inspection/testing)
```sql
-- See what's ready to run now
SELECT * FROM fetcher.get_ready_deployments();

-- Check what would run at specific time
SELECT * FROM fetcher.get_ready_deployments('2026-01-27 14:30:00');
```

### Queue deployments (production)
```sql
-- Manually queue (typically called by pg_cron)
SELECT fetcher.queue_deployments();
```

### Add a new deployment
```sql
-- Create deployment running every 15 minutes
INSERT INTO fetcher.deployments (
  handlers_id, label, filename_prefix, schedule, temporal_offset
) VALUES (
  1, 'new-source', 'newsource', '*/15 * * * *', 0
);

-- Link adapters to deployment
INSERT INTO fetcher.deployment_adapters (deployments_id, adapters_id)
VALUES (10, 42);
```

### Monitor deployment health
```sql
-- Find deployments not run recently
SELECT label, schedule, last_deployed_datetime
FROM fetcher.deployments
WHERE is_active
AND last_deployed_datetime < now() - interval '2 hours';
```

## Files

- **scheduler.sql**: Cron expression validation and evaluation functions
- **deployments.sql**: Tables, domain types, and scheduling functions
