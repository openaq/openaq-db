# OpenAQ V2 Database

The OpenAQ V2 Database requires
- PostgreSQL v14+
- PostGIS v3.4+

All scripts in the [openaqdb/idempotent](openaqdb/idempotent/) directory are designed so that updates can be made in place by just rerunning the SQL over the top of an existing database. As there may be dependencies in the changes, each .sql script should be run in full within a transaction.

Scripts in [openaqdb/tables](openaqdb/tables) represent the actual table structure and any changes to the structure will require migrations to be created. It is recommended to keep the sql in these files to represent the final current state of the database so that it is easier to look at this repository to see the structure of the database.

Rather than tracking version to version migration files, it is recommended to manage the desired final SQL using a tool like [Migra](https://github.com/djrobstep/migra). To use, you can run the SQL scripts into a Docker database, run migra against the Docker database and the target database to get the migration script. Check the migration script before running to bring the target database in sync with the updated code.

# Local setup

```
## change names/passwords as needed
## from the repo root directory
docker compose up --build
## If you want the mock data added try
ADD_MOCK_DATA=true docker compose up --build
```

# Connecting to DB
```shell
## if using the defaults
psql -h localhost -U postgres -p 5777 -d openaq
```
