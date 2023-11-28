# Testing schema
Consists of a few methods to make it easier to add and remove fake data for the purpose of testing. This can be installed with
```shell
## Assuming the use of the local database
## and from the current directory
psql --single-transaction -h 172.17.0.2 -p 5432 -U postgres -d postgres -f testing_schema.sql
```

# Timezone check
Adds fake data along with some `canary` data that is meant to help debug timezone issues. Queries are meant to illustrate potential problems with queries related to timezones. To install
```shell
## Assuming the use of the local database
## and from the current directory
psql -h 172.17.0.2 -p 5432 -U postgres -d postgres -f check_timezones.sql
```
