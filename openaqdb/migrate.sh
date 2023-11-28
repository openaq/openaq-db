


# Get the latest fetchlogs id from the restored database

PROD_URL=postgres://$DATABASE_READ_USER:$DATABASE_READ_PASSWORD@$DATABASE_HOST:$DATABASE_PORT/$DATABASE_DB
LOCAL_URL=postgres://$DATABASE_WRITE_USER:$DATABASE_WRITE_PASSWORD@localhost:5432/$DATABASE_DB

    # now copy the last known fetchlogs id

# at this point the ingester should be running
# once the ingestor has started
# Now get a list of all the values since then
psql $PROD_URL -XAwtc "SELECT key FROM fetchlogs WHERE fetchlogs_id > $FETCHLOGS_ID" > '/tmp/fetchlog_keys.csv'
psql $LOCAL_URL \
     -c "BEGIN" \
     -c "DROP TABLE IF EXISTS fetchlog_keys" \
     -c "CREATE TABLE IF NOT EXISTS fetchlog_keys (key varchar)" \
     -c "COPY fetchlog_keys FROM '/tmp/fetchlog_keys.csv' WITH (FORMAT csv)" \
     -c "INSERT INTO fetchlogs (key) SELECT key FROM fetchlog_keys ON CONFLICT DO NOTHING" \
     -c "DROP TABLE IF EXISTS fetchlog_keys" \
     -c "COMMIT"
