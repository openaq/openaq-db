SET search_path TO public, rollups, _timescaledb_internal, timescaledb_information;

DROP VIEW IF EXISTS hypertables_info;

CREATE OR REPLACE VIEW hypertables_info AS
SELECT
    hypertable_schema as schema,
    hypertable_name as table,
    compression_enabled as compressed,
    total_chunks as chunks,
    number_compressed_chunks as c_chunks,
    pg_size_pretty(table_bytes) as table_size,
    pg_size_pretty(index_bytes) as index_size,
    pg_size_pretty(total_bytes) as total_size,
    pg_size_pretty(before_compression_table_bytes) as u_table_size,
    pg_size_pretty(before_compression_index_bytes) as u_index_size,
    pg_size_pretty(before_compression_total_bytes) as u_total_size,
    pg_size_pretty(after_compression_table_bytes) as c_table_size,
    pg_size_pretty(after_compression_index_bytes) as c_index_size,
    pg_size_pretty(after_compression_total_bytes) as c_total_size
FROM timescaledb_information.hypertables h
LEFT JOIN LATERAL
(SELECT * FROM hypertable_compression_stats(h.hypertable_name::regclass)) AS c ON true
LEFT JOIN LATERAL
(SELECT * FROM hypertable_detailed_size(h.hypertable_name::regclass))
AS t ON true;
