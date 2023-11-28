CREATE OR REPLACE FUNCTION calculate_partition_stats() RETURNS timestamptz AS $$
WITH measurement_tables AS (
  SELECT data_table_partitions_id
  , ('"' || table_schema || '"."' || table_name || '"') AS table_name
  FROM data_table_partitions
), measurement_sizes AS (
  SELECT data_table_partitions_id
  , table_name
  , pg_table_size(table_name) as table_size
  , pg_indexes_size(table_name) as index_size
  FROM measurement_tables
), stats_inserted AS (
  INSERT INTO partitions_stats (
  data_table_partitions_id
  , table_size
  , index_size
  , row_count
  , calculated_on
  )
  SELECT data_table_partitions_id
  , table_size
  , index_size
  , row_count_estimate(table_name) as row_count
  , now() as calculated_on
  FROM measurement_sizes
  ON CONFLICT(data_table_partitions_id) DO UPDATE
  SET table_size = EXCLUDED.table_size
  , index_size = EXCLUDED.index_size
  , row_count = EXCLUDED.row_count
  , calculated_on = EXCLUDED.calculated_on
  RETURNING calculated_on
  ) SELECT MAX(calculated_on) FROM stats_inserted;
$$ LANGUAGE SQL;


CREATE OR REPLACE VIEW data_table_stats AS
SELECT t.table_schema||'.'||t.table_name as table_name
  , ROUND(AVG(table_size)) as table_size_avg
  , MAX(table_size) as table_size_max
  , MIN(table_size) as table_size_min
  , SUM(table_size) as table_size_total
  , PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY table_size) as table_size_med
  , ROUND(AVG(index_size)) as index_size_avg
  , MAX(index_size) as index_size_max
  , MIN(index_size) as index_size_min
  , SUM(index_size) as index_size_total
  , PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY index_size) as index_size_med
  , COUNT(table_size) as partitions_count
  , MIN(s.calculated_on) as calculated_on
	, MIN(p.start_date) as partition_start_date
	, MAX(p.end_date) as partition_end_date
  FROM partitions_stats s
  JOIN data_table_partitions p ON (s.data_table_partitions_id = p.data_table_partitions_id)
  JOIN data_tables t ON (p.data_tables_id = t.data_tables_id)
  WHERE table_size > 0
  GROUP BY 1;
