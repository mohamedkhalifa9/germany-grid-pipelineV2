SELECT DATE(timestamp) as day, SUM(value) as total_generation
FROM (
    SELECT timestamp, value FROM read_parquet('/opt/airflow/spark_data/silver-data/solar/*.parquet')
    UNION ALL
    SELECT timestamp, value FROM read_parquet('/opt/airflow/spark_data/silver-data/wind_onshore/*.parquet')
    UNION ALL
    SELECT timestamp, value FROM read_parquet('/opt/airflow/spark_data/silver-data/wind_offshore/*.parquet')
    UNION ALL
    SELECT timestamp, value FROM read_parquet('/opt/airflow/spark_data/silver-data/hydro/*.parquet')
    UNION ALL
    SELECT timestamp, value FROM read_parquet('/opt/airflow/spark_data/silver-data/biomass/*.parquet')
    UNION ALL
    SELECT timestamp, value FROM read_parquet('/opt/airflow/spark_data/silver-data/nuclear/*.parquet')
    UNION ALL
    SELECT timestamp, value FROM read_parquet('/opt/airflow/spark_data/silver-data/other_renewable/*.parquet')
)
GROUP BY day
ORDER BY day