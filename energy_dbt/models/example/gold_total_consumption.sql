SELECT 
    DATE(timestamp) as day,
    SUM(value) as total_consumption 
FROM read_parquet('/opt/airflow/spark_data/silver-data/consumption/*.parquet') 
GROUP BY day 
ORDER BY day ASC