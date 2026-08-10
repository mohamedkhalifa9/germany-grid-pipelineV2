SELECT DATE(timestamp) as DAY, avg(value) as 
DAILY_AVG 
 FROM 
 read_parquet('/opt/airflow/spark_data/silver-data/prices/*.parquet') 
 group by DATE(timestamp) ORDER BY DAY ASC