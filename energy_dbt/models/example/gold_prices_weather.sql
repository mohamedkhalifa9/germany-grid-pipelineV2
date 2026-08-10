SELECT Date(prices.timestamp) AS day, AVG(prices.value) as average_price, AVG(weather.temperature_2m_max) as average_temperature_2m_max, AVG(weather.temperature_2m_min) as average_temperature_2m_min, AVG(weather.shortwave_radiation_sum) as average_shortwave_radiation_sum, AVG(weather.wind_speed_10m_max) as average_wind_speed_10m_max
from read_parquet('/opt/airflow/spark_data/silver-data/prices/*.parquet') as prices
join read_parquet('/opt/airflow/spark_data/raw-data/weather/*.parquet') as weather
on Date(prices.timestamp) = Date(weather.date)  group by Date(prices.timestamp)