import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator

def get_weather_data(**context):
    execution_date = context['logical_date']
    execution_date = execution_date.strftime("%Y-%m-%d")

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "daily": ["temperature_2m_max", "temperature_2m_min", "shortwave_radiation_sum", "wind_speed_10m_max"],
        "models": "dwd_icon_seamless",
        "timezone": "Europe/Berlin",
        "start_date": f"{execution_date}",
        "end_date": f"{execution_date}"
    }
    responses = openmeteo.weather_api(url, params = params)
    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    daily = response.Daily()
    daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
    daily_shortwave_radiation_sum = daily.Variables(2).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(3).ValuesAsNumpy()

    
    daily_data = {
	"date": pd.date_range(
		start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
		end =  pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = daily.Interval()),
		inclusive = "left"
	).tz_convert(response.Timezone().decode())
    }

    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["shortwave_radiation_sum"] = daily_shortwave_radiation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max

    daily_dataframe = pd.DataFrame(data = daily_data)

    os.makedirs("/opt/airflow/spark_data/raw-data/weather", exist_ok=True)
    daily_dataframe.to_parquet(f"/opt/airflow/spark_data/raw-data/weather/{execution_date}.parquet")


with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    max_active_tasks=1
) as dag:
    get_weather_data_task = PythonOperator(
        task_id="get_weather_data",
        python_callable=get_weather_data

    )
    dbt_prices_weather_task = BashOperator(
    task_id="dbt_run_gold_prices_weather",
    bash_command="cd /opt/airflow/energy_dbt && dbt run --select gold_prices_weather --profiles-dir /opt/airflow/energy_dbt"
)

    get_weather_data_task >> dbt_prices_weather_task

    