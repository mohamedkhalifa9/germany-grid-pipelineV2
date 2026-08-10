from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import requests
from datetime import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

SMARD_CODES = {
    "4169": "prices",
    "4068": "solar",
    "1225": "wind_offshore",
    "4067": "wind_onshore",
    "1226": "hydro",
    "4066": "biomass",
    "1224": "nuclear",
    "1228": "other_renewable",
    "410": "consumption",
    "4359": "residual_load"
}
def get_smard_data(code, **context):
    execution_date = context['logical_date']
    index_url = f"https://www.smard.de/app/chart_data/{code}/DE/index_hour.json"
    index = requests.get(index_url).json()
    timestamps = index["timestamps"]
    
    # Find the timestamp closest to execution_date
    execution_ms = int(execution_date.timestamp() * 1000)
    week_timestamp = max(t for t in timestamps if t <= execution_ms)
    return week_timestamp


def upload_to_raw_data(ti, code):
    latest_timestamp = ti.xcom_pull(task_ids=f"fetch_smard_data_{code}")
    prefix = SMARD_CODES[code]
    data_url = f"https://www.smard.de/app/chart_data/{code}/DE/{code}_DE_hour_{latest_timestamp}.json"
    data = requests.get(data_url).json()
    
    output_dir = f"/opt/airflow/spark_data/raw-data/{prefix}"
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = f"{output_dir}/{latest_timestamp}.json"
    if os.path.exists(output_path):
        return f"Already exists, skipping upload"
    with open(output_path, "w") as f:
        json.dump(data, f)
    
    return f"Saved to {output_path}"
def transform_silver(energy_type):
    spark = SparkSession.builder \
    .appName("transform_silver") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()
   
    s3_input = f"/opt/airflow/spark_data/raw-data/{energy_type}/*.json"
    s3_output = f"/opt/airflow/spark_data/silver-data/{energy_type}"
    df = spark.read.json(s3_input)
    df = df.select(F.explode(df["series"]).alias("data_point"))
    df = df.select(
        df["data_point"][0].alias("timestamp_ms"),
        df["data_point"][1].alias("value")
    )
    df = df.withColumn("timestamp", (F.col("timestamp_ms") / 1000).cast("timestamp"))
    df = df.drop("timestamp_ms")
    df = df.dropna(subset=["value"])
    print(df)
    df.write.mode("overwrite").parquet(s3_output)
    spark.stop()

with DAG(
    dag_id="smard_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    max_active_tasks=3
) as dag:
    
    prices_transform_task = None
    total_consumption_transform_task = None
    transform_tasks = []
    
    for code, prefix in SMARD_CODES.items():

        fetch_task = PythonOperator(
            task_id=f"fetch_smard_data_{code}",
            python_callable=get_smard_data,
            op_kwargs={'code': code}
        )
        upload_task = PythonOperator(
            task_id=f"upload_to_s3_{code}",
            python_callable=upload_to_raw_data,
            op_kwargs={'code': code}
        )
        transform_task = PythonOperator(
            task_id=f"transform_silver_{prefix}",
            python_callable=transform_silver,
            op_kwargs={'energy_type': prefix}
        )
        transform_tasks.append(transform_task)
        fetch_task >> upload_task >> transform_task
        
        if prefix == "prices":
            prices_transform_task = transform_task
        if prefix == "consumption":
            total_consumption_transform_task = transform_task
        

    dbt_task = BashOperator(
        task_id="dbt_run_gold_daily_prices",
        bash_command="cd /opt/airflow/energy_dbt && dbt run --select gold_daily_prices --profiles-dir /opt/airflow/energy_dbt"
    )
    total_consumption_dbt_task = BashOperator(
        task_id="dbt_run_gold_total_consumption",
        bash_command="cd /opt/airflow/energy_dbt && dbt run --select gold_total_consumption --profiles-dir /opt/airflow/energy_dbt"
    )
    total_generation_dbt_task = BashOperator(
        task_id="dbt_run_gold_total_generation",
        bash_command="cd /opt/airflow/energy_dbt && dbt run --select gold_total_generation --profiles-dir /opt/airflow/energy_dbt"
    )
    
    prices_transform_task >> dbt_task
    total_consumption_transform_task >> total_consumption_dbt_task
    transform_tasks >> total_generation_dbt_task
