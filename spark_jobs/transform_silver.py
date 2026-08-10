from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime




def transform_silver(energy_type):
    spark = SparkSession.builder \
    .appName("transform_silver") \
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
    dag_id="transform_silver",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    for energy_type in ["prices", "solar", "wind_onshore", "wind_offshore", "hydro", "biomass", "nuclear", "other_renewable", "consumption", "residual_load"]:
        transform_task = PythonOperator(
            task_id=f"transform_silver_{energy_type}",
            python_callable=transform_silver,
            op_kwargs={'energy_type': energy_type}
        )