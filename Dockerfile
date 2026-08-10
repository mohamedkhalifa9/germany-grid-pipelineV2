FROM apache/airflow:3.0.2
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk
COPY energy_dbt/ /opt/airflow/energy_dbt/
USER airflow
RUN pip install requests pyspark dbt-duckdb openmeteo-requests 'click==8.2.1' requests-cache retry-requests 'urllib3<2'