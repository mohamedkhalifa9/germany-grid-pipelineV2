from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def task_one():
    print("Task 1 running")

def task_two():
    print("Task 2 running")

with DAG(
    dag_id="first_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(task_id="task_1", python_callable=task_one)
    t2 = PythonOperator(task_id="task_2", python_callable=task_two)

    t1 >> t2