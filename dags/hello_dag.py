from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def greet():
    print("👋 Hello from Airflow in Codespaces!")

with DAG(
    dag_id="hello_codespaces_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["demo"]
) as dag:
    task = PythonOperator(
        task_id="say_hello",
        python_callable=greet
    )
