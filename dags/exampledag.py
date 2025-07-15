from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_task():
    print("✅ Hello from a clean, working Airflow DAG!")

with DAG(
    dag_id="hello_world_clean_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"]
) as dag:
    task = PythonOperator(
        task_id="say_hello",
        python_callable=hello_task
    )
