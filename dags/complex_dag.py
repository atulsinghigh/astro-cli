from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

def extract():
    print("🔍 Extracting data...")
    return "data_123"  # This will be pushed to XCom

def transform(ti):
    extracted_data = ti.xcom_pull(task_ids='extract_task')
    transformed_data = f"{extracted_data}_transformed"
    print(f"🔧 Transformed Data: {transformed_data}")
    return transformed_data

def load(ti):
    transformed_data = ti.xcom_pull(task_ids='transform_task')
    print(f"🚚 Loading data: {transformed_data}")

with DAG(
    dag_id="etl_complex_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "xcom"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    # Define dependencies: Extract → Transform → Load
    extract_task >> transform_task >> load_task