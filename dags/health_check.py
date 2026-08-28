from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from Airflow!")
    return "Success"

def print_context(ds, **kwargs):
    print(f"Execution date is: {ds}")
    print(f"Dag run: {kwargs.get('dag_run')}")

with DAG(
    'test_health_check',
    default_args=default_args,
    description='A simple health check DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Fixed: Changed task_1_id to task_id
    task_1 = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello,
    )

    # Fixed: Changed task_task_id to task_id
    task_2 = PythonOperator(
        task_id='print_context_task',
        python_callable=print_context,
    )

    task_1 >> task_2
