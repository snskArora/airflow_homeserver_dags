import random
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

def choose_branch():
    choice = random.choice(['option_a', 'option_b'])
    return choice

with DAG(
    'test_branch_and_bash',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = BashOperator(
        task_id='start_bash_task',
        bash_command='echo "Starting test pipeline..."',
    )

    branch_task = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=choose_branch,
    )

    path_a = BashOperator(
        task_id='option_a',
        bash_command='echo "Selected Path A"',
    )

    path_b = BashOperator(
        task_id='option_b',
        bash_command='echo "Selected Path B"',
    )

    end = BashOperator(
        task_id='join_node',
        bash_command='echo "Pipeline complete"',
        trigger_rule='none_failed_min_one_success',
    )

    start >> branch_task >> [path_a, path_b] >> end
