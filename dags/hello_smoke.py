"""The smallest possible DAG: proves the scheduler, the DAG processor and task execution work."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello_smoke",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # run only when triggered by hand
    catchup=False,
    tags=["smoke"],
) as dag:
    BashOperator(task_id="say_hello", bash_command="echo hello from airflow")