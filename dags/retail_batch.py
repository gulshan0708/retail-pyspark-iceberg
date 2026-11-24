"""Retail batch: ingest >> clean >> aggregate >> load.

Each task runs one pipeline inside the Spark container with spark-submit and passes the batch
date. For a manual run, set the date in the run config, e.g. {"batch_date": "2024-07-01"}.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context
from retail_helpers import build_spark_command, resolve_batch_date, run_in_spark

PIPELINES = [
    ("ingest", "pipelines/pipeline1_ingest.py"),
    ("clean", "pipelines/pipeline2_clean.py"),
    ("aggregate", "pipelines/pipeline3_aggregate.py"),
    ("load", "pipelines/pipeline4_load.py"),
]


def run_pipeline(script: str) -> None:
    """The body of every task: work out the batch date, then run one pipeline in the Spark container."""
    context = get_current_context()
    batch_date = resolve_batch_date(
        conf=context["dag_run"].conf,
        logical_date=context.get("logical_date"),
        today=datetime.now(timezone.utc).date(),
    )
    print(f"Running {script} for batch date {batch_date}")
    run_in_spark(build_spark_command(script, batch_date))


with DAG(
    dag_id="retail_batch",
    description="Raw files to bronze, silver, gold and Postgres: one spark-submit per task",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # run only when triggered; scheduling comes in Step 3
    catchup=False,
    max_active_runs=1,      # the pipelines overwrite tables, so never run two at once
    default_args={"execution_timeout": timedelta(minutes=30)},
    doc_md=__doc__,         # shown on the DAG's page in the UI
    tags=["retail", "spark"],
) as dag:
    previous = None
    for task_id, script in PIPELINES:
        task = PythonOperator(
            task_id=task_id,
            python_callable=run_pipeline,
            op_kwargs={"script": script},
        )
        if previous is not None:
            previous >> task  # "previous must finish successfully before task starts"
        previous = task