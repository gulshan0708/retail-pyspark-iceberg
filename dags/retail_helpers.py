"""Helpers for the retail DAG. Kept free of Airflow imports so they can be unit-tested anywhere."""

from __future__ import annotations

from datetime import date, datetime

SPARK_CONTAINER = "retail-spark"
PROJECT_DIR = "/home/iceberg/project"
# The same packages as scripts/submit.sh (the S3A connector and the Postgres JDBC driver)
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4"


def resolve_batch_date(conf: dict | None, logical_date: datetime | None, today: date) -> str:
    """The day a run processes. An explicit value in the run's config wins, then the run's
    logical date, then today. Anything that isn't a YYYY-MM-DD date raises ValueError."""
    requested = (conf or {}).get("batch_date")
    if requested:
        return date.fromisoformat(str(requested)).isoformat()
    if logical_date is not None:
        return logical_date.date().isoformat()
    return today.isoformat()


def build_spark_command(script: str, batch_date: str) -> list[str]:
    """The same spark-submit call that scripts/submit.sh makes."""
    return ["spark-submit", "--packages", SPARK_PACKAGES, script, "--batch-date", batch_date]


def run_in_spark(command: list[str]) -> None:
    """Runs a command inside the Spark container, like `docker exec`, streams its output into
    the task log, and raises if it exits with an error (which marks the Airflow task as failed)."""
    import docker  # imported here: only the Airflow image has it, and the unit tests don't need it

    client = docker.from_env()
    try:
        exec_id = client.api.exec_create(
            SPARK_CONTAINER,
            command,
            environment={"PYTHONPATH": PROJECT_DIR},
            workdir=PROJECT_DIR,
        )["Id"]
        for chunk in client.api.exec_start(exec_id, stream=True):
            print(chunk.decode("utf-8", errors="replace"), end="", flush=True)
        exit_code = client.api.exec_inspect(exec_id)["ExitCode"]
    finally:
        client.close()
    if exit_code != 0:
        raise RuntimeError(f"spark-submit failed with exit code {exit_code}")