"""Fixtures for integration tests. These run INSIDE the Spark container (see
scripts/test_integration.sh), where the catalog, storage and Postgres are reachable."""

import pytest

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader

PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4"
SCRATCH = "it_scratch"  # a namespace used only by tests


@pytest.fixture(scope="session")
def spark_it():
    # spark-submit gets these packages from scripts/submit.sh. Here we start Spark
    # from Python, so they go into the session config instead.
    extra = {
        "spark.jars.packages": PACKAGES,
        "spark.sql.shuffle.partitions": "2",
        "spark.ui.enabled": "false",
    }
    with SparkSessionManager(app_name="integration-tests", extra_conf=extra) as spark:
        yield spark


@pytest.fixture
def loader(spark_it):
    return IcebergLoader(spark_it)


def _drop_scratch_tables(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS demo.{SCRATCH}")
    for row in spark.sql(f"SHOW TABLES IN demo.{SCRATCH}").collect():
        spark.sql(f"DROP TABLE IF EXISTS demo.{SCRATCH}.{row.tableName}")


@pytest.fixture(scope="session", autouse=True)
def scratch_namespace(spark_it):
    """Starts every run from an empty scratch namespace and removes it afterwards."""
    _drop_scratch_tables(spark_it)
    yield
    _drop_scratch_tables(spark_it)
    spark_it.sql(f"DROP NAMESPACE IF EXISTS demo.{SCRATCH}")