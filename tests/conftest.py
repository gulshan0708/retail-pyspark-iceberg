"""Shared pytest fixtures."""

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Spark runs Python UDFs in separate worker processes. They must use the same
# Python as the tests and be able to `import src`. These variables have to be
# set before the first SparkSession is created.
ROOT = Path(__file__).resolve().parents[1]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYTHONPATH"] = os.pathsep.join(
    filter(None, [str(ROOT), os.environ.get("PYTHONPATH")])
)


@pytest.fixture(scope="session")
def spark():
    """One SparkSession shared by every test in the run."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()