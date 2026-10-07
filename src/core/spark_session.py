"""Wraps SparkSession creation/teardown as a context manager, and
provides an alternate constructor for building the session from
environment variables (the pattern this whole stack already uses via
Docker Compose env vars)."""

from __future__ import annotations
import os
from pyspark.sql import SparkSession


class SparkSessionManager:
    """Context manager guaranteeing `spark.stop()` runs on exit, even if
    an exception is raised inside the `with` block."""

    def __init__(self, app_name: str = "retail-pipeline", catalog_name: str = "demo"):
        self.app_name = app_name
        self.catalog_name = catalog_name
        self._spark: SparkSession | None = None

    def __enter__(self) -> SparkSession:
        self._spark = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.catalog_name}",
                    "org.apache.iceberg.spark.SparkCatalog")
            .getOrCreate()
        )
        return self._spark

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._spark is not None:
            self._spark.stop()
        # returning False/None re-raises any exception that occurred —
        # we only want to guarantee cleanup, not swallow errors.
        return False

    @classmethod
    def from_env(cls) -> "SparkSessionManager":
        """Alternate constructor — builds config from env vars instead of
        hardcoded arguments. This is the `@classmethod`-as-alternate-
        constructor pattern: same class, different way of assembling it."""
        app_name = os.environ.get("SPARK_APP_NAME", "retail-pipeline")
        catalog_name = os.environ.get("ICEBERG_CATALOG", "demo")
        return cls(app_name=app_name, catalog_name=catalog_name)