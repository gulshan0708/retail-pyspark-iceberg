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

    def __init__(
        self,
        app_name: str = "retail-pipeline",
        catalog_name: str = "demo",
        extra_conf: dict[str, str] | None = None,
    ):
        self.app_name = app_name
        self.catalog_name = catalog_name
        # `x or {}` means: use x if it has a value, otherwise an empty dict.
        self.extra_conf = extra_conf or {}
        self._spark: SparkSession | None = None

    def _build_conf(self) -> dict[str, str]:
        """All Spark settings in one place. `os.environ.get(NAME, default)`
        reads an environment variable and falls back to the default if it
        isn't set, so the same code works in the container and in tests."""
        conf = {
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            f"spark.sql.catalog.{self.catalog_name}":
                "org.apache.iceberg.spark.SparkCatalog",
            # S3A: lets plain spark.read/write use s3a:// paths on our storage.
            # The `spark.hadoop.` prefix passes the setting on to Hadoop.
            "spark.hadoop.fs.s3a.endpoint":
                os.environ.get("S3_ENDPOINT", "http://minio:9000"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key":
                os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "spark.hadoop.fs.s3a.secret.key":
                os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        }
        conf.update(self.extra_conf)  # caller-supplied settings win
        return conf

    def __enter__(self) -> SparkSession:
        builder = SparkSession.builder.appName(self.app_name)
        # .items() gives (key, value) pairs; we apply each setting in turn.
        for key, value in self._build_conf().items():
            builder = builder.config(key, value)
        self._spark = builder.getOrCreate()
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