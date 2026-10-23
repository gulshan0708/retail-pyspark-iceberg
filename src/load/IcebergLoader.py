"""Iceberg loader: writes DataFrames to Iceberg tables and reads them
back across time (snapshots)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from src.load.base import BaseLoader


class IcebergLoader(BaseLoader):
    """All table arguments are "layer.table" strings, e.g. "bronze.orders".
    The catalog prefix (default "demo") is added internally."""

    def __init__(self, spark: SparkSession, catalog_name: str = "demo"):
        self.spark = spark
        self.catalog_name = catalog_name

    # ---- helpers (leading underscore = internal use) ----

    def _fq(self, table: str) -> str:
        """Fully-qualified name: 'bronze.orders' -> 'demo.bronze.orders'."""
        return f"{self.catalog_name}.{table}"

    def _ensure_namespace(self, table: str) -> None:
        namespace = table.rsplit(".", 1)[0]
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{namespace}")

    def _table_exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(self._fq(table))

    def _write(
        self,
        df: DataFrame,
        table: str,
        partition_exprs: list[Column] | None,
        mode: str,
    ) -> None:
        self._ensure_namespace(table)
        writer = df.writeTo(self._fq(table)).tableProperty("format-version", "2")
        if partition_exprs:
            # `*` unpacks the list into separate arguments
            writer = writer.partitionedBy(*partition_exprs)

        if mode == "overwrite" or not self._table_exists(table):
            writer.createOrReplace()      # first run, or full refresh
        elif mode == "append":
            writer.append()
        elif mode == "overwrite_partitions":
            writer.overwritePartitions()  # replaces only partitions present in df
        else:
            raise ValueError(f"Unsupported write mode: {mode}")

    # ---- writes ----

    def load(self, df: DataFrame, target: str, mode: str = "append") -> None:
        """Generic write that satisfies the BaseLoader contract."""
        self._write(df, target, None, mode)

    def write_bronze(
        self,
        df: DataFrame,
        table: str,
        partition_col: str | None = None,
        mode: str = "overwrite",
    ) -> None:
        """Bronze write. If partition_col is a date column, the table is
        partitioned with days(partition_col) (hidden partitioning).
        Default mode is a full refresh because our raw sources are small
        full snapshots."""
        exprs = [F.days(F.col(partition_col))] if partition_col else None
        self._write(df, table, exprs, mode)

    def write_silver(
        self,
        df: DataFrame,
        table: str,
        partition_exprs: list[Column] | None = None,
        mode: str = "overwrite",
    ) -> None:
        """Silver write with caller-chosen partition transforms, e.g.
        [F.months("order_date")] or [F.bucket(8, "customer_id")]."""
        self._write(df, table, partition_exprs, mode)

    def merge_gold(self, df: DataFrame, table: str, key_columns: list[str]) -> None:
        """Idempotent upsert. Source rows must be unique on key_columns,
        otherwise Spark raises a cardinality-violation error."""

        if not key_columns:
            raise ValueError("key_columns must not be empty")

        missing_columns = set(key_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(
                f"Missing key columns in source: {sorted(missing_columns)}"
            )


        self._ensure_namespace(table)
        if not self._table_exists(table):
            self._write(df, table, None, "overwrite")
            return

        df.createOrReplaceTempView("merge_source")
        try:
            on_clause = " AND ".join(f"t.{c} <=> s.{c}" for c in key_columns)
            self.spark.sql(
                f"""
                MERGE INTO {self._fq(table)} t
                USING merge_source s
                ON {on_clause}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        finally:
            self.spark.catalog.dropTempView("merge_source")

    # ---- time travel / metadata ----

    def list_snapshots(self, table: str) -> DataFrame:
        """Iceberg exposes metadata as hidden tables: `<table>.snapshots`
        lists every commit. Also available: .history, .files."""
        return self.spark.table(f"{self._fq(table)}.snapshots").select(
            "committed_at", "snapshot_id", "parent_id", "operation"
        )

    def read_as_of(
        self,
        table: str,
        snapshot_id: int | None = None,
        timestamp_ms: int | None = None,
    ) -> DataFrame:
        """Read the table as it was at a snapshot or timestamp (epoch
        milliseconds)."""
        reader = self.spark.read
        if snapshot_id is not None:
            reader = reader.option("snapshot-id", snapshot_id)
        elif timestamp_ms is not None:
            reader = reader.option("as-of-timestamp", timestamp_ms)
        return reader.format("iceberg").load(self._fq(table))

    def time_travel_report(
        self, table: str, since_snapshot_id: int
    ) -> tuple[DataFrame, DataFrame]:
        """What changed since an earlier snapshot? Returns (added, removed).
        exceptAll = rows in the left DataFrame missing from the right,
        keeping duplicates. An updated row appears as one removed + one added."""
        current = self.spark.table(self._fq(table))
        previous = self.read_as_of(table, snapshot_id=since_snapshot_id)
        return current.exceptAll(previous), previous.exceptAll(current)

    def read_table(self, table: str) -> DataFrame:
        """Reads an Iceberg table, e.g. read_table("bronze.orders"). 
        The catalog prefix is added here, so pipelines never hardcode it."""
        return self.spark.table(self._fq(table))