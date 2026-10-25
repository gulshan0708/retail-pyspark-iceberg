"""Iceberg loader: writes DataFrames to Iceberg tables and reads them
back across time (snapshots)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from src.load.base import BaseLoader
from datetime import datetime, timezone


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


    def read_as_of(
        self,
        table: str,
        snapshot_id: int | None = None,
        timestamp_ms: int | None = None,
    ) -> DataFrame:
        """Reads the table as it was at a snapshot id or a timestamp (epoch
        milliseconds). Uses Spark's SQL time-travel syntax: Spark loads the
        real table first and then pins the snapshot, which avoids the
        `snapshot_id_<id>` pseudo-table lookup that this catalog server rejects."""
        fq_name = self._fq(table)
        if snapshot_id is not None:
            return self.spark.sql(f"SELECT * FROM {fq_name} VERSION AS OF {snapshot_id}")
        if timestamp_ms is not None:
            # TIMESTAMP AS OF accepts a number of seconds since 1970
            return self.spark.sql(f"SELECT * FROM {fq_name} TIMESTAMP AS OF {timestamp_ms / 1000}")
        return self.spark.table(fq_name)

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
 
    # ---- time travel / metadata ----
    def _snapshots(self, table: str) -> list[dict]:
        """All snapshots of a table, oldest first, read through Iceberg's Java API.

        `spark._jvm` is PySpark's bridge (py4j) to the Java side, and
        `spark._jsparkSession` is the Java-side session. The leading underscore
        marks them as internal, but this is the only way to get an Iceberg
        `Table` object from PySpark. Loading it this way uses the normal table
        lookup, so it avoids the REST catalog's metadata-table problem."""
        java_table = self.spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            self.spark._jsparkSession, self._fq(table)
        )
        snapshots = []
        iterator = java_table.snapshots().iterator()  # a Java iterator, not a Python one
        while iterator.hasNext():
            snapshot = iterator.next()
            snapshots.append({
                "committed_at": datetime.fromtimestamp(
                    snapshot.timestampMillis() / 1000, tz=timezone.utc
                ),
                "snapshot_id": snapshot.snapshotId(),
                "parent_id": snapshot.parentId(),  # None for the first snapshot
                "operation": snapshot.operation(),
            })
        return snapshots

    def snapshot_ids(self, table: str) -> list[int]:
        """Snapshot ids, oldest first."""
        return [s["snapshot_id"] for s in self._snapshots(table)]

    def list_snapshots(self, table: str) -> DataFrame:
        """Snapshots as a small DataFrame, oldest first."""
        rows = [
            (s["committed_at"], s["snapshot_id"], s["parent_id"], s["operation"])
            for s in self._snapshots(table)
        ]
        return self.spark.createDataFrame(
            rows, "committed_at timestamp, snapshot_id bigint, parent_id bigint, operation string"
        )