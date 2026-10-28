"""Iceberg loader: writes DataFrames to Iceberg tables and reads them
back across time (snapshots)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from src.load.base import BaseLoader
from datetime import datetime, timezone
import time
import contextlib
from typing import Iterator


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
            # Build the seconds value from integers: a float like 1717243200.123
            # can be off by a microsecond, which could pick the wrong snapshot.
            seconds = f"{timestamp_ms // 1000}.{timestamp_ms % 1000:03d}"
            return self.spark.sql(f"SELECT * FROM {fq_name} TIMESTAMP AS OF {seconds}")
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
        #java_table = self.spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        #    self.spark._jsparkSession, self._fq(table)
        #)
        java_table = self._java_table(table)
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






    # ---- table internals through Iceberg's Java API ----

    def _java_table(self, table: str):
        """The Iceberg `Table` object behind a table name (see _snapshots for why py4j)."""
        return self.spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            self.spark._jsparkSession, self._fq(table)
        )

    def location(self, table: str) -> str:
        """Where the table's files live, e.g. s3://warehouse/step10/customers."""
        return str(self._java_table(table).location())

    def snapshot_summary(self, table: str, snapshot_id: int | None = None) -> dict:
        """What a snapshot did: operation, files added/deleted, record totals.
        Defaults to the latest snapshot."""
        java_table = self._java_table(table)
        snapshot = (
            java_table.snapshot(snapshot_id) if snapshot_id is not None
            else java_table.currentSnapshot()
        )
        summary = {str(key): str(value) for key, value in snapshot.summary().items()}
        summary["operation"] = str(snapshot.operation())
        return summary

    def data_files(self, table: str) -> list[dict]:
        """Every data file in the current snapshot: path, record count, size, the
        partition spec it was written under, and how many delete files apply to it."""
        scan = self._java_table(table).newScan().planFiles()
        files = []
        try:
            iterator = scan.iterator()
            while iterator.hasNext():
                task = iterator.next()
                data_file = task.file()
                files.append({
                    "path": str(data_file.path()),
                    "records": data_file.recordCount(),
                    "bytes": data_file.fileSizeInBytes(),
                    "spec_id": task.spec().specId(),
                    "delete_files": task.deletes().size(),
                })
        finally:
            scan.close()  # the scan holds resources until closed
        return files

    def files_matching(self, table: str, column: str, value) -> int:
        """How many data files Iceberg would have to read for `column = value`. It
        decides from each file's min/max statistics, without reading any data."""
        expressions = self.spark._jvm.org.apache.iceberg.expressions.Expressions
        scan = self._java_table(table).newScan().filter(expressions.equal(column, value)).planFiles()
        count = 0
        try:
            iterator = scan.iterator()
            while iterator.hasNext():
                iterator.next()
                count += 1
        finally:
            scan.close()
        return count

    # ---- row-level changes ----

    def delete_where(self, table: str, condition: str) -> None:
        """Row-level DELETE. `condition` is SQL text we write ourselves, never user input."""
        self.spark.sql(f"DELETE FROM {self._fq(table)} WHERE {condition}")

    def update_where(self, table: str, assignments: str, condition: str) -> None:
        """Row-level UPDATE, e.g. update_where(t, "category = 'Toys & Games'", "category = 'Toys'")."""
        self.spark.sql(f"UPDATE {self._fq(table)} SET {assignments} WHERE {condition}")

    def set_write_mode(self, table: str, mode: str) -> None:
        """'copy-on-write' (default) rewrites whole files; 'merge-on-read' writes small
        delete files and leaves the data files alone. Needs table format version 2."""
        if mode not in ("copy-on-write", "merge-on-read"):
            raise ValueError(f"Unsupported write mode: {mode}")
        self.spark.sql(
            f"ALTER TABLE {self._fq(table)} SET TBLPROPERTIES ("
            f"'write.delete.mode'='{mode}', 'write.update.mode'='{mode}', 'write.merge.mode'='{mode}')"
        )

    # ---- table maintenance ----

    def _timestamp_literal(self, hours_ago: int = 0) -> str:
        """A SQL TIMESTAMP literal for `hours_ago` hours before now, in the session's own time zone."""
        stamp = self.spark.sql(
            f"SELECT date_format(current_timestamp() - INTERVAL {int(hours_ago)} HOURS, "
            "'yyyy-MM-dd HH:mm:ss.SSS')"
        ).first()[0]
        return f"TIMESTAMP '{stamp}'"

    def compact(
        self,
        table: str,
        strategy: str = "binpack",
        sort_order: str | None = None,
        options: dict[str, str] | None = None,
    ) -> dict:
        """rewrite_data_files: 'binpack' merges small files, 'sort' also orders the rows."""
        args = [f"table => '{table}'", f"strategy => '{strategy}'"]
        if sort_order:
            args.append(f"sort_order => '{sort_order}'")
        if options:
            pairs = ", ".join(f"'{key}', '{value}'" for key, value in options.items())
            args.append(f"options => map({pairs})")
        row = self.spark.sql(
            f"CALL {self.catalog_name}.system.rewrite_data_files({', '.join(args)})"
        ).first()
        return row.asDict()

    def expire_snapshots(self, table: str, retain_last: int = 1) -> dict:
        """Forgets old snapshots (and deletes files only they used). Time travel to
        an expired snapshot is no longer possible."""
        row = self.spark.sql(
            f"CALL {self.catalog_name}.system.expire_snapshots("
            f"table => '{table}', older_than => {self._timestamp_literal()}, retain_last => {retain_last})"
        ).first()
        return row.asDict()

    def remove_orphan_files(
        self, table: str, dry_run: bool = True, older_than_hours: int = 24
    ) -> list[str]:
        """Finds (and with dry_run=False deletes) unreferenced files older than
        `older_than_hours`. Iceberg's SQL procedure refuses anything under 24 hours:
        a file that a running job has written but not yet committed looks exactly like
        an orphan, so deleting young files can corrupt a table."""
        rows = self.spark.sql(
            f"CALL {self.catalog_name}.system.remove_orphan_files("
            f"table => '{table}', older_than => {self._timestamp_literal(older_than_hours)}, "
            f"dry_run => {str(dry_run).lower()})"
        ).collect()
        return [row["orphan_file_location"] for row in rows]
    def force_remove_orphan_files(self, table: str) -> list[str]:
        """Deletes unreferenced files of ANY age through Iceberg's Java Action API, which
        has no 24-hour guard. Only use this when you know nothing else is writing to the
        table, such as in a local demo. Returns the deleted file locations."""
        actions = self.spark._jvm.org.apache.iceberg.spark.actions.SparkActions.get(
            self.spark._jsparkSession
        )
        now_ms = int(time.time() * 1000)
        result = actions.deleteOrphanFiles(self._java_table(table)).olderThan(now_ms).execute()
        deleted = []
        iterator = result.orphanFileLocations().iterator()
        while iterator.hasNext():
            deleted.append(str(iterator.next()))
        return deleted

    def set_sort_order(self, table: str, columns: str) -> None:
        """Sets the table's write order, e.g. set_sort_order(t, "country ASC NULLS LAST")."""
        self.spark.sql(f"ALTER TABLE {self._fq(table)} WRITE ORDERED BY {columns}")

    def metadata_location(self, table: str) -> str:
        """The table's current metadata JSON file: the entry point other engines
        (DuckDB, Trino, ...) use to read an Iceberg table."""
        return str(self._java_table(table).operations().current().metadataFileLocation())

    # ---- branches and write-audit-publish ----

    def enable_wap(self, table: str) -> None:
        """Lets writes be redirected to a branch (needed for spark.wap.branch to work)."""
        self.spark.sql(f"ALTER TABLE {self._fq(table)} SET TBLPROPERTIES ('write.wap.enabled'='true')")

    def create_branch(self, table: str, branch: str) -> None:
        """A branch is a named pointer to a snapshot, like a git branch. This creates
        (or resets) it at the table's current snapshot."""
        self.spark.sql(f"ALTER TABLE {self._fq(table)} CREATE OR REPLACE BRANCH `{branch}`")

    def drop_branch(self, table: str, branch: str) -> None:
        self.spark.sql(f"ALTER TABLE {self._fq(table)} DROP BRANCH IF EXISTS `{branch}`")

    @contextlib.contextmanager
    def wap_branch(self, branch: str) -> Iterator[None]:
        """While the `with` block runs, writes to a WAP-enabled table are committed to
        `branch` instead of main. Readers of main see nothing until the branch is published."""
        self.spark.conf.set("spark.wap.branch", branch)
        try:
            yield
        finally:
            self.spark.conf.unset("spark.wap.branch")

    def read_branch(self, table: str, branch: str) -> DataFrame:
        return self.spark.sql(f"SELECT * FROM {self._fq(table)} VERSION AS OF '{branch}'")

    def fast_forward_branch(self, table: str, branch: str, target: str = "main") -> None:
        """Publish: moves `target` (normally main) forward to where `branch` points. It
        only works if `target` hasn't moved since the branch was created."""
        self.spark.sql(
            f"CALL {self.catalog_name}.system.fast_forward("
            f"table => '{table}', branch => '{target}', to => '{branch}')"
        )

    # ---- incremental reads ----

    def read_incremental(
        self, table: str, from_snapshot_id: int, to_snapshot_id: int | None = None
    ) -> DataFrame:
        """Rows ADDED after `from_snapshot_id` (exclusive) up to `to_snapshot_id`
        (inclusive, default: the latest). Only append snapshots count: deletes,
        overwrites and compactions in between are ignored."""
        reader = self.spark.read.format("iceberg").option("start-snapshot-id", from_snapshot_id)
        if to_snapshot_id is not None:
            reader = reader.option("end-snapshot-id", to_snapshot_id)
        return reader.load(self._fq(table))