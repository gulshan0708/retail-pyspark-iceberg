"""Compaction, sort order with file pruning, and orphan files.
Run: scripts/submit.sh scripts/iceberg_maintenance.py
"""

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader


def describe_files(loader, table) -> str:
    files = loader.data_files(table)
    return f"{len(files)} file(s), {sum(f['bytes'] for f in files):,} bytes"


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        loader = IcebergLoader(spark)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.step10")
        for name in ("lines_files", "lines_sorted", "orphan_demo"):
            spark.sql(f"DROP TABLE IF EXISTS demo.step10.{name}")
        lines = loader.read_table("silver.order_lines")

        print("=== 1. Compaction: many small files become one")
        table = "step10.lines_files"
        loader.write_silver(lines.repartition(12), table)  # 12 tasks, so 12 small files
        print("before:", describe_files(loader, table))
        print("result:", loader.compact(table, options={"min-input-files": "2"}))
        print("after: ", describe_files(loader, table), "| snapshot operation:",
              loader.snapshot_summary(table)["operation"])

        print("\n=== 2. Sort order: how many files must a filter on country = 'US' read?")
        table = "step10.lines_sorted"
        loader.write_silver(lines.repartition(12), table)  # every file holds rows of every country
        total_bytes = sum(f["bytes"] for f in loader.data_files(table))
        print("before sorting:", describe_files(loader, table),
              "| files to read for country = 'US':", loader.files_matching(table, "country", "US"))

        order = "country ASC NULLS LAST"
        loader.set_sort_order(table, order)
        loader.compact(
            table,
            strategy="sort",
            sort_order=order,
            options={"rewrite-all": "true", "target-file-size-bytes": str(max(total_bytes // 6, 1024))},
        )
        print("after sorting: ", describe_files(loader, table),
              "| files to read for country = 'US':", loader.files_matching(table, "country", "US"))

        print("\n=== 3. Orphan files: drop a table without purging, then recreate it")
        table = "step10.orphan_demo"
        customers = loader.read_table("bronze.customers").limit(50)
        loader.write_bronze(customers, table)
        print("location:", loader.location(table))
        spark.sql(f"DROP TABLE demo.{table}")  # removes the catalog entry; the files stay (assumed)
        loader.write_bronze(customers, table)  # same location, brand-new table

        found = loader.remove_orphan_files(table, dry_run=True)  # the safe procedure: files older than 24 hours
        print(f"safe dry run (files older than 24 hours): {len(found)} orphan(s). "
              "Everything here is minutes old, so the safety rule protects it.")
        deleted = loader.force_remove_orphan_files(table)
        print(f"force-removed {len(deleted)} orphan file(s):", deleted[:3])
        print("orphans left:", len(loader.force_remove_orphan_files(table)),
              "| rows still readable:", loader.read_table(table).count())


if __name__ == "__main__":
    main()