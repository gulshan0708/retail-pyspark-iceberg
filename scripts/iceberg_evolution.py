"""Iceberg schema evolution and partition evolution, on scratch copies in `step10`.
Run: scripts/submit.sh scripts/iceberg_evolution.py
"""

from collections import Counter

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader


def specs_in_use(loader, table) -> dict:
    """How many data files were written under each partition spec id."""
    return dict(sorted(Counter(f["spec_id"] for f in loader.data_files(table)).items()))


def partition_fields(spark, table) -> list:
    """The table's CURRENT partition fields, read from DESCRIBE's 'Part N' rows."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED demo.{table}").collect()
    return [row.data_type for row in rows if str(row.col_name).startswith("Part ")]


def add_truncate_field(spark, table) -> str:
    """The argument order of truncate() in this DDL differs between docs, so try both."""
    for expression in ("truncate(category, 3)", "truncate(3, category)"):
        try:
            spark.sql(f"ALTER TABLE demo.{table} ADD PARTITION FIELD {expression}")
            return expression
        except Exception as exc:
            print(f"  '{expression}' was rejected ({type(exc).__name__})")
    raise RuntimeError("neither truncate argument order was accepted")


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        loader = IcebergLoader(spark)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.step10")
        for name in ("customers", "order_lines"):
            spark.sql(f"DROP TABLE IF EXISTS demo.step10.{name}")

        print("=== 1. Schema evolution on step10.customers (a copy of bronze.customers)")
        table = "step10.customers"
        loader.write_bronze(loader.read_table("bronze.customers"), table)
        snapshots_before = loader.snapshot_ids(table)
        print("columns:", loader.read_table(table).columns)

        spark.sql(f"ALTER TABLE demo.{table} ADD COLUMN loyalty_tier string")
        print("snapshots before/after ADD COLUMN:", len(snapshots_before), "/", len(loader.snapshot_ids(table)))
        print("loyalty_tier right after ADD COLUMN:", loader.read_table(table).select("loyalty_tier").distinct().collect())

        loader.update_where(
            table,
            "loyalty_tier = CASE WHEN customer_id % 3 = 0 THEN 'gold' "
            "WHEN customer_id % 3 = 1 THEN 'silver' ELSE 'bronze' END",
            "true",
        )
        loader.read_table(table).groupBy("loyalty_tier").count().orderBy("loyalty_tier").show()

        old = loader.read_as_of(table, snapshot_id=snapshots_before[-1])
        print("columns in the snapshot taken BEFORE the new column:", old.columns)

        paths_before_ddl = sorted(f["path"] for f in loader.data_files(table))
        spark.sql(f"ALTER TABLE demo.{table} RENAME COLUMN loyalty_tier TO tier")
        spark.sql(f"ALTER TABLE demo.{table} ALTER COLUMN customer_id TYPE bigint")
        current = loader.read_table(table)
        print("customer_id type after widening:", dict(current.dtypes)["customer_id"])
        print("tier counts after the rename:", sorted((r.tier, r["count"]) for r in current.groupBy("tier").count().collect()))
        spark.sql(f"ALTER TABLE demo.{table} DROP COLUMN tier")
        paths_after_ddl = sorted(f["path"] for f in loader.data_files(table))
        print("same data files after rename + widen + drop:", paths_before_ddl == paths_after_ddl)

        print("\n=== 2. Partition evolution on step10.order_lines (a copy of silver.order_lines)")
        table = "step10.order_lines"
        lines = loader.read_table("silver.order_lines")
        loader.write_silver(lines, table, partition_exprs=[F.months("order_date")])
        print("partitioning:", partition_fields(spark, table), "| files per spec id:", specs_in_use(loader, table))

        spark.sql(f"ALTER TABLE demo.{table} REPLACE PARTITION FIELD months(order_date) WITH days(order_date)")
        loader.write_silver(lines.limit(300), table, mode="append")  # written under the NEW spec
        print("partitioning:", partition_fields(spark, table), "| files per spec id:", specs_in_use(loader, table))

        used = add_truncate_field(spark, table)
        loader.write_silver(lines.limit(300), table, mode="append")
        print(f"added {used}")
        print("partitioning:", partition_fields(spark, table), "| files per spec id:", specs_in_use(loader, table))

        print("rows:", loader.read_table(table).count(), "(expected", lines.count() + 600, ")")


if __name__ == "__main__":
    main()