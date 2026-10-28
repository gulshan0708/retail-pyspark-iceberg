"""Write-Audit-Publish with an Iceberg branch, plus a tag.
Run: scripts/submit.sh scripts/iceberg_wap.py
"""

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader
from src.quality.checker import DataQualityChecker


def audit(df) -> list:
    """Quality checks on a batch. An empty list means it may be published."""
    problems = []
    if DataQualityChecker.check_non_negative(df, ["total_revenue"]).count() > 0:
        problems.append("negative total_revenue")
    if DataQualityChecker.check_uniqueness(df, ["order_date"]).count() > 0:
        problems.append("duplicate order_date")
    return problems


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        loader = IcebergLoader(spark)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.step10")
        spark.sql("DROP TABLE IF EXISTS demo.step10.daily_sales_wap")

        table = "step10.daily_sales_wap"
        daily = loader.read_table("gold.daily_sales")
        loader.write_bronze(daily, table)
        loader.enable_wap(table)
        start_rows = loader.read_table(table).count()
        print("rows on main at the start:", start_rows)

        # Two incoming batches: the last 3 days shifted into the future. One is bad.
        latest = daily.orderBy(F.col("order_date").desc()).limit(3)
        good_batch = latest.withColumn("order_date", F.date_add("order_date", 365))
        bad_batch = (
            latest.withColumn("order_date", F.date_add("order_date", 730))
            .withColumn("total_revenue", -F.col("total_revenue"))
        )

        def publish_if_clean(batch, label) -> None:
            print(f"\n=== {label}")
            loader.create_branch(table, "audit")
            with loader.wap_branch("audit"):
                loader.write_bronze(batch, table, mode="append")  # lands on the branch only
            print(f"rows on main: {loader.read_table(table).count()} | "
                  f"rows on the audit branch: {loader.read_branch(table, 'audit').count()}")
            problems = audit(loader.read_branch(table, "audit"))  # the audit reads the BRANCH
            if problems:
                print("AUDIT FAILED:", problems, "-> not published")
            else:
                loader.fast_forward_branch(table, "audit")
                print("audit passed -> published. rows on main now:", loader.read_table(table).count())
            loader.drop_branch(table, "audit")

        publish_if_clean(good_batch, "1. A good batch")
        publish_if_clean(bad_batch, "2. A bad batch (negative revenue)")
        print("\nrows on main at the end:", loader.read_table(table).count(),
              "(started at", start_rows, "+ 3 good rows)")

        print("\n=== 3. A tag: a permanent name for a snapshot")
        spark.sql(f"ALTER TABLE demo.{table} CREATE OR REPLACE TAG `after_good_batch`")
        extra = latest.withColumn("order_date", F.date_add("order_date", 1095))
        loader.write_bronze(extra, table, mode="append")  # no WAP branch is active, so this goes to main
        tagged = spark.sql(f"SELECT * FROM demo.{table} VERSION AS OF 'after_good_batch'").count()
        print(f"rows at the tag: {tagged} | rows now: {loader.read_table(table).count()}")


if __name__ == "__main__":
    main()