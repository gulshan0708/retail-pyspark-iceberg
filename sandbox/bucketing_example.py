"""Hive-style bucketing: pre-organize two tables by the join key so that joining them
needs no shuffle. Run: scripts/submit.sh sandbox/bucketing_example.py
"""

import contextlib
import glob
import io

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager

WAREHOUSE = "/tmp/step9c_warehouse"
BUCKETS = 8
ROWS = 200_000
SHUFFLE_MARKER = "Exchange hashpartitioning"


def explain_text(df) -> str:
    """df.explain() prints the plan; this captures that printout as a string."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return buffer.getvalue()


def main() -> None:
    conf = {
        "spark.sql.defaultCatalog": "spark_catalog",         # this image defaults to Iceberg's `demo`; bucketBy needs Spark's own catalog
        "spark.sql.warehouse.dir": WAREHOUSE,                # where saveAsTable puts its files
        "spark.sql.adaptive.enabled": "false",               # keep the printed plan the final plan
        "spark.sql.autoBroadcastJoinThreshold": "-1",        # no broadcast joins: force a sort-merge join
    }
    with SparkSessionManager(app_name="sandbox-bucketing", extra_conf=conf) as spark:
        spark.sparkContext.setLogLevel("WARN")
        orders = spark.range(ROWS).select(
            F.col("id").alias("order_id"), (F.col("id") % 100).alias("customer_id")
        ).repartition(8)
        items = spark.range(ROWS).select(
            F.col("id").alias("order_id"), ((F.col("id") * 3) % 997).alias("amount")
        ).repartition(8)

        try:
            print("=== 1. Joining two ordinary DataFrames")
            plain = orders.join(items, "order_id")
            plain_plan = explain_text(plain)
            print(plain_plan)
            print(f"shuffles for the join ({SHUFFLE_MARKER}):", plain_plan.count(SHUFFLE_MARKER))

            print("\n=== 2. Writing both as bucketed tables")
            for name, df in [("bucketed_orders", orders), ("bucketed_items", items)]:
                (
                    df.repartition(BUCKETS, "order_id")  # one hash partition per bucket => one file per bucket
                    .write.bucketBy(BUCKETS, "order_id")
                    .sortBy("order_id")
                    .mode("overwrite")
                    .saveAsTable(name)
                )
            files = glob.glob(f"{WAREHOUSE}/bucketed_orders/*.parquet")
            print(f"bucketed_orders: {len(files)} Parquet file(s), one per bucket")
            (
                spark.sql("DESCRIBE EXTENDED bucketed_orders")
                .filter(F.col("col_name").isin("Num Buckets", "Bucket Columns", "Sort Columns"))
                .show(truncate=False)
            )

            print("=== 3. Joining the two bucketed tables")
            bucketed = spark.table("bucketed_orders").join(spark.table("bucketed_items"), "order_id")
            bucketed_plan = explain_text(bucketed)
            print(bucketed_plan)
            print(f"shuffles for the join ({SHUFFLE_MARKER}):", bucketed_plan.count(SHUFFLE_MARKER))
            print("same row count:", plain.count() == bucketed.count() == ROWS)
        finally:
            spark.sql("DROP TABLE IF EXISTS bucketed_orders")
            spark.sql("DROP TABLE IF EXISTS bucketed_items")


if __name__ == "__main__":
    main()