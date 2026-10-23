"""Checks the silver tables: counts, cleaned values, and row reconciliation."""

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        loader = IcebergLoader(spark)
        orders = loader.read_table("silver.orders_enriched")
        lines = loader.read_table("silver.order_lines")
        bronze_orders = loader.read_table("bronze.orders")
        bronze_items = loader.read_table("bronze.order_items")

        print("silver.orders_enriched:", orders.count())
        print("silver.order_lines:", lines.count())
        print("orders with unknown country:", orders.filter(F.col("country") == "UNKNOWN").count())

        print("--- status values (expect only placed/shipped/delivered/cancelled)")
        orders.groupBy("status").count().orderBy("status").show()

        print("--- schema (note the nested items array)")
        orders.printSchema()

        # Reconciliation 1: where did the missing lines go?
        orphaned = bronze_items.join(bronze_orders.select("order_id"), on="order_id", how="left_anti")
        print("bronze items whose order is not in bronze.orders:", orphaned.count())
        print("bronze items minus silver lines:", bronze_items.count() - lines.count())

        # Reconciliation 2: the two silver tables must agree on revenue
        order_sum = orders.agg(F.sum("order_total")).first()[0]
        line_sum = lines.agg(F.sum("line_total")).first()[0]
        print("sum(order_total) =", order_sum, "| sum(line_total) =", line_sum, "| equal:", order_sum == line_sum)


if __name__ == "__main__":
    main()