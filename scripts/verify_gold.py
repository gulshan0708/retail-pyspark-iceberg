"""Checks the gold tables: row counts, duplicate keys, and revenue reconciliation."""

from pyspark.sql import functions as F

from pipelines.pipeline3_aggregate import GOLD_TABLE_KEYS, PIVOT_COUNTRIES
from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        loader = IcebergLoader(spark)

        print("--- row counts and duplicate keys (duplicate_keys must be 0)")
        for table, keys in GOLD_TABLE_KEYS.items():
            df = loader.read_table(table)
            duplicates = df.groupBy(*keys).count().filter("count > 1").count()
            print(f"{table:32s} rows={df.count():5d} duplicate_keys={duplicates}")

        expected = (
            loader.read_table("silver.orders_enriched").agg(F.sum("order_total")).first()[0]
        )
        print(f"--- revenue reconciliation (silver total = {expected})")

        daily = loader.read_table("gold.daily_sales").agg(F.sum("total_revenue")).first()[0]
        grand_total = (
            loader.read_table("gold.sales_by_country_category")
            .filter("country IS NULL AND category IS NULL")
            .first()["revenue"]
        )
        revenue_cols = [f"revenue_{c.lower()}" for c in PIVOT_COUNTRIES]
        # Built-in sum() adds the Column objects together: one expression per row.
        pivot_total = (
            loader.read_table("gold.category_country_pivot")
            .select(sum(F.coalesce(F.col(c), F.lit(0)) for c in revenue_cols).alias("row_total"))
            .agg(F.sum("row_total"))
            .first()[0]
        )
        ranked = loader.read_table("gold.product_rankings").agg(F.sum("revenue")).first()[0]

        for label, value in [
            ("daily_sales", daily),
            ("rollup grand total", grand_total),
            ("pivot, all countries", pivot_total),
            ("product_rankings", ranked),
        ]:
            print(f"{label:22s} {value}  matches: {value == expected}")

        print("--- first 3 days")
        loader.read_table("gold.daily_sales").orderBy("order_date").show(3)
        print("--- top 3 products per category")
        (
            loader.read_table("gold.product_rankings")
            .filter("row_num <= 3")
            .orderBy("category", "row_num")
            .show(15, truncate=False)
        )
        print("--- one customer's order history")
        (
            loader.read_table("gold.customer_order_history")
            .filter("customer_id = 1")
            .orderBy("order_date", "order_id")
            .show()
        )


if __name__ == "__main__":
    main()