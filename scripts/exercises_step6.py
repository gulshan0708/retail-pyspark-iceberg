"""Step 6 exercises: join types and UDF vs built-in function."""

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader
from src.transform.cleaning import CleaningTransformer
from src.transform.enrichment import EnrichmentTransformer


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        loader = IcebergLoader(spark)
        customers = loader.read_table("bronze.customers")
        items = loader.read_table("bronze.order_items")
        products = loader.read_table("bronze.products")
        orders = CleaningTransformer().transform(loader.read_table("bronze.orders"))
        enrichment = EnrichmentTransformer(customers, items, products)

        print("=== Exercise 1: join types (orders x customers)")
        counts = {}
        for how in ["inner", "left", "right", "outer", "left_semi", "left_anti"]:
            counts[how] = enrichment.join_customers(orders, how=how).count()
            print(f"{how:10s} {counts[how]}")
        print("inner + left_anti == left:", counts["inner"] + counts["left_anti"] == counts["left"])
        print("left_semi == inner:", counts["left_semi"] == counts["inner"])

        print("--- plan, left join (find the join strategy line)")
        enrichment.join_customers(orders, how="left").explain()
        print("--- plan, right join")
        enrichment.join_customers(orders, how="right").explain()

        print("=== Exercise 2: UDF vs built-in function")
        base = loader.read_table("silver.orders_enriched").drop("is_high_value")
        via_udf = enrichment.flag_high_value(base).withColumnRenamed("is_high_value", "udf_flag")
        both = enrichment.flag_high_value_builtin(via_udf).withColumnRenamed(
            "is_high_value", "builtin_flag"
        )
        print("rows where UDF and built-in disagree:",
              both.filter(F.col("udf_flag") != F.col("builtin_flag")).count())
        print("--- plan with the UDF")
        via_udf.explain()
        print("--- plan with the built-in")
        enrichment.flag_high_value_builtin(base).explain()


if __name__ == "__main__":
    main()