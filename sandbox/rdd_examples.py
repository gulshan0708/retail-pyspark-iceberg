"""RDD basics on the silver order lines. Run: scripts/submit.sh sandbox/rdd_examples.py

DataFrames are what real pipelines use (Spark can optimize them). RDDs are the lower-level
API underneath: knowing them explains what DataFrames do for you.
"""

import time
from operator import add

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader


def section(title: str) -> None:
    print(f"\n=== {title}")


def timed(label: str, action):
    start = time.perf_counter()
    result = action()
    print(f"  {label:<40s} {time.perf_counter() - start:6.2f}s")
    return result


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        sc = spark.sparkContext
        loader = IcebergLoader(spark)
        lines = loader.read_table("silver.order_lines")
        orders = loader.read_table("silver.orders_enriched")

        section("1. From DataFrame to RDD: every element is a Row")
        rows = lines.rdd
        print("partitions:", rows.getNumPartitions())
        print("first element:", rows.first())

        section("2. map: one input element becomes exactly one output element")
        pairs = rows.map(lambda r: (r.country, float(r.line_total)))  # (key, value) pairs
        print(pairs.take(3))

        section("3. filter: keep the elements where the function returns True")
        big_rdd = rows.filter(lambda r: r.quantity >= 4).count()
        big_df = lines.filter("quantity >= 4").count()
        print(f"lines with quantity >= 4: RDD={big_rdd}, DataFrame={big_df}")

        section("4. flatMap: one input element becomes zero or more output elements")
        order_products = orders.rdd.flatMap(
            lambda r: [(r.order_id, item.product_id) for item in (r["items"] or [])]
        )
        print(f"(order, product) pairs from the nested arrays: {order_products.count()}")
        print(f"rows in silver.order_lines:                     {lines.count()}")

        section("5. reduceByKey: combine all values that share a key")
        revenue_rdd = pairs.reduceByKey(add)
        rdd_result = {country: round(total, 2) for country, total in revenue_rdd.collect()}
        df_result = {
            r.country: float(r.revenue)
            for r in lines.groupBy("country").agg(F.sum("line_total").alias("revenue")).collect()
        }
        same = rdd_result.keys() == df_result.keys() and all(
            abs(rdd_result[k] - df_result[k]) < 0.01 for k in df_result
        )
        print("revenue per country matches the DataFrame groupBy:", same)

        section("6. reduceByKey vs groupByKey")
        via_group = pairs.groupByKey().mapValues(sum)
        print("same totals:", {k: round(v, 2) for k, v in via_group.collect()} == rdd_result)
        print("reduceByKey lineage:\n", revenue_rdd.toDebugString().decode("utf-8"))
        print("groupByKey lineage:\n", via_group.toDebugString().decode("utf-8"))

        section("7. More building blocks")
        print("lines per country (countByKey):", dict(pairs.countByKey()))
        print("top 3 countries (sortBy):", revenue_rdd.sortBy(lambda kv: kv[1], ascending=False).take(3))
        print("grand total (reduce):", round(pairs.map(lambda kv: kv[1]).reduce(add), 2))
        print("rows per partition (mapPartitions):",
              rows.mapPartitions(lambda part: [sum(1 for _ in part)]).collect())

        section("8. Back to a DataFrame")
        revenue_rdd.toDF(["country", "revenue"]).orderBy(F.col("revenue").desc()).show()

        section("9. The same job with RDDs and with DataFrames (noisy timings)")
        n = 2_000_000
        timed("RDD:       map + reduceByKey",
              lambda: sc.range(n).map(lambda x: (x % 1000, 1)).reduceByKey(add).count())
        timed("DataFrame: groupBy + count",
              lambda: spark.range(n).groupBy((F.col("id") % 1000).alias("k")).count().count())


if __name__ == "__main__":
    main()