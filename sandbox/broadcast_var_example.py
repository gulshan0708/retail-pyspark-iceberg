"""Broadcast variables: ship a small lookup table to every executor once.
Run: scripts/submit.sh sandbox/broadcast_var_example.py
"""

import pickle
from operator import add

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader


def close_enough(left: dict, right: dict) -> bool:
    return left.keys() == right.keys() and all(abs(left[k] - right[k]) < 0.01 for k in right)


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        sc = spark.sparkContext
        loader = IcebergLoader(spark)
        lines = loader.read_table("silver.order_lines")
        products = loader.read_table("bronze.products")

        # Expected answer, from the category column silver.order_lines already has.
        expected = {
            r.category: float(r.revenue)
            for r in lines.groupBy("category").agg(F.sum("line_total").alias("revenue")).collect()
        }

        print("=== 1. RDD with a broadcast lookup (product_id -> category)")
        category_of = {r.product_id: r.category for r in products.select("product_id", "category").collect()}
        broadcast = sc.broadcast(category_of)
        print(f"lookup: {len(category_of)} entries, {len(pickle.dumps(category_of))} bytes pickled")
        print(f"a plain Python variable would be sent with each of {lines.rdd.getNumPartitions()} tasks; "
              "a broadcast goes to each executor once")

        by_broadcast = (
            lines.rdd.map(lambda r: (broadcast.value.get(r.product_id, "UNKNOWN"), float(r.line_total)))
            .reduceByKey(add)
            .collect()
        )
        by_broadcast = {category: round(total, 2) for category, total in by_broadcast}
        print("matches the category column:", close_enough(by_broadcast, expected))

        print("\n=== 2. The DataFrame way: a broadcast join")
        joined = lines.select("product_id", "line_total").join(
            F.broadcast(products.select("product_id", "category")), on="product_id"
        )
        joined.explain()  # look for BroadcastHashJoin
        by_join = {
            r.category: float(r.revenue)
            for r in joined.groupBy("category").agg(F.sum("line_total").alias("revenue")).collect()
        }
        print("matches the category column:", close_enough(by_join, expected))

        broadcast.unpersist()  # frees the executors' copies; destroy() removes it for good


if __name__ == "__main__":
    main()