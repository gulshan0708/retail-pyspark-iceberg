"""Step 7 exercises: ranking ties, rollup vs cube, window plans, rolling frames."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader
from src.transform.aggregation import AggregationTransformer
from src.transform.window import WindowTransformer


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        loader = IcebergLoader(spark)
        orders = loader.read_table("silver.orders_enriched").select(
            "order_id", "customer_id", "order_date", "order_total"
        )
        lines = loader.read_table("silver.order_lines")
        aggregation = AggregationTransformer()

        print("=== Exercise 1: row_number vs rank vs dense_rank on tied values")
        tied = spark.createDataFrame(
            [("A", 100), ("B", 50), ("C", 50), ("D", 10)], "product string, revenue int"
        )
        w = Window.orderBy(F.col("revenue").desc())  # no partitionBy: Spark will warn, it's 4 rows
        tied.select(
            "product",
            "revenue",
            F.row_number().over(w).alias("row_number"),
            F.rank().over(w).alias("rank"),
            F.dense_rank().over(w).alias("dense_rank"),
        ).show()

        print("=== Exercise 2: rollup vs cube")
        print("rollup rows:", aggregation.sales_rollup(lines).count())
        print("cube rows:  ", aggregation.sales_cube(lines).count())
        print("--- grouping() tells subtotal rows apart from real nulls (1 = rolled up)")
        (
            lines.rollup("country", "category")
            .agg(
                F.sum("line_total").alias("revenue"),
                F.grouping("country").alias("country_rolled_up"),
                F.grouping("category").alias("category_rolled_up"),
            )
            .filter("category_rolled_up = 1")
            .orderBy("country")
            .show(truncate=False)
        )

        print("=== Exercise 3: query plans")
        print("--- window function")
        WindowTransformer().customer_running_total(orders).explain()
        print("--- groupBy aggregation")
        aggregation.daily_revenue(orders).explain()

        print("=== Exercise 4: a rolling frame (average of the customer's last 3 orders)")
        last_three = (
            Window.partitionBy("customer_id")
            .orderBy("order_date", "order_id")
            .rowsBetween(-2, 0)
        )
        (
            orders.filter("customer_id = 1")
            .withColumn("avg_last_3_orders", F.round(F.avg("order_total").over(last_three), 2))
            .orderBy("order_date", "order_id")
            .show()
        )


if __name__ == "__main__":
    main()