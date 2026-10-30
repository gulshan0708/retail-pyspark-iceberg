"""DataFrame methods the pipelines never needed.
Run: scripts/submit.sh sandbox/dataframe_extras.py
"""

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader


def section(title: str) -> None:
    print(f"\n=== {title}")


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        loader = IcebergLoader(spark)
        customers = loader.read_table("bronze.customers")
        lines = loader.read_table("silver.order_lines")
        orders = loader.read_table("silver.orders_enriched")

        section("1. like and ilike: wildcard matching (% = any text, _ = one character)")
        print("emails ending in .com:", customers.filter(F.col("email").like("%.com")).count())
        customers.filter(F.col("first_name").ilike("an%")).select("first_name").distinct().show()  # ilike ignores case

        section("2. Array columns: build, measure and search")
        orders.select(
            "order_id",
            F.array("order_id", "customer_id").alias("ids"),                         # build an array from columns
            F.size("items").alias("item_count"),                                     # number of elements
            F.array_contains(F.col("items.product_id"), 1).alias("has_product_1"),   # does the array contain 1?
        ).show(5, truncate=False)

        section("3. Sampling and splitting")
        print("10% sample:", lines.sample(fraction=0.1, seed=42).count(), "of", lines.count())
        train, test = lines.randomSplit([0.8, 0.2], seed=42)
        print("random split 80/20:", train.count(), "+", test.count())

        section("4. summary(): count, extremes and percentiles")
        lines.select("quantity", "line_total").summary("count", "min", "50%", "max").show()

        section("5. union (by position) vs unionByName (by name)")
        a = spark.createDataFrame([(1, "x")], "id int, label string")
        b = spark.createDataFrame([("y", 2)], "label string, id int")  # same columns, different order
        print("union matches columns by POSITION, so the second row lands in the wrong columns:")
        a.union(b).show()
        print("unionByName matches by NAME:")
        a.unionByName(b).show()

        section("6. Getting rows back to Python")
        print("take(2):", lines.select("order_id", "line_total").take(2))
        print("head():", lines.select("order_id").head())
        print("tail(2):", lines.orderBy("order_id", "order_item_id").select("order_id").tail(2))

        section("7. na.replace and na.drop options")
        customers.na.replace(["US"], ["USA"], subset=["country"]).groupBy("country").count().orderBy("country").show()
        print("rows where ALL of email and country are null:",
              customers.count() - customers.na.drop(how="all", subset=["email", "country"]).count())

        section("8. toPandas(): pulls the result into the driver's memory (only for small results)")
        try:
            print(lines.select("order_id", "line_total").limit(3).toPandas())
        except Exception as exc:
            print("toPandas needs pandas in the container:", type(exc).__name__)


if __name__ == "__main__":
    main()