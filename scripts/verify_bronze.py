"""Prints row counts for every bronze and quarantine table, plus why rows were rejected."""

from src.core.spark_session import SparkSessionManager

ENTITIES = ["customers", "products", "orders", "order_items", "payments"]


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        for layer in ("bronze", "quarantine"):
            for entity in ENTITIES:
                print(f"{layer}.{entity}: {spark.table(f'demo.{layer}.{entity}').count()}")

        for entity in ENTITIES:
            print(f"--- reject reasons: {entity}")
            spark.sql(
                f"SELECT reject_reason, count(*) AS n "
                f"FROM demo.quarantine.{entity} GROUP BY reject_reason"
            ).show(truncate=False)

       ## print("--- partitions of bronze.orders (hidden partitioning, days(order_date))")
       ## spark.sql(
       ##     "SELECT partition, record_count FROM demo.bronze.orders.partitions LIMIT 5"
       ## ).show(truncate=False)


if __name__ == "__main__":
    main()