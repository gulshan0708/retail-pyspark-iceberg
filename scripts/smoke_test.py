"""Checks that Spark can reach everything the pipelines need."""

from src.core.spark_session import SparkSessionManager


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        # 1. Raw files in object storage (needs hadoop-aws)
        orders = spark.read.option("header", "true").csv("s3a://raw/orders/orders.csv")
        print("orders rows:", orders.count())

        # 2. Postgres over JDBC (needs the Postgres driver)
        products = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/retail")
            .option("dbtable", "products")
            .option("user", "postgres")
            .option("password", "postgres")
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        print("products rows:", products.count())

        # 3. Iceberg catalog
        spark.sql("SHOW NAMESPACES IN demo").show()


if __name__ == "__main__":
    main()