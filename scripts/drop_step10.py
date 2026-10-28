"""Drops every table in the step10 scratch namespace.
Run: scripts/submit.sh scripts/drop_step10.py
"""

from src.core.spark_session import SparkSessionManager


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.step10")
        for row in spark.sql("SHOW TABLES IN demo.step10").collect():
            name = f"demo.step10.{row.tableName}"
            try:
                spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")  # also deletes the files
            except Exception:
                spark.sql(f"DROP TABLE IF EXISTS {name}")        # fall back: catalog entry only
            print("dropped", name)
        spark.sql("DROP NAMESPACE IF EXISTS demo.step10")


if __name__ == "__main__":
    main()