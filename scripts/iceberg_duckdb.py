"""Multi-engine proof: DuckDB reads an Iceberg table that Spark wrote.
Setup:  docker exec retail-spark python3 -m pip install --quiet --root-user-action=ignore duckdb
Run:    scripts/submit.sh scripts/iceberg_duckdb.py
"""

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader

try:
    import duckdb
except ImportError:
    duckdb = None


def open_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    # Tell DuckDB how to reach our storage (the same endpoint and keys Spark uses)
    con.execute(
        "CREATE OR REPLACE SECRET rustfs (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', "
        "ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')"
    )
    return con


def main() -> None:
    if duckdb is None:
        print("DuckDB is not installed in the container. Run:\n"
              "  docker exec retail-spark python3 -m pip install --quiet --root-user-action=ignore duckdb")
        return

    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        loader = IcebergLoader(spark)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.step10")
        spark.sql("DROP TABLE IF EXISTS demo.step10.duck_demo")
        table = "step10.duck_demo"

        loader.write_bronze(spark.range(100).withColumnRenamed("id", "n"), table)
        first_file = loader.metadata_location(table)
        print("Spark wrote 100 rows. Metadata file:", first_file)

        loader.write_bronze(spark.range(100, 150).withColumnRenamed("id", "n"), table, mode="append")
        second_file = loader.metadata_location(table)
        print("Spark appended 50 rows. Metadata file:", second_file)

        try:
            con = open_duckdb()
            for label, path in [("first version ", first_file), ("latest version", second_file)]:
                count = con.execute(f"SELECT count(*) FROM iceberg_scan('{path}')").fetchone()[0]
                print(f"DuckDB, {label}: {count} rows")
            print("DuckDB first 3 rows:", con.execute(
                f"SELECT * FROM iceberg_scan('{second_file}') ORDER BY n LIMIT 3").fetchall())
        except Exception as exc:
            print("DuckDB could not read the table:", type(exc).__name__, str(exc)[:300])


if __name__ == "__main__":
    main()