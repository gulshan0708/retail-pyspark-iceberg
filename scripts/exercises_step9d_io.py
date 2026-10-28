"""Step 9d (part 2): CSV read modes, schema inference, and Hive-style partitionBy writes.
Run: scripts/submit.sh scripts/exercises_step9d_io.py
Everything is written under /tmp/step9d inside the container.
"""

import contextlib
import glob
import io
import os
import shutil

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader

OUT = "/tmp/step9d"

# Rows 2, 3 and 4 are damaged: a text value in a number column, a missing column,
# and an extra column.
MESSY_ORDERS = """order_id,customer_id,quantity,unit_price
1,10,2,19.99
2,11,abc,5.00
3,12,1
4,13,3,7.50,extra
5,14,4,12.00
"""

# A zip code with a leading zero.
ZIPS = """id,zip,amount
1,01234,10.5
2,98765,20
"""

ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
])


def explain_text(df) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return buffer.getvalue()


def read_orders(spark, schema=ORDER_SCHEMA, **options):
    reader = spark.read.schema(schema).option("header", "true")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.csv(f"{OUT}/messy_orders.csv")


def main() -> None:
    shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(OUT)
    with open(f"{OUT}/messy_orders.csv", "w") as f:
        f.write(MESSY_ORDERS)
    with open(f"{OUT}/zips.csv", "w") as f:
        f.write(ZIPS)

    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")

        print("=== 1. PERMISSIVE (the default): bad values become null and nothing flags them")
        read_orders(spark).show()

        print("=== 2. PERMISSIVE plus a corrupt-record column: keep the raw text of bad rows")
        with_corrupt = StructType(ORDER_SCHEMA.fields + [StructField("_corrupt_record", StringType())])
        # cache() first: Spark refuses to query only the corrupt-record column of a raw file
        df = read_orders(spark, with_corrupt).cache()
        print("bad rows:")
        df.filter(F.col("_corrupt_record").isNotNull()).show(truncate=False)
        print("good rows:")
        df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record").show()
        df.unpersist()

        print("=== 3. DROPMALFORMED: silently drops the bad rows")
        kept = read_orders(spark, mode="DROPMALFORMED").collect()
        print("order_ids kept:", [row.order_id for row in kept])

        print("=== 4. FAILFAST: stops at the first bad row")
        try:
            read_orders(spark, mode="FAILFAST").collect()
        except Exception as exc:  # the exact exception class varies, so catch broadly
            print("FAILFAST raised:", type(exc).__name__)
            print(str(exc).splitlines()[0][:200])

        print("=== 5. inferSchema guesses, and can guess wrong")
        inferred = (
            spark.read.option("header", "true").option("inferSchema", "true").csv(f"{OUT}/zips.csv")
        )
        inferred.printSchema()
        inferred.show()
        print("with an explicit schema:")
        spark.read.schema("id int, zip string, amount double").option("header", "true").csv(
            f"{OUT}/zips.csv"
        ).show()

        print("=== 6. Hive-style partitionBy: one directory per value")
        lines = IcebergLoader(spark).read_table("silver.order_lines").select(
            "order_id", "order_date", "country", "category", "line_total"
        )
        by_country = f"{OUT}/lines_by_country"
        lines.repartition("country").write.mode("overwrite").partitionBy("country").parquet(by_country)
        print("directories:", sorted(d for d in os.listdir(by_country) if not d.startswith("_")))

        back = spark.read.parquet(by_country)
        back.printSchema()  # country is a column again, rebuilt from the directory names
        filtered = back.filter("country = 'US'")
        plan = explain_text(filtered)
        print("\n".join(line.strip() for line in plan.splitlines() if "PartitionFilters" in line))
        print(f"US rows: {filtered.count()} of {back.count()}")

        print("=== 7. Partitioning by a high-cardinality column creates many tiny partitions")
        by_day = f"{OUT}/lines_by_date"
        lines.write.mode("overwrite").partitionBy("order_date").parquet(by_day)
        directories = [d for d in os.listdir(by_day) if d.startswith("order_date=")]
        files = sum(len(glob.glob(f"{by_day}/{d}/*.parquet")) for d in directories)
        print(f"{len(directories)} directories, {files} Parquet files for {lines.count()} rows")


if __name__ == "__main__":
    main()