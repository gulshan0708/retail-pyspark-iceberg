"""Step 9b: performance exercises. Run with:

    scripts/submit.sh scripts/exercises_step9b.py [--hold SECONDS]

Timings depend on your machine and are noisy: read them as "which is bigger",
not as exact numbers. Needs silver.order_lines (run Pipelines 1 and 2 first).
"""

import argparse
import glob
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader

ROWS = 1_000_000        # raise this if the timings are too small to compare
OUT_DIR = "/tmp/step9b"  # local files inside the container


def execute(df) -> None:
    """Runs the whole query and throws the result away (the "noop" sink), so every
    column is really computed. A plain count() would let Spark skip unused columns."""
    df.write.format("noop").mode("overwrite").save()


def timed(label, action):
    """Runs action(), prints how long it took, and returns its result."""
    start = time.perf_counter()
    result = action()
    print(f"  {label:<48s} {time.perf_counter() - start:6.2f}s")
    return result


def make_big(spark):
    """ROWS rows with a deliberately CPU-heavy column, so recomputing it costs something."""
    heavy = F.col("id").cast("string")
    for _ in range(4):
        heavy = F.sha2(heavy, 256)
    return spark.range(ROWS).select(
        "id",
        (F.col("id") % 1000).alias("bucket"),
        F.substring(heavy, 1, 16).alias("payload"),
    )


def summarize(df):
    return df.groupBy("bucket").agg(F.count("*").alias("n"), F.max("payload").alias("max_payload"))


# ---------------------------------------------------------------- A
def part_a_plans(lines) -> None:
    print("\n=== A. Reading a plan: where are the shuffles?")
    revenue = lines.groupBy("country", "category").agg(F.sum("line_total").alias("revenue"))
    revenue.explain("formatted")
    print("  Look for 'Exchange': that node is the shuffle that groupBy needs.")


# ---------------------------------------------------------------- B
def part_b_shuffle_partitions(spark) -> None:
    print("\n=== B. Shuffle partitions and adaptive query execution (AQE)")
    keys = ("spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions")
    original = {key: spark.conf.get(key) for key in keys}
    buckets = spark.range(ROWS).select((F.col("id") % 1000).alias("bucket"))

    def grouped():
        return buckets.groupBy("bucket").count()

    execute(grouped())  # warm-up, not timed: the first run pays start-up costs

    settings = [
        ("AQE off, 200 shuffle partitions (Spark default)", "false", "200"),
        ("AQE off,   8 shuffle partitions", "false", "8"),
        ("AQE on,  200 shuffle partitions", "true", "200"),
    ]
    for label, aqe, partitions in settings:
        spark.conf.set("spark.sql.adaptive.enabled", aqe)
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        df = grouped()
        timed(label, lambda: execute(df))
        print(f"      partitions after the shuffle: {df.rdd.getNumPartitions()}")

    for key, value in original.items():  # put the session back as we found it
        spark.conf.set(key, value)


# ---------------------------------------------------------------- C
def rows_per_partition(df) -> list:
    """Row count of every non-empty partition, via spark_partition_id()."""
    rows = df.groupBy(F.spark_partition_id().alias("pid")).count().orderBy("pid").collect()
    return [row["count"] for row in rows]


def count_parquet_files(df, name) -> int:
    path = f"{OUT_DIR}/{name}"
    df.write.mode("overwrite").parquet(path)
    return len(glob.glob(f"{path}/*.parquet"))


def part_c_repartition_vs_coalesce(lines) -> None:
    print("\n=== C. repartition vs coalesce")
    base = lines.repartition(8)
    variants = [
        ("repartition(8)", base),
        ("repartition(8).coalesce(2)", base.coalesce(2)),
        ("repartition(8).repartition(4)", base.repartition(4)),
        ("repartition('country')", lines.repartition("country")),
    ]
    for label, df in variants:
        print(f"\n  -- {label}")
        print(f"     partitions: {df.rdd.getNumPartitions()}")
        print(f"     rows per non-empty partition: {rows_per_partition(df)}")
        df.explain()

    print("\n  -- files written (Parquet, local disk): one file per partition")
    writes = [
        ("repartition(20)", lines.repartition(20)),
        ("repartition(4)", lines.repartition(4)),
        ("coalesce(1)", lines.coalesce(1)),
    ]
    for label, df in writes:
        folder = label.replace("(", "_").replace(")", "")
        print(f"     {label:<18s} -> {count_parquet_files(df, folder)} file(s)")


# ---------------------------------------------------------------- D
def part_d_caching(spark) -> None:
    print("\n=== D. cache vs persist")
    print("  Without caching, the CPU-heavy column is recomputed on every run:")
    big = make_big(spark)
    timed("run 1", lambda: execute(summarize(big)))
    timed("run 2 (recomputed from scratch)", lambda: execute(summarize(big)))

    default = make_big(spark).cache()
    print(f"\n  cache() on a DataFrame uses: {default.storageLevel}")
    default.unpersist()

    levels = [
        ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
        ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
        ("DISK_ONLY", StorageLevel.DISK_ONLY),
    ]
    for name, level in levels:
        cached = make_big(spark).persist(level)
        print(f"\n  persist({name}) -> {cached.storageLevel}")
        timed("run 1 (fills the cache)", lambda: execute(summarize(cached)))
        timed("run 2 (reads the cache)", lambda: execute(summarize(cached)))
        if name == "MEMORY_AND_DISK":
            print("  plan with a cached input (look for InMemoryTableScan):")
            summarize(cached).explain()
        cached.unpersist()  # always release what you cached


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 9b performance exercises")
    parser.add_argument(
        "--hold",
        type=int,
        default=0,
        help="keep the Spark UI (http://localhost:4040) alive this many seconds at the end",
    )
    args = parser.parse_args()

    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        lines = IcebergLoader(spark).read_table("silver.order_lines")
        part_a_plans(lines)
        part_b_shuffle_partitions(spark)
        part_c_repartition_vs_coalesce(lines)
        part_d_caching(spark)
        if args.hold:
            print(f"\nSpark UI is open at http://localhost:4040 for {args.hold} seconds...")
            time.sleep(args.hold)


if __name__ == "__main__":
    main()