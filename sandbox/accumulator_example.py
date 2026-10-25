"""Accumulators: counters that tasks can only add to and only the driver can read.
Run: scripts/submit.sh sandbox/accumulator_example.py
"""

from src.core.spark_session import SparkSessionManager

SOURCE = "s3a://raw/order_items/"
HEADER = "order_item_id"


def parse(line, malformed, negative_quantity):
    """Turns one CSV line into (order_id, quantity); counts problems in accumulators."""
    try:
        parts = line.split(",")
        item_id, order_id, product_id, quantity = (int(p) for p in parts[:4])
        float(parts[4])  # unit_price must be a number
    except (ValueError, IndexError):  # a bad number, or too few columns
        malformed.add(1)
        return None
    if quantity < 0:
        negative_quantity.add(1)
    return (order_id, quantity)


def parsed_rdd(raw, malformed, negative_quantity):
    data_lines = raw.filter(lambda line: not line.startswith(HEADER))  # each file has a header
    return data_lines.map(lambda line: parse(line, malformed, negative_quantity)).filter(
        lambda result: result is not None
    )


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        sc = spark.sparkContext
        raw = sc.textFile(SOURCE)

        print("=== 1. Accumulators updated inside a transformation (map)")
        malformed, negative = sc.accumulator(0), sc.accumulator(0)
        parsed = parsed_rdd(raw, malformed, negative)
        print("rows parsed:", parsed.count())
        print(f"after action 1: malformed={malformed.value}, negative_quantity={negative.value}")
        parsed.count()  # a second action re-runs the whole map
        print(f"after action 2: malformed={malformed.value}, negative_quantity={negative.value}"
              "   <- counted again")

        print("\n=== 2. Fix A: cache, so the map runs only once")
        malformed_a, negative_a = sc.accumulator(0), sc.accumulator(0)
        cached = parsed_rdd(raw, malformed_a, negative_a).cache()
        cached.count()
        cached.count()
        print(f"after two actions: malformed={malformed_a.value}, negative_quantity={negative_a.value}")
        cached.unpersist()

        print("\n=== 3. Fix B: update accumulators inside an action (foreach)")
        malformed_b, negative_b = sc.accumulator(0), sc.accumulator(0)
        raw.filter(lambda line: not line.startswith(HEADER)).foreach(
            lambda line: parse(line, malformed_b, negative_b)
        )
        print(f"after foreach: malformed={malformed_b.value}, negative_quantity={negative_b.value}")


if __name__ == "__main__":
    main()