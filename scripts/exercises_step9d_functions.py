"""Step 9d (part 1): the derived customer columns, selectExpr, and three kinds of UDF.
Run: scripts/submit.sh scripts/exercises_step9d_functions.py
"""

import time

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader
from src.transform.profile import ProfileTransformer

# A pandas_udf needs pandas and pyarrow. If the container lacks them, the import fails
# and we skip that part instead of crashing.
try:
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf(StringType())
    def title_case_vectorized(names: pd.Series) -> pd.Series:
        return names.str.title()  # works on a whole batch of names at once
except ImportError:
    title_case_vectorized = None


@F.udf(StringType())
def title_case_scalar(name):
    """A scalar UDF: called once per row, in a Python process."""
    return name.title() if name is not None else None


def execute(df) -> None:
    df.write.format("noop").mode("overwrite").save()  # compute everything, keep nothing


def timed(label, action):
    start = time.perf_counter()
    action()
    print(f"  {label:<40s} {time.perf_counter() - start:6.2f}s")


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        customers = IcebergLoader(spark).read_table("bronze.customers")
        profile = ProfileTransformer()

        print("=== 1. String and date functions (DataFrame API)")
        enriched = profile.transform(customers)
        enriched.select(
            "customer_id", "full_name", "initials", "email_domain",
            "email_user", "signup_month", "signup_weekday",
        ).show(5, truncate=False)
        enriched.groupBy("signup_weekday").count().orderBy(F.col("count").desc()).show()

        print("=== 2. The same columns as SQL strings (selectExpr)")
        via_sql = profile.transform_with_sql(customers)
        differing = enriched.exceptAll(via_sql).count() + via_sql.exceptAll(enriched).count()
        print("rows that differ between the two versions:", differing)

        print("=== 3. Title-casing 500,000 names: built-in vs scalar UDF vs pandas_udf (noisy timings)")
        names = spark.range(500_000).select(
            F.concat(F.lit("anna kim "), F.col("id").cast("string")).alias("name")
        )
        timed("built-in initcap", lambda: execute(names.select(F.initcap("name"))))
        timed("scalar UDF (one value at a time)", lambda: execute(names.select(title_case_scalar("name"))))
        if title_case_vectorized is None:
            print("  pandas_udf skipped. Install the libraries with:\n"
                  "  docker exec retail-spark python3 -m pip install pandas pyarrow")
        else:
            timed("pandas_udf (a batch at a time)", lambda: execute(names.select(title_case_vectorized("name"))))
            print("--- plan with the pandas_udf (look for ArrowEvalPython)")
            names.select(title_case_vectorized("name")).explain()
        print("--- plan with the scalar UDF (look for BatchEvalPython)")
        names.select(title_case_scalar("name")).explain()


if __name__ == "__main__":
    main()