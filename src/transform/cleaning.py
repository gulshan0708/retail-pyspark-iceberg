"""Cleaning transformer for the `orders` entity (Pipeline 2)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from src.transform.base import BaseTransformer

VALID_STATUSES = ["placed", "shipped", "delivered", "cancelled"]


class CleaningTransformer(BaseTransformer):
    """Standardises raw orders: trims whitespace, normalises status text,
    fixes types, removes duplicate order_ids, renames order_status to
    `status`. Downstream layers use the column name `status`."""

    def transform(self, df: DataFrame) -> DataFrame:
        # List comprehension: names of every string column in the schema.
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]
        for c in string_cols:
            df = df.withColumn(c, F.trim(F.col(c)))

        df = (
            df
            # lowercase, then collapse repeated whitespace into one space
            .withColumn(
                "order_status",
                F.regexp_replace(F.lower(F.col("order_status")), r"\s+", " "),
            )
            # anything outside the allowed list becomes 'unknown'
            .withColumn(
                "order_status",
                F.when(F.col("order_status").isin(VALID_STATUSES), F.col("order_status"))
                .otherwise(F.lit("unknown")),
            )
            .withColumn("order_id", F.col("order_id").cast("int"))
            .withColumn("order_date", F.to_date(F.col("order_date")))
            .withColumnRenamed("order_status", "status")
            .dropDuplicates(["order_id"])
        )
        return df.select("order_id", "customer_id", "order_date", "status", "payment_id")
    