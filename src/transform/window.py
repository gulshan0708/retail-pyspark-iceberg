"""Window-function transformer (Pipeline 3)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from src.transform.base import BaseTransformer


class WindowTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        """Default: enrich order-level data with running total and
        recency columns."""
        df = self.customer_running_total(df)
        df = self.days_since_last_order(df)
        return df

    def customer_running_total(self, df: DataFrame) -> DataFrame:
        w = (
            Window.partitionBy("customer_id")
            .orderBy("order_date", "order_id")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        return df.withColumn("running_total", F.sum("order_total").over(w))

    def rank_products(self, df: DataFrame) -> DataFrame:
        """Expects line-level data with category/product columns. Compare
        the three ranking functions on tied revenue: row_number never
        ties, rank leaves gaps after ties, dense_rank has no gaps."""
        revenue = df.groupBy("category", "product_id", "product_name").agg(
            F.sum("line_total").alias("revenue")
        )
        w = Window.partitionBy("category").orderBy(F.col("revenue").desc())
        return (
            revenue
            .withColumn("row_num", F.row_number().over(w))
            .withColumn("rank", F.rank().over(w))
            .withColumn("dense_rank", F.dense_rank().over(w))
        )

    def days_since_last_order(self, df: DataFrame) -> DataFrame:
        """lag looks at a previous row in the window, lead at a following
        one. datediff subtracts two dates into a number of days."""
        w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
        return (
            df.withColumn("prev_order_date", F.lag("order_date", 1).over(w))
            .withColumn("next_order_date", F.lead("order_date", 1).over(w))
            .withColumn(
                "days_since_last_order",
                F.datediff(F.col("order_date"), F.col("prev_order_date")),
            )
        )