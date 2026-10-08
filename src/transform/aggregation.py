"""Aggregation transformer (Pipeline 3). Order-level methods expect the
enriched orders DataFrame; line-level methods expect the exploded +
product-joined DataFrame (order_date, country, category, line_total...)."""

from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.transform.base import BaseTransformer



class AggregationTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        """Default behaviour so this class satisfies the BaseTransformer
        contract: the daily revenue summary."""
        return self.daily_revenue(df)

    def daily_revenue(self, df: DataFrame) -> DataFrame:
        return df.groupBy("order_date").agg(
            F.sum("order_total").alias("total_revenue"),
            F.avg("order_total").alias("avg_order_value"),
            F.count("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.min("order_total").alias("min_order"),
            F.max("order_total").alias("max_order"),
        )

    def category_pivot(self, df: DataFrame, values: list[str] | None = None) -> DataFrame:
        """Revenue per category, one column per country. Passing `values`
        (the list of countries) saves Spark an extra pass to discover them."""
        grouped = df.groupBy("category")
        pivoted = grouped.pivot("country", values) if values else grouped.pivot("country")
        return pivoted.agg(F.sum("line_total"))

    def sales_rollup(self, df: DataFrame) -> DataFrame:
        return (
            df.rollup("country", "category")
            .agg(
                F.sum("line_total").alias("revenue"),
                F.countDistinct("order_id").alias("orders"),
            )
            .orderBy("country", "category")
        )

    def sales_cube(self, df: DataFrame) -> DataFrame:
        """Like rollup, but subtotals for ALL combinations (category-only
        totals too)."""
        return df.cube("country", "category").agg(F.sum("line_total").alias("revenue"))