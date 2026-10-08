"""Enrichment transformer: joins, nesting order items, exploding them
back out, joining products, and a UDF example."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from src.transform.base import BaseTransformer

HIGH_VALUE_THRESHOLD = 500.0


@F.udf(returnType=BooleanType())
def _is_high_value(total):
    # Spark passes decimals in as Python Decimal; Decimal > float raises
    # a TypeError, so convert explicitly.
    return total is not None and float(total) > HIGH_VALUE_THRESHOLD


class EnrichmentTransformer(BaseTransformer):
    def __init__(
        self,
        customers_df: DataFrame,
        order_items_df: DataFrame,
        products_df: DataFrame,
    ):
        # Protected attributes (leading underscore = "internal, don't touch")
        self._customers = customers_df
        self._order_items = order_items_df
        self._products = products_df

    def transform(self, df: DataFrame) -> DataFrame:
        """Default enrichment for the orders stream: add customer columns,
        then nest the line items and compute order_total."""
        df = self.join_customers(df)
        df = self.nest_order_items(df)
        return df

    def join_customers(
        self, orders_df: DataFrame, how: str = "left", use_broadcast: bool = True
    ) -> DataFrame:
        """`how` accepts inner/left/right/outer/left_semi/left_anti so you
        can try every join type. Joining on the column NAME
        (on="customer_id") keeps one copy of the key instead of two."""
        right = F.broadcast(self._customers) if use_broadcast else self._customers
        return orders_df.join(right, on="customer_id", how=how)

    def nest_order_items(self, orders_df: DataFrame) -> DataFrame:
        """Builds the nested array: one row per order with an `items`
        array of structs, plus order_total."""
        items = self._order_items.groupBy("order_id").agg(
            F.collect_list(
                F.struct("order_item_id", "product_id", "quantity", "unit_price")
            ).alias("items"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("order_total"),
        )
        return (
            orders_df.join(items, on="order_id", how="left")
            # orders with no items get null totals; make that 0
            .withColumn("order_total", F.coalesce(F.col("order_total"), F.lit(0)))
        )

    @staticmethod
    def explode_items(df: DataFrame) -> DataFrame:
        """One row per line item. @staticmethod: no `self` needed, pure
        DataFrame-in, DataFrame-out."""
        exploded = df.withColumn("item", F.explode(F.col("items")))
        return exploded.select(
            "order_id",
            "customer_id",
            "order_date",
            "country",
            F.col("item.order_item_id").alias("order_item_id"),
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.unit_price").alias("unit_price"),
            (F.col("item.quantity") * F.col("item.unit_price")).alias("line_total"),
        )

    def join_products(self, items_df: DataFrame) -> DataFrame:
        """Adds product_name, category, brand to line items. Inner join:
        items pointing at unknown products are dropped (the quality
        checker's FK check in Pipeline 1 should already have flagged them)."""
        products = self._products.select("product_id", "product_name", "category", "brand")
        return items_df.join(F.broadcast(products), on="product_id", how="inner")

    def flag_high_value(self, df: DataFrame) -> DataFrame:
        """UDF version. Note the behaviour difference: a null total gives
        False here, but null in the built-in version below."""
        return df.withColumn("is_high_value", _is_high_value(F.col("order_total")))

    def flag_high_value_builtin(self, df: DataFrame) -> DataFrame:
        """Same logic with a built-in comparison. Faster, optimizer-
        friendly. Prefer this whenever built-ins can express the logic."""
        return df.withColumn("is_high_value", F.col("order_total") > HIGH_VALUE_THRESHOLD)