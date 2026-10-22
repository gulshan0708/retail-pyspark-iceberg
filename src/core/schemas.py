"""Explicit schemas for the raw sources (see Step 1 of the design doc).

StructField(name, type) is nullable by default. Spark's file readers make
every column nullable regardless of this flag, so we keep the default.
"""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("signup_date", DateType()),
    StructField("country", StringType()),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_date", DateType()),
    StructField("order_status", StringType()),
    StructField("payment_id", IntegerType()),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(10, 2)),
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("payment_method", StringType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("payment_status", StringType()),
    StructField("payment_date", TimestampType()),
])

# Documentation only: JDBC reads take their types from the Postgres table.
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("unit_cost", DecimalType(10, 2)),
    StructField("current_price", DecimalType(10, 2)),
])