"""Unit tests for CleaningTransformer."""

from datetime import date

from chispa import assert_df_equality

from src.transform.cleaning import CleaningTransformer

RAW = "order_id int, customer_id int, order_date date, order_status string, payment_id int"
CLEAN = "order_id int, customer_id int, order_date date, status string, payment_id int"


def test_status_is_trimmed_lowercased_and_renamed(spark):
    raw = spark.createDataFrame(
        [
            (1, 10, date(2024, 1, 1), "  SHIPPED ", 100),
            (2, 11, date(2024, 1, 2), "Delivered", 101),
        ],
        RAW,
    )
    result = CleaningTransformer().transform(raw)
    expected = spark.createDataFrame(
        [
            (1, 10, date(2024, 1, 1), "shipped", 100),
            (2, 11, date(2024, 1, 2), "delivered", 101),
        ],
        CLEAN,
    )
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


def test_unrecognised_status_becomes_unknown(spark):
    raw = spark.createDataFrame([(1, 10, date(2024, 1, 1), "teleported", 100)], RAW)
    result = CleaningTransformer().transform(raw)
    assert result.first().status == "unknown"


def test_duplicate_order_ids_are_removed(spark):
    row = (1, 10, date(2024, 1, 1), "placed", 100)
    raw = spark.createDataFrame([row, row], RAW)
    assert CleaningTransformer().transform(raw).count() == 1


def test_output_has_exactly_the_silver_columns(spark):
    raw = spark.createDataFrame([(1, 10, date(2024, 1, 1), "placed", 100)], RAW)
    result = CleaningTransformer().transform(raw)
    assert result.columns == ["order_id", "customer_id", "order_date", "status", "payment_id"]