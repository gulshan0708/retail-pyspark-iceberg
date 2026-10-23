"""Unit tests for DataQualityChecker. Each test builds a tiny DataFrame in
memory, so no Docker, storage or catalog is involved."""

import pytest
from chispa import assert_df_equality

from src.core.exceptions import DataQualityError
from src.quality.checker import DataQualityChecker


def test_check_nulls_returns_rows_with_a_null_in_any_required_column(spark):
    df = spark.createDataFrame(
        [(1, "a@x.com"), (2, None), (None, "c@x.com")], "id int, email string"
    )
    result = DataQualityChecker.check_nulls(df, ["id", "email"])
    expected = spark.createDataFrame([(2, None), (None, "c@x.com")], "id int, email string")
    assert_df_equality(result, expected, ignore_row_order=True)


def test_check_nulls_ignores_columns_that_are_not_required(spark):
    df = spark.createDataFrame([(1, None)], "id int, note string")
    assert DataQualityChecker.check_nulls(df, ["id"]).count() == 0


def test_check_uniqueness_returns_every_copy_of_a_duplicated_key(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], "id int, val string")
    result = DataQualityChecker.check_uniqueness(df, ["id"])
    expected = spark.createDataFrame([(1, "a"), (1, "b")], "id int, val string")
    assert_df_equality(result, expected, ignore_row_order=True, ignore_column_order=True)


def test_check_pattern_returns_rows_that_do_not_match(spark):
    df = spark.createDataFrame(
        [(1, "ok@example.com"), (2, "not-an-email")], "id int, email string"
    )
    result = DataQualityChecker.check_pattern(df, "email", r"^[^@]+@[^@]+\.[a-z]+$")
    expected = spark.createDataFrame([(2, "not-an-email")], "id int, email string")
    assert_df_equality(result, expected, ignore_row_order=True)


def test_check_pattern_does_not_flag_nulls(spark):
    # A null is not a pattern failure: check_nulls is responsible for nulls.
    df = spark.createDataFrame([(1, None)], "id int, email string")
    assert DataQualityChecker.check_pattern(df, "email", r"^[^@]+@[^@]+$").count() == 0


def test_check_non_negative_flags_a_negative_in_any_listed_column(spark):
    df = spark.createDataFrame(
        [(1, 5, 2.0), (2, -3, 2.0), (3, 5, -1.0)], "id int, qty int, price double"
    )
    result = DataQualityChecker.check_non_negative(df, ["qty", "price"])
    assert sorted(row.id for row in result.collect()) == [2, 3]


def test_check_fk_integrity_returns_only_orphans(spark):
    orders = spark.createDataFrame([(1, 10), (2, 99)], "order_id int, customer_id int")
    customers = spark.createDataFrame([(10,)], "customer_id int")
    result = DataQualityChecker().check_fk_integrity(
        orders, "customer_id", customers, "customer_id"
    )
    expected = spark.createDataFrame([(2, 99)], "order_id int, customer_id int")
    assert_df_equality(result, expected)


def test_raise_if_invalid_raises_with_the_row_count(spark):
    bad_rows = spark.createDataFrame([(1,), (2,)], "id int")
    with pytest.raises(DataQualityError, match="2 invalid row"):
        DataQualityChecker().raise_if_invalid(bad_rows, "orders")


def test_raise_if_invalid_is_silent_when_there_are_no_bad_rows(spark):
    no_bad_rows = spark.createDataFrame([], "id int")
    DataQualityChecker().raise_if_invalid(no_bad_rows, "orders")  # must not raise