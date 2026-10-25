"""Unit tests for WindowTransformer."""

from datetime import date
from decimal import Decimal

import pytest

from src.transform.window import WindowTransformer


@pytest.fixture
def orders(spark):
    return spark.createDataFrame(
        [
            (1, 10, date(2024, 1, 1), Decimal("10.00")),
            (2, 10, date(2024, 1, 11), Decimal("20.00")),
            (3, 10, date(2024, 1, 12), Decimal("5.00")),
            (4, 11, date(2024, 1, 5), Decimal("100.00")),
        ],
        "order_id int, customer_id int, order_date date, order_total decimal(31,2)",
    )


def test_running_total_restarts_for_each_customer(orders):
    result = {r.order_id: r for r in WindowTransformer().customer_running_total(orders).collect()}
    assert [result[i].running_total for i in (1, 2, 3, 4)] == [
        Decimal("10.00"),
        Decimal("30.00"),
        Decimal("35.00"),
        Decimal("100.00"),  # customer 11 starts again from zero
    ]


def test_days_since_last_order_uses_lag_and_lead(orders):
    result = {r.order_id: r for r in WindowTransformer().days_since_last_order(orders).collect()}
    assert result[1].days_since_last_order is None  # first order of customer 10
    assert result[2].days_since_last_order == 10
    assert result[3].days_since_last_order == 1
    assert result[4].days_since_last_order is None  # first order of customer 11
    assert result[2].prev_order_date == date(2024, 1, 1)
    assert result[1].next_order_date == date(2024, 1, 11)
    assert result[3].next_order_date is None  # last order of customer 10


def test_default_transform_adds_running_total_and_recency_columns(orders):
    columns = WindowTransformer().transform(orders).columns
    assert "running_total" in columns
    assert "days_since_last_order" in columns


def test_rank_products_handles_ties_and_restarts_per_category(spark):
    lines = spark.createDataFrame(
        [
            ("Books", 1, "A", Decimal("60.00")),
            ("Books", 1, "A", Decimal("40.00")),  # product A totals 100
            ("Books", 2, "B", Decimal("50.00")),
            ("Books", 3, "C", Decimal("50.00")),  # B and C tie at 50
            ("Books", 4, "D", Decimal("10.00")),
            ("Toys", 5, "E", Decimal("1.00")),
        ],
        "category string, product_id int, product_name string, line_total decimal(10,2)",
    )
    result = {r.product_id: r for r in WindowTransformer().rank_products(lines).collect()}
    assert (result[1].rank, result[1].dense_rank) == (1, 1)
    assert result[2].rank == result[3].rank == 2  # ties share a rank
    assert result[2].dense_rank == result[3].dense_rank == 2
    assert sorted([result[2].row_num, result[3].row_num]) == [2, 3]  # row_number never ties
    assert (result[4].row_num, result[4].rank, result[4].dense_rank) == (4, 4, 3)  # rank skips 3
    assert result[5].row_num == 1  # a new category restarts the ranking