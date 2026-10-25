"""Unit tests for AggregationTransformer."""

from datetime import date
from decimal import Decimal

import pytest

from src.transform.aggregation import AggregationTransformer


@pytest.fixture
def orders(spark):
    return spark.createDataFrame(
        [
            (1, 10, date(2024, 1, 1), Decimal("20.00")),
            (2, 11, date(2024, 1, 1), Decimal("30.00")),
            (3, 10, date(2024, 1, 1), Decimal("10.00")),  # same customer, same day
            (4, 10, date(2024, 1, 2), Decimal("40.00")),
        ],
        "order_id int, customer_id int, order_date date, order_total decimal(31,2)",
    )


@pytest.fixture
def lines(spark):
    return spark.createDataFrame(
        [
            (1, "US", "Books", Decimal("10.00")),
            (1, "US", "Toys", Decimal("5.00")),
            (2, "UK", "Books", Decimal("20.00")),
            (3, "US", "Books", Decimal("7.00")),
        ],
        "order_id int, country string, category string, line_total decimal(10,2)",
    )


def test_daily_revenue_aggregates_each_day(orders):
    rows = {r.order_date: r for r in AggregationTransformer().daily_revenue(orders).collect()}
    day1 = rows[date(2024, 1, 1)]
    assert day1.total_revenue == Decimal("60.00")
    assert day1.avg_order_value == Decimal("20.00")
    assert day1.order_count == 3
    assert day1.unique_customers == 2  # customer 10 ordered twice
    assert (day1.min_order, day1.max_order) == (Decimal("10.00"), Decimal("30.00"))
    assert rows[date(2024, 1, 2)].total_revenue == Decimal("40.00")


def test_rollup_adds_country_subtotals_and_a_grand_total(lines):
    result = AggregationTransformer().sales_rollup(lines).collect()
    rows = {(r.country, r.category): (r.revenue, r.orders) for r in result}
    assert rows[("US", "Books")] == (Decimal("17.00"), 2)
    assert rows[("US", "Toys")] == (Decimal("5.00"), 1)
    assert rows[("UK", "Books")] == (Decimal("20.00"), 1)
    assert rows[("US", None)] == (Decimal("22.00"), 2)  # country subtotal
    assert rows[("UK", None)] == (Decimal("20.00"), 1)
    assert rows[(None, None)] == (Decimal("42.00"), 3)  # grand total
    assert len(rows) == 6


def test_cube_also_adds_category_subtotals(lines):
    result = AggregationTransformer().sales_cube(lines).collect()
    rows = {(r.country, r.category): r.revenue for r in result}
    assert rows[(None, "Books")] == Decimal("37.00")  # Books across all countries
    assert rows[(None, "Toys")] == Decimal("5.00")
    assert len(rows) == 8  # the 6 rollup rows plus 2 category-only subtotals


def test_pivot_makes_one_column_per_country(lines):
    result = AggregationTransformer().category_pivot(lines, values=["UK", "US"])
    assert result.columns == ["category", "UK", "US"]
    rows = {r.category: (r.UK, r.US) for r in result.collect()}
    assert rows["Books"] == (Decimal("20.00"), Decimal("17.00"))
    assert rows["Toys"] == (None, Decimal("5.00"))  # no UK toys were sold


def test_pivot_silently_drops_countries_missing_from_the_values_list(lines):
    result = AggregationTransformer().category_pivot(lines, values=["US"])
    assert result.columns == ["category", "US"]  # UK revenue is simply gone
    total = sum(r.US for r in result.collect() if r.US is not None)
    assert total == Decimal("22.00")  # 42.00 overall minus 20.00 for the UK


def test_default_transform_is_the_daily_revenue_summary(orders):
    assert "total_revenue" in AggregationTransformer().transform(orders).columns