"""Unit tests for EnrichmentTransformer."""

from datetime import date
from decimal import Decimal

import pytest

from src.transform.enrichment import EnrichmentTransformer

CUSTOMERS = (
    "customer_id int, first_name string, last_name string, "
    "email string, signup_date date, country string"
)
ORDERS = "order_id int, customer_id int, order_date date, status string, payment_id int"
ITEMS = (
    "order_item_id int, order_id int, product_id int, "
    "quantity int, unit_price decimal(10,2)"
)
PRODUCTS = (
    "product_id int, product_name string, category string, brand string, "
    "unit_cost decimal(10,2), current_price decimal(10,2)"
)


@pytest.fixture
def customers(spark):
    return spark.createDataFrame(
        [(10, "Anna", "Kim", "anna@x.com", date(2023, 1, 1), "US")], CUSTOMERS
    )


@pytest.fixture
def orders(spark):
    # Order 2 belongs to customer 99, who does not exist: an unmatched order.
    return spark.createDataFrame(
        [
            (1, 10, date(2024, 1, 1), "placed", 100),
            (2, 99, date(2024, 1, 2), "shipped", 101),
        ],
        ORDERS,
    )


@pytest.fixture
def items(spark):
    # Order 1 has two lines (20.00 + 5.50). Order 2 has none.
    return spark.createDataFrame(
        [
            (1, 1, 1, 2, Decimal("10.00")),
            (2, 1, 2, 1, Decimal("5.50")),
        ],
        ITEMS,
    )


@pytest.fixture
def products(spark):
    return spark.createDataFrame(
        [
            (1, "Widget", "Home", "Acme", Decimal("5.00"), Decimal("10.00")),
            (2, "Gadget", "Electronics", "Globex", Decimal("2.00"), Decimal("5.50")),
        ],
        PRODUCTS,
    )


@pytest.fixture
def enrichment(customers, items, products):
    # A fixture can depend on other fixtures: pytest builds them first.
    return EnrichmentTransformer(customers, items, products)


@pytest.mark.parametrize(
    "how, expected_rows",
    [("inner", 1), ("left", 2), ("right", 1), ("outer", 2), ("left_semi", 1), ("left_anti", 1)],
)
def test_join_customers_row_counts_by_join_type(enrichment, orders, how, expected_rows):
    assert enrichment.join_customers(orders, how=how).count() == expected_rows


def test_left_join_keeps_unmatched_orders_with_null_customer_columns(enrichment, orders):
    result = enrichment.join_customers(orders, how="left")
    unmatched = result.filter("order_id = 2").first()
    assert result.count() == 2
    assert unmatched.first_name is None


def test_nest_order_items_builds_array_and_total(enrichment, orders):
    order1 = enrichment.nest_order_items(orders).filter("order_id = 1").first()
    assert {item.product_id for item in order1["items"]} == {1, 2}
    assert order1["order_total"] == Decimal("25.50")


def test_order_without_items_gets_null_array_and_zero_total(enrichment, orders):
    order2 = enrichment.nest_order_items(orders).filter("order_id = 2").first()
    assert order2["items"] is None
    assert order2["order_total"] == 0


def test_explode_items_gives_one_row_per_line_and_drops_orders_without_items(
    enrichment, orders
):
    enriched = enrichment.nest_order_items(enrichment.join_customers(orders))
    lines = EnrichmentTransformer.explode_items(enriched)
    assert lines.count() == 2  # order 2 had no items, so explode drops it
    assert sorted(row.line_total for row in lines.collect()) == [
        Decimal("5.50"),
        Decimal("20.00"),
    ]


def test_join_products_adds_product_columns_and_drops_unknown_products(spark, enrichment):
    lines = spark.createDataFrame([(1, 1), (2, 999)], "order_id int, product_id int")
    result = enrichment.join_products(lines)
    assert result.count() == 1
    assert result.first().product_name == "Widget"


def test_high_value_udf_and_builtin_agree(spark, enrichment):
    df = spark.createDataFrame(
        [(1, Decimal("500.00")), (2, Decimal("500.01")), (3, Decimal("10.00"))],
        "order_id int, order_total decimal(31,2)",
    )
    via_udf = {r.order_id: r.is_high_value for r in enrichment.flag_high_value(df).collect()}
    via_builtin = {
        r.order_id: r.is_high_value for r in enrichment.flag_high_value_builtin(df).collect()
    }
    # 500.00 is not high value: the rule is "greater than 500"
    assert via_udf == via_builtin == {1: False, 2: True, 3: False}


def test_a_null_total_is_false_for_the_udf_but_null_for_the_builtin(spark, enrichment):
    df = spark.createDataFrame([(1, None)], "order_id int, order_total decimal(31,2)")
    assert enrichment.flag_high_value(df).first().is_high_value is False
    assert enrichment.flag_high_value_builtin(df).first().is_high_value is None