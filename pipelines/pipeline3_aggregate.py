"""Pipeline 3: Aggregate & Window.

Reads the silver tables and builds five gold tables, each written with
MERGE INTO so re-running the same batch never creates duplicates:

  gold.daily_sales                one row per order_date
  gold.sales_by_country_category  rollup of revenue by country and category (with subtotals)
  gold.category_country_pivot     one row per category, one revenue column per country
  gold.product_rankings           product revenue ranked within each category
  gold.customer_order_history     per order: customer running total, days since previous order
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession

from src.core.config import PipelineConfig
from src.core.decorators import log_execution_time
from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader
from src.transform.aggregation import AggregationTransformer
from src.transform.window import WindowTransformer

logger = logging.getLogger(__name__)

# The columns that identify one row in each gold table (the MERGE key).
GOLD_TABLE_KEYS = {
    "gold.daily_sales": ["order_date"],
    "gold.sales_by_country_category": ["country", "category"],
    "gold.category_country_pivot": ["category"],
    "gold.product_rankings": ["category", "product_id"],
    "gold.customer_order_history": ["order_id"],
}

# Explicit pivot values keep the table's columns identical on every run.
# A country missing from this list would silently disappear from the pivot;
# scripts/verify_gold.py reconciles totals to catch that.
PIVOT_COUNTRIES = ["DE", "FR", "IN", "PL", "UK", "US", "UNKNOWN"]


@log_execution_time
def run(spark: SparkSession, config: PipelineConfig) -> dict:
    logger.info("Pipeline 3 starting, batch_date=%s", config.batch_date)
    loader = IcebergLoader(spark, catalog_name=config.catalog_name)
    aggregation = AggregationTransformer()
    window = WindowTransformer()

    # The nested items array isn't needed here, so select only what we use.
    orders = loader.read_table("silver.orders_enriched").select(
        "order_id", "customer_id", "order_date", "order_total"
    )
    lines = loader.read_table("silver.order_lines")
    # Each silver table feeds several outputs, so keep them in memory.
    orders.cache()
    lines.cache()

    try:
        pivot = aggregation.category_pivot(lines, PIVOT_COUNTRIES)
        for country in PIVOT_COUNTRIES:
            pivot = pivot.withColumnRenamed(country, f"revenue_{country.lower()}")

        outputs = {
            "gold.daily_sales": aggregation.daily_revenue(orders),
            "gold.sales_by_country_category": aggregation.sales_rollup(lines),
            "gold.category_country_pivot": pivot,
            "gold.product_rankings": window.rank_products(lines),
            "gold.customer_order_history": window.transform(orders),
        }
        for table, df in outputs.items():
            loader.merge_gold(df, table, GOLD_TABLE_KEYS[table])
            logger.info("merged %s", table)

        stats = {table: loader.read_table(table).count() for table in outputs}
    finally:
        orders.unpersist()
        lines.unpersist()

    for table, rows in stats.items():
        logger.info("%-34s %d rows", table, rows)
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline 3: aggregate and window")
    parser.add_argument(
        "--batch-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Logical date of this run as YYYY-MM-DD (default: today)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    config = PipelineConfig(batch_date=args.batch_date)
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        run(spark, config)


if __name__ == "__main__":
    main()