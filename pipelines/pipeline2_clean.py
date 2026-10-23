"""Pipeline 2: Clean & Enrich.

Reads the bronze tables and builds two silver tables:
  silver.orders_enriched  one row per order: cleaned, customer columns added,
                          line items nested in an `items` array, order_total
  silver.order_lines      one row per order line: items exploded back out and
                          joined to product details
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.core.config import PipelineConfig
from src.core.decorators import log_execution_time
from src.core.pipeline import Pipeline
from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader
from src.transform.cleaning import CleaningTransformer
from src.transform.defaults import DefaultsTransformer
from src.transform.enrichment import EnrichmentTransformer

logger = logging.getLogger(__name__)


@log_execution_time
def run(spark: SparkSession, config: PipelineConfig) -> dict:
    logger.info("Pipeline 2 starting, batch_date=%s", config.batch_date)
    loader = IcebergLoader(spark, catalog_name=config.catalog_name)

    orders = loader.read_table("bronze.orders")
    customers = loader.read_table("bronze.customers")
    items = loader.read_table("bronze.order_items")
    products = loader.read_table("bronze.products")

    enrichment = EnrichmentTransformer(customers, items, products)

    # Composition: the Pipeline holds a list of transformers and runs them in
    # order. It never needs to know which concrete class each step is.
    order_pipeline = (
        Pipeline()
        .add_step(CleaningTransformer())
        .add_step(enrichment)
        .add_step(DefaultsTransformer({"country": "UNKNOWN"}))
    )
    orders_enriched = enrichment.flag_high_value_builtin(order_pipeline.run(orders))
    orders_enriched.cache()  # reused: two outputs plus the counts below

    try:
        order_lines = enrichment.join_products(
            EnrichmentTransformer.explode_items(orders_enriched)
        )

        loader.write_silver(
            orders_enriched,
            "silver.orders_enriched",
            partition_exprs=[F.months("order_date")],
        )
        loader.write_silver(
            order_lines,
            "silver.order_lines",
            partition_exprs=[F.months("order_date"), F.bucket(4, "product_id")],
        )

        stats = {
            "orders_enriched": orders_enriched.count(),
            "order_lines": order_lines.count(),
            "unknown_country_orders": orders_enriched.filter(
                F.col("country") == "UNKNOWN"
            ).count(),
        }
    finally:
        orders_enriched.unpersist()  # released even if a step above failed

    for name, value in stats.items():
        logger.info("%-24s %d", name, value)
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline 2: clean and enrich bronze data")
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