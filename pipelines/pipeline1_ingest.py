"""Pipeline 1: Ingest & Validate.

Reads the five raw sources, validates each one, writes rows that pass to
Iceberg bronze tables and rows that fail to quarantine tables.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.core.config import PipelineConfig
from src.core.decorators import log_execution_time
from src.core.exceptions import DataQualityError
from src.core.schemas import (
    CUSTOMERS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    ORDERS_SCHEMA,
    PAYMENTS_SCHEMA,
    PRODUCTS_SCHEMA,
)
from src.core.spark_session import SparkSessionManager
from src.extract.csv_extractor import CSVExtractor
from src.extract.jdbc_extractor import JDBCExtractor
from src.extract.json_extractor import JSONExtractor
from src.load.IcebergLoader import IcebergLoader
from src.quality.checker import DataQualityChecker

logger = logging.getLogger(__name__)

JDBC_URL = "jdbc:postgresql://postgres:5432/retail"
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
MAX_REMOVED_RATIO = 0.05  # circuit breaker: fail if >5% of an entity is rejected


def tag(df: DataFrame, reason: str) -> DataFrame:
    """Adds a reject_reason column so quarantined rows say why they failed."""
    return df.withColumn("reject_reason", F.lit(reason))


def validate(
    df: DataFrame,
    checker: DataQualityChecker,
    key_cols: list[str],
    required_cols: list[str],
    non_negative_cols: list[str] | None = None,
    pattern_checks: list[tuple[str, str]] | None = None,
    fk_checks: list[tuple[str, DataFrame, str]] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Returns (valid_rows, rejected_rows). Everything here is lazy: no
    Spark job runs until a caller counts or writes the results."""
    # dict.fromkeys removes duplicates while keeping order; keys are always required
    required = list(dict.fromkeys(key_cols + required_cols))

    # 1. Exact duplicates: keep one row per key, quarantine the surplus copies.
    #    exceptAll = rows in df that are not in deduped, counting duplicates.
    deduped = df.dropDuplicates(key_cols)
    duplicate_rows = tag(df.exceptAll(deduped), "duplicate_key")

    # 2. Rule checks, each returning the rows that violate one rule.
    rule_rejects = [tag(checker.check_nulls(deduped, required), "null_required_field")]
    for column, pattern in pattern_checks or []:
        bad = checker.check_pattern(deduped, column, pattern)
        rule_rejects.append(tag(bad, f"bad_format_{column}"))
    for column in non_negative_cols or []:
        bad = checker.check_non_negative(deduped, [column])
        rule_rejects.append(tag(bad, f"negative_{column}"))
    for fk_col, reference_df, reference_key in fk_checks or []:
        orphans = checker.check_fk_integrity(deduped, fk_col, reference_df, reference_key)
        rule_rejects.append(tag(orphans, f"orphan_{fk_col}"))

    # 3. Valid = deduped rows that appear in none of the rule rejects.
    #    (Not subtracting duplicate_rows: they share a key with the survivor.)
    bad_keys = reduce(
        DataFrame.unionByName, [r.select(*key_cols) for r in rule_rejects]
    ).distinct()
    valid = deduped.dropna(subset=required).join(bad_keys, on=key_cols, how="left_anti")
    rejected = reduce(DataFrame.unionByName, [duplicate_rows, *rule_rejects])
    return valid, rejected


def process_entity(
    loader: IcebergLoader,
    checker: DataQualityChecker,
    name: str,
    raw: DataFrame,
    partition_col: str | None = None,
    **rules,
) -> dict:
    """Validates one entity, writes quarantine + bronze tables, returns stats."""
    valid, rejected = validate(raw, checker, **rules)

    raw_count = raw.count()      # actions: these trigger Spark jobs
    valid_count = valid.count()
    removed = raw_count - valid_count
    removed_ratio = removed / raw_count if raw_count else 0.0

    # Post-condition: after validation, keys must be unique.
    checker.raise_if_invalid(
        checker.check_uniqueness(valid, rules["key_cols"]),
        f"{name}: duplicate keys survived validation",
    )

    # Quarantine is written first so you can inspect rejects even if we then fail.
    loader.write_bronze(rejected, f"quarantine.{name}")

    if removed_ratio > MAX_REMOVED_RATIO:
        raise DataQualityError(
            f"{name}: {removed}/{raw_count} rows rejected ({removed_ratio:.1%}), "
            f"limit is {MAX_REMOVED_RATIO:.0%}. Bronze not written."
        )

    loader.write_bronze(valid, f"bronze.{name}", partition_col=partition_col)
    logger.info("%s: raw=%d valid=%d removed=%d", name, raw_count, valid_count, removed)
    return {"entity": name, "raw": raw_count, "valid": valid_count, "removed": removed}


@log_execution_time
def run(spark: SparkSession, config: PipelineConfig) -> list[dict]:
    logger.info("Pipeline 1 starting, batch_date=%s", config.batch_date)
    checker = DataQualityChecker()
    loader = IcebergLoader(spark, catalog_name=config.catalog_name)
    paths = config.source_paths

    # ---- Extract (one extractor class per source type) ----
    customers_raw = JSONExtractor(spark, paths["customers"], CUSTOMERS_SCHEMA).extract()
    products_raw = JDBCExtractor(spark, JDBC_URL, "products", PRODUCTS_SCHEMA).extract()
    orders_raw = CSVExtractor(spark, paths["orders"], ORDERS_SCHEMA).extract()
    items_raw = CSVExtractor(spark, paths["order_items"], ORDER_ITEMS_SCHEMA).extract()
    payments_raw = JSONExtractor(spark, paths["payments"], PAYMENTS_SCHEMA).extract()

    # Each raw DataFrame is used many times below (counts, checks, writes,
    # foreign-key lookups). Without cache(), Spark re-reads the source each time.
    raw_frames = [customers_raw, products_raw, orders_raw, items_raw, payments_raw]
    for df in raw_frames:
        df.cache()

    stats = []
    try:
        checker.profile(orders_raw)  # prints summary statistics (an action)

        stats.append(process_entity(
            loader, checker, "customers", customers_raw,
            key_cols=["customer_id"],
            required_cols=["email"],
            pattern_checks=[("email", EMAIL_REGEX)],
        ))
        stats.append(process_entity(
            loader, checker, "products", products_raw,
            key_cols=["product_id"],
            required_cols=["product_name", "category"],
            non_negative_cols=["unit_cost", "current_price"],
        ))
        stats.append(process_entity(
            loader, checker, "orders", orders_raw,
            partition_col="order_date",
            key_cols=["order_id"],
            required_cols=["customer_id", "order_date"],
            fk_checks=[("customer_id", customers_raw.select("customer_id"), "customer_id")],
        ))
        stats.append(process_entity(
            loader, checker, "order_items", items_raw,
            key_cols=["order_item_id"],
            required_cols=["order_id", "product_id", "quantity"],
            non_negative_cols=["quantity", "unit_price"],
            fk_checks=[
                ("order_id", orders_raw.select("order_id"), "order_id"),
                ("product_id", products_raw.select("product_id"), "product_id"),
            ],
        ))
        stats.append(process_entity(
            loader, checker, "payments", payments_raw,
            key_cols=["payment_id"],
            required_cols=["order_id", "amount"],
            non_negative_cols=["amount"],
            fk_checks=[("order_id", orders_raw.select("order_id"), "order_id")],
        ))
    finally:
        # Runs even if a step above raised, so cached data is always released.
        for df in raw_frames:
            df.unpersist()

    logger.info("%-12s %8s %8s %8s", "entity", "raw", "valid", "removed")
    for s in stats:
        logger.info("%-12s %8d %8d %8d", s["entity"], s["raw"], s["valid"], s["removed"])
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline 1: ingest and validate raw data")
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
    config = PipelineConfig(
        batch_date=args.batch_date,
        source_paths={
            "customers": "s3a://raw/customers/",
            "orders": "s3a://raw/orders/",
            "order_items": "s3a://raw/order_items/",
            "payments": "s3a://raw/payments/",
        },
    )
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")  # hide Spark's INFO noise
        run(spark, config)


if __name__ == "__main__":
    main()