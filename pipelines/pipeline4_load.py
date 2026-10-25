"""Pipeline 4: Load & Publish.

The gold Iceberg tables are already the primary deliverable: any engine that
reads Iceberg (Spark, Trino, DuckDB, ...) can query them directly. This
pipeline adds two things:

  1. A serving layer: two small summary tables copied into Postgres, for
     dashboards that need fast, simple lookups.
  2. A change report: what changed in gold.daily_sales since the previous
     snapshot, found with Iceberg time travel.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.core.config import PipelineConfig
from src.core.decorators import log_execution_time
from src.core.spark_session import SparkSessionManager
from src.load.changes import classify_changes
from src.load.IcebergLoader import IcebergLoader
from src.load.PostgresLoader import PostgresLoader

logger = logging.getLogger(__name__)

TOP_N = 3                       # products per category exported to Postgres
REPORT_TABLE = "gold.daily_sales"
REPORT_KEYS = ["order_date"]


def change_report(
    loader: IcebergLoader,
    table: str,
    key_cols: list[str],
    since_snapshot_id: int | None = None,
) -> dict:
    """Compares the table's latest snapshot with an earlier one (default: the
    one just before the latest) and prints which rows changed."""
    loader.list_snapshots(table).show(truncate=False)
    snapshot_ids = loader.snapshot_ids(table)

    if since_snapshot_id is None:
        if len(snapshot_ids) < 2:
            logger.info("%s has %d snapshot(s): nothing to compare yet", table, len(snapshot_ids))
            return {"inserted": 0, "updated": 0, "deleted": 0}
        since_snapshot_id = snapshot_ids[-2]  # negative index: second from the end

    added, removed = loader.time_travel_report(table, since_snapshot_id)
    changes = classify_changes(added, removed, key_cols).cache()
    try:
        counts = {row["change"]: row["count"] for row in changes.groupBy("change").count().collect()}
        changes.orderBy("change", *key_cols).show(30, truncate=False)
    finally:
        changes.unpersist()
    return {name: counts.get(name, 0) for name in ("inserted", "updated", "deleted")}


@log_execution_time
def run(spark: SparkSession, config: PipelineConfig) -> dict:
    logger.info("Pipeline 4 starting, batch_date=%s", config.batch_date)
    loader = IcebergLoader(spark, catalog_name=config.catalog_name)
    postgres = PostgresLoader(config.jdbc_url)

    # 1. Serving layer: export small, query-friendly summaries to Postgres.
    top_products = (
        loader.read_table("gold.product_rankings")
        .filter(F.col("row_num") <= TOP_N)
        .select(
            "category",
            F.col("row_num").alias("rank_in_category"),
            "product_id",
            "product_name",
            "revenue",
        )
    )
    exports = {
        "daily_sales_summary": loader.read_table("gold.daily_sales"),
        "top_products_by_category": top_products,
    }
    exported = {}
    for table, df in exports.items():
        postgres.export_summary(df, table)
        exported[table] = df.count()
        logger.info("exported %s (%d rows)", table, exported[table])

    # 2. Capstone: what did the latest run change in gold.daily_sales?
    changes = change_report(loader, REPORT_TABLE, REPORT_KEYS)
    logger.info("changes in %s since the previous snapshot: %s", REPORT_TABLE, changes)
    return {"exported": exported, "changes": changes}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline 4: load and publish")
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