"""Integration tests for IcebergLoader against the real REST catalog and storage.
Everything is written under the scratch namespace `it_scratch`."""

from datetime import date

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from src.load.changes import classify_changes

pytestmark = pytest.mark.integration  # marks every test in this file

AMOUNTS = "id int, amount int"


@pytest.fixture
def history_table(spark_it, loader):
    """A table with two snapshots: day 1 created it, day 2 changed id 2 and added id 3.
    (Not named "history": that is an Iceberg metadata table name and confuses the REST catalog.)"""
    spark_it.sql("DROP TABLE IF EXISTS demo.it_scratch.versions")  # clean start for each test
    day1 = spark_it.createDataFrame([(1, 10), (2, 20)], AMOUNTS)
    day2 = spark_it.createDataFrame([(2, 99), (3, 30)], AMOUNTS)
    loader.merge_gold(day1, "it_scratch.versions", ["id"])
    loader.merge_gold(day2, "it_scratch.versions", ["id"])
    return "it_scratch.versions", day1, day2


def test_write_bronze_creates_a_table_partitioned_by_day(spark_it, loader):
    df = spark_it.createDataFrame(
        [(1, date(2024, 1, 1)), (2, date(2024, 1, 2))], "id int, order_date date"
    )
    loader.write_bronze(df, "it_scratch.partitioned", partition_col="order_date")

    result = loader.read_table("it_scratch.partitioned")
    assert_df_equality(result, df, ignore_row_order=True, ignore_nullable=True)

    # Flatten every cell of DESCRIBE's output into one string and look for the transform.
    description = " ".join(
        str(cell)
        for row in spark_it.sql("DESCRIBE TABLE EXTENDED demo.it_scratch.partitioned").collect()
        for cell in row
    )
    assert "days(order_date)" in description


def test_write_bronze_overwrite_replaces_the_previous_contents(spark_it, loader):
    first = spark_it.createDataFrame([(1,), (2,), (3,)], "id int")
    second = spark_it.createDataFrame([(9,)], "id int")
    loader.write_bronze(first, "it_scratch.refresh")
    loader.write_bronze(second, "it_scratch.refresh")
    assert [row.id for row in loader.read_table("it_scratch.refresh").collect()] == [9]


def test_merge_gold_inserts_updates_and_is_idempotent(spark_it, loader):
    day1 = spark_it.createDataFrame([(1, 10), (2, 20)], AMOUNTS)
    day2 = spark_it.createDataFrame([(2, 99), (3, 30)], AMOUNTS)

    loader.merge_gold(day1, "it_scratch.sales", ["id"])  # the first call creates the table
    loader.merge_gold(day1, "it_scratch.sales", ["id"])  # same data again: nothing duplicated
    assert loader.read_table("it_scratch.sales").count() == 2

    loader.merge_gold(day2, "it_scratch.sales", ["id"])
    rows = {row.id: row.amount for row in loader.read_table("it_scratch.sales").collect()}
    assert rows == {1: 10, 2: 99, 3: 30}  # 1 untouched, 2 updated, 3 inserted


def test_merge_gold_matches_rows_whose_key_is_null(spark_it, loader):
    # The rollup table has subtotal rows with a null key. A plain "=" never matches
    # null, so every re-run would add them again. The null-safe "<=>" prevents that.
    df = spark_it.createDataFrame([("US", 5), (None, 5)], "country string, revenue int")
    loader.merge_gold(df, "it_scratch.nullkeys", ["country"])
    loader.merge_gold(df, "it_scratch.nullkeys", ["country"])
    assert loader.read_table("it_scratch.nullkeys").count() == 2


def test_snapshots_are_listed_oldest_first_and_readable_by_id(spark_it, loader, history_table):
    table, day1, _ = history_table
    snapshot_ids = loader.snapshot_ids(table)
    assert len(snapshot_ids) >= 2  # one to create the table, one for the second merge
    assert loader.list_snapshots(table).count() == len(snapshot_ids)

    first_version = loader.read_as_of(table, snapshot_id=snapshot_ids[0])
    assert_df_equality(first_version, day1, ignore_row_order=True, ignore_nullable=True)


def test_change_report_labels_updated_and_inserted_rows(spark_it, loader, history_table):
    table, _, _ = history_table
    first_snapshot = loader.snapshot_ids(table)[0]
    added, removed = loader.time_travel_report(table, first_snapshot)
    rows = classify_changes(added, removed, ["id"]).collect()
    assert {row.id: row.change for row in rows} == {2: "updated", 3: "inserted"}


def test_read_as_of_a_timestamp(spark_it, loader, history_table):
    table, day1, _ = history_table
    first_commit_ms = (
        loader.list_snapshots(table)
        .select(F.expr("unix_millis(committed_at)").alias("ms"))
        .first()
        .ms
    )
    result = loader.read_as_of(table, timestamp_ms=first_commit_ms)
    assert_df_equality(result, day1, ignore_row_order=True, ignore_nullable=True)