"""Integration tests for the row-level, evolution and maintenance methods of IcebergLoader."""

import pytest

pytestmark = pytest.mark.integration


def test_delete_where_removes_rows_but_an_old_snapshot_still_has_them(spark_it, loader):
    table = "it_scratch.rows_to_delete"
    loader.write_bronze(spark_it.createDataFrame([(1, "a"), (2, "b")], "id int, name string"), table)
    before = loader.snapshot_ids(table)[-1]

    loader.delete_where(table, "id = 1")

    assert [row.id for row in loader.read_table(table).collect()] == [2]
    assert loader.read_as_of(table, snapshot_id=before).count() == 2  # time travel still sees row 1


def test_update_where_changes_only_the_matching_rows(spark_it, loader):
    table = "it_scratch.rows_to_update"
    loader.write_bronze(spark_it.createDataFrame([(1, "Toys"), (2, "Books")], "id int, category string"), table)

    loader.update_where(table, "category = 'Toys & Games'", "category = 'Toys'")

    rows = {row.id: row.category for row in loader.read_table(table).collect()}
    assert rows == {1: "Toys & Games", 2: "Books"}


def test_compact_reduces_the_number_of_data_files(spark_it, loader):
    table = "it_scratch.small_files"
    loader.write_bronze(spark_it.range(400).repartition(4), table)  # 4 tasks, so about 4 files
    files_before = len(loader.data_files(table))
    assert files_before >= 2

    loader.compact(table, options={"min-input-files": "2"})

    assert len(loader.data_files(table)) < files_before
    assert loader.read_table(table).count() == 400  # no rows lost


def test_expire_snapshots_keeps_only_the_latest(spark_it, loader):
    table = "it_scratch.expiring"
    loader.write_bronze(spark_it.createDataFrame([(1,)], "id int"), table)
    loader.write_bronze(spark_it.createDataFrame([(2,)], "id int"), table, mode="append")
    assert len(loader.snapshot_ids(table)) == 2

    loader.expire_snapshots(table, retain_last=1)

    assert len(loader.snapshot_ids(table)) == 1
    assert loader.read_table(table).count() == 2  # the data itself is untouched


def test_schema_evolution_adds_a_column_that_is_null_for_old_rows(spark_it, loader):
    table = "it_scratch.evolving"
    loader.write_bronze(spark_it.createDataFrame([(1,), (2,)], "id int"), table)

    spark_it.sql("ALTER TABLE demo.it_scratch.evolving ADD COLUMN tier string")
    assert loader.read_table(table).filter("tier IS NOT NULL").count() == 0

    loader.update_where(table, "tier = 'gold'", "id = 1")
    tiers = {row.id: row.tier for row in loader.read_table(table).collect()}
    assert tiers == {1: "gold", 2: None}

def test_wap_write_lands_on_the_branch_until_it_is_published(spark_it, loader):
    table = "it_scratch.wap_table"
    loader.write_bronze(spark_it.createDataFrame([(1,)], "id int"), table)
    loader.enable_wap(table)
    loader.create_branch(table, "audit")

    with loader.wap_branch("audit"):
        loader.write_bronze(spark_it.createDataFrame([(2,)], "id int"), table, mode="append")

    assert loader.read_table(table).count() == 1              # main is untouched
    assert loader.read_branch(table, "audit").count() == 2    # the new row is on the branch

    loader.fast_forward_branch(table, "audit")
    assert loader.read_table(table).count() == 2              # published


def test_incremental_read_returns_only_rows_appended_between_snapshots(spark_it, loader):
    table = "it_scratch.appends"
    loader.write_bronze(spark_it.createDataFrame([(1,), (2,)], "id int"), table)
    loader.write_bronze(spark_it.createDataFrame([(3,)], "id int"), table, mode="append")
    loader.write_bronze(spark_it.createDataFrame([(4,), (5,)], "id int"), table, mode="append")
    ids = loader.snapshot_ids(table)

    between = loader.read_incremental(table, ids[0], ids[1])
    since_first = loader.read_incremental(table, ids[0])

    assert sorted(row.id for row in between.collect()) == [3]
    assert sorted(row.id for row in since_first.collect()) == [3, 4, 5]


def test_metadata_location_is_a_metadata_json_file(spark_it, loader):
    table = "it_scratch.located"
    loader.write_bronze(spark_it.createDataFrame([(1,)], "id int"), table)
    location = loader.metadata_location(table)
    assert location.startswith("s3://") and location.endswith(".metadata.json")