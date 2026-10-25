"""Unit tests for classify_changes."""

from src.load.changes import classify_changes


def test_classify_changes_labels_inserted_updated_and_deleted_rows(spark):
    removed = spark.createDataFrame([(1, 5), (2, 20)], "id int, val int")  # only in the old version
    added = spark.createDataFrame([(1, 10), (3, 30)], "id int, val int")   # only in the new version
    rows = classify_changes(added, removed, ["id"]).collect()
    assert {r.id: (r.val, r.change) for r in rows} == {
        1: (10, "updated"),   # key 1 is in both: the row changed from 5 to 10
        2: (20, "deleted"),   # key 2 only in the old version
        3: (30, "inserted"),  # key 3 only in the new version
    }


def test_classify_changes_is_empty_when_nothing_changed(spark):
    nothing = spark.createDataFrame([], "id int, val int")
    assert classify_changes(nothing, nothing, ["id"]).count() == 0