"""Incremental reads between snapshots, and an attempt at Iceberg's changelog view.
Run: scripts/submit.sh scripts/iceberg_incremental.py
"""

from pyspark.sql import functions as F

from src.core.spark_session import SparkSessionManager
from src.load.changes import classify_changes
from src.load.IcebergLoader import IcebergLoader


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("ERROR")
        loader = IcebergLoader(spark)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.step10")
        for name in ("events", "cdc"):
            spark.sql(f"DROP TABLE IF EXISTS demo.step10.{name}")

        print("=== 1. Incremental reads: only what was appended since a snapshot")
        events = "step10.events"

        def batch(start, end, label):
            return spark.range(start, end).withColumn("batch", F.lit(label))

        loader.write_bronze(batch(1, 101, 1), events)                     # snapshot 1: 100 rows
        loader.write_bronze(batch(101, 151, 2), events, mode="append")    # snapshot 2:  50 rows
        loader.write_bronze(batch(151, 181, 3), events, mode="append")    # snapshot 3:  30 rows
        ids = loader.snapshot_ids(events)
        print("snapshots:", len(ids), "| total rows:", loader.read_table(events).count())
        for label, df in [
            ("after snapshot 1, up to snapshot 2", loader.read_incremental(events, ids[0], ids[1])),
            ("after snapshot 2 (to the latest)   ", loader.read_incremental(events, ids[1])),
            ("after snapshot 1 (to the latest)   ", loader.read_incremental(events, ids[0])),
        ]:
            print(f"{label}: {df.count()} rows, by batch:", sorted((r.batch, r['count']) for r in df.groupBy('batch').count().collect()))
        print("A job would store the last snapshot id it processed and read from there next time.")

        print("\n--- a delete is not an append, so it is invisible to an incremental read")
        loader.delete_where(events, "id = 5")
        after_delete = loader.snapshot_ids(events)[-1]
        try:
            seen = loader.read_incremental(events, ids[2], after_delete).count()
            print("rows seen between the last append and the delete:", seen)
        except Exception as exc:
            print("incremental read raised:", type(exc).__name__, str(exc).splitlines()[0][:150])

        print("\n=== 2. Capturing inserts, updates and deletes")
        cdc = "step10.cdc"
        loader.write_bronze(spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id int, name string"), cdc)
        first = loader.snapshot_ids(cdc)[-1]
        loader.update_where(cdc, "name = 'B'", "id = 2")
        loader.delete_where(cdc, "id = 3")
        loader.write_bronze(spark.createDataFrame([(4, "d")], "id int, name string"), cdc, mode="append")
        last = loader.snapshot_ids(cdc)[-1]

        print("--- a) Iceberg's changelog view (it may be blocked by the catalog server)")
        try:
            spark.sql(
                "CALL demo.system.create_changelog_view("
                f"table => '{cdc}', "
                f"options => map('start-snapshot-id', '{first}', 'end-snapshot-id', '{last}'), "
                "changelog_view => 'cdc_changes', identifier_columns => array('id'))"
            )
            spark.sql("SELECT id, name, _change_type FROM cdc_changes ORDER BY _change_ordinal, id").show()
        except Exception as exc:
            print("blocked:", type(exc).__name__, str(exc).splitlines()[0][:200])

        print("--- b) The portable way: compare two snapshots by key (what Pipeline 4 does)")
        added, removed = loader.time_travel_report(cdc, first)
        classify_changes(added, removed, ["id"]).orderBy("id").show()


if __name__ == "__main__":
    main()