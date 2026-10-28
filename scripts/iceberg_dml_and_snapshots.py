"""Row-level DELETE/UPDATE, copy-on-write vs merge-on-read, and why a delete is not an erasure.
Run: scripts/submit.sh scripts/iceberg_dml_and_snapshots.py
"""

from src.core.spark_session import SparkSessionManager
from src.load.IcebergLoader import IcebergLoader

SUMMARY_KEYS = [
    "operation", "added-data-files", "deleted-data-files",
    "added-delete-files", "added-records", "deleted-records", "total-records",
]


def show_summary(loader, table, label) -> None:
    summary = loader.snapshot_summary(table)
    print(f"  {label}: {({key: summary.get(key) for key in SUMMARY_KEYS})}")
    print(f"  files: {[(f['records'], f['delete_files']) for f in loader.data_files(table)]}  (records, delete files)")


def main() -> None:
    with SparkSessionManager.from_env() as spark:
        spark.sparkContext.setLogLevel("WARN")
        loader = IcebergLoader(spark)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.step10")
        for name in ("customers", "products"):
            spark.sql(f"DROP TABLE IF EXISTS demo.step10.{name}")
        customers, products = "step10.customers", "step10.products"
        loader.write_bronze(loader.read_table("bronze.customers"), customers)
        loader.write_bronze(loader.read_table("bronze.products"), products)

        def in_table(table, customer_id) -> int:
            return loader.read_table(table).filter(f"customer_id = {customer_id}").count()

        print("=== 1. DELETE with copy-on-write (the default): a customer asks to be erased")
        before_delete = loader.snapshot_ids(customers)[-1]
        print("rows before:", loader.read_table(customers).count())
        loader.delete_where(customers, "customer_id = 7")
        print("rows after: ", loader.read_table(customers).count())
        show_summary(loader, customers, "snapshot")

        print("\n=== 2. ...but the data is still reachable through time travel")
        old = loader.read_as_of(customers, snapshot_id=before_delete)
        print("customer 7 in the old snapshot:", old.filter("customer_id = 7").count(),
              "| in the current table:", in_table(customers, 7))

        print("\n=== 3. UPDATE: correct a category value")
        loader.update_where(products, "category = 'Toys & Games'", "category = 'Toys'")
        loader.read_table(products).groupBy("category").count().orderBy("category").show()
        show_summary(loader, products, "snapshot")

        print("\n=== 4. DELETE with merge-on-read: small delete files instead of rewritten data files")
        loader.set_write_mode(customers, "merge-on-read")
        loader.delete_where(customers, "customer_id = 8")
        print("customer 8 now:", in_table(customers, 8))
        show_summary(loader, customers, "snapshot")

        print("\n=== 5. Compaction applies the delete files to the data files")
        print("  result:", loader.compact(customers, options={"delete-file-threshold": "1"}))
        show_summary(loader, customers, "snapshot")

        print("\n=== 6. expire_snapshots makes the deleted data unreachable")
        print("snapshots before:", len(loader.snapshot_ids(customers)))
        print("  result:", loader.expire_snapshots(customers, retain_last=1))
        print("snapshots after: ", len(loader.snapshot_ids(customers)))
        try:
            loader.read_as_of(customers, snapshot_id=before_delete).count()
            print("old snapshot is STILL readable")
        except Exception as exc:
            print("old snapshot is gone:", type(exc).__name__)


if __name__ == "__main__":
    main()