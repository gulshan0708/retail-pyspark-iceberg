"""Compares two versions of a table and labels each difference."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def classify_changes(added: DataFrame, removed: DataFrame, key_cols: list[str]) -> DataFrame:
    """`added` = rows in the new version but not the old; `removed` = the reverse
    (exactly what IcebergLoader.time_travel_report returns).

    - inserted: the key appears only in `added`
    - updated:  the key appears in both (we keep the new version of the row)
    - deleted:  the key appears only in `removed`

    Join on key columns by name: a null key never matches, so use this on
    tables whose keys are never null (not on the rollup table)."""
    inserted = added.join(removed, on=key_cols, how="left_anti").withColumn(
        "change", F.lit("inserted")
    )
    updated = added.join(removed, on=key_cols, how="left_semi").withColumn(
        "change", F.lit("updated")
    )
    deleted = removed.join(added, on=key_cols, how="left_anti").withColumn(
        "change", F.lit("deleted")
    )
    return inserted.unionByName(updated).unionByName(deleted)