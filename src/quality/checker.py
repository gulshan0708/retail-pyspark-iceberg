"""Data quality checks run against bronze-layer DataFrames before they're
trusted downstream. Each check either returns a pass/fail signal or
raises a DataQualityError — the pipeline orchestrator decides whether to
halt or quarantine based on which.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.core.exceptions import DataQualityError


class DataQualityChecker:
    """Groups related validation checks. Most checks here are
    @staticmethod — pure functions that take a DataFrame and return a
    result, with no dependency on instance state. check_fk_integrity()
    and profile() are regular instance methods instead, since a real
    version of this class would accumulate quarantine/profiling results
    across multiple checks called in sequence during one pipeline run.
    """

    def __init__(self):
        self._quarantined_rows: dict[str, DataFrame] = {}

    @staticmethod
    def check_nulls(df: DataFrame, required_columns: list[str]) -> DataFrame:
        """Returns only the rows that are null in any required column —
        the pipeline can route these to a quarantine table instead of
        silently dropping them. isNull()/isNotNull() are the standard
        way to test for Spark's null marker (distinct from Python None
        inside a DataFrame column)."""
        condition = None
        for col in required_columns:
            col_condition = F.col(col).isNull()
            condition = col_condition if condition is None else (condition | col_condition)
        return df.filter(condition)

    @staticmethod
    def check_uniqueness(df: DataFrame, key_columns: list[str]) -> DataFrame:
        """Returns duplicate rows based on key_columns, so callers can
        inspect what violated uniqueness before deciding how to handle
        it (keep first, keep latest, quarantine all)."""
        dup_keys = (
            df.groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .select(*key_columns)
        )
        return df.join(dup_keys, on=key_columns, how="inner")

    @staticmethod
    def check_pattern(df: DataFrame, column: str, pattern: str) -> DataFrame:
        """Returns rows where `column` does NOT match the given regex —
        e.g. malformed emails. `rlike` is Spark's column-level regex
        match, equivalent to SQL's RLIKE."""
        return df.filter(~F.col(column).rlike(pattern))

    @staticmethod
    def check_non_negative(df: DataFrame, numeric_columns: list[str]) -> DataFrame:
        """Returns rows where any numeric column is negative — used for
        quantity/amount/unit_price fields that should never be below 0."""
        condition = None
        for col in numeric_columns:
            col_condition = F.col(col) < 0
            condition = col_condition if condition is None else (condition | col_condition)
        return df.filter(condition)

    def check_fk_integrity(
        self, df: DataFrame, fk_column: str, reference_df: DataFrame, reference_key: str
    ) -> DataFrame:
        """Left-anti join: returns rows from `df` whose fk_column has NO
        match in reference_df's reference_key — i.e. orphaned foreign
        keys. 'left_anti' is a join type that keeps left-side rows with
        NO match on the right, the exact opposite of a normal inner join.
        This is an instance method (uses self) because a real pipeline
        run stores quarantined orphans here for a later summary report."""
        orphans = df.join(
            reference_df, df[fk_column] == reference_df[reference_key], how="left_anti"
        )
        self._quarantined_rows[fk_column] = orphans
        return orphans

    def profile(self, df: DataFrame) -> None:
        """Prints summary statistics (count, mean, stddev, min, max per
        column) — a quick sanity check on a new dataset before writing
        real validation rules against it. .describe() is an action: it
        triggers a Spark job to compute these stats, unlike the
        transformations above which stay lazy until something calls
        .show()/.count()/.write() on their result."""
        df.describe().show()

    def raise_if_invalid(self, invalid_rows: DataFrame, message: str) -> None:
        """Call this after any check above — if invalid_rows is
        non-empty, raises DataQualityError with context. count() is an
        action, so this is the point where Spark actually executes the
        check we just defined lazily."""
        bad_count = invalid_rows.count()
        if bad_count > 0:
            raise DataQualityError(f"{message}: {bad_count} invalid row(s) found")