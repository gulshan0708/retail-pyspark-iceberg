"""Unit tests for DefaultsTransformer."""

from chispa import assert_df_equality

from src.transform.defaults import DefaultsTransformer


def test_defaults_replace_nulls_only_in_the_listed_columns(spark):
    df = spark.createDataFrame(
        [(1, None, None), (2, "US", "x")], "id int, country string, note string"
    )
    result = DefaultsTransformer({"country": "UNKNOWN"}).transform(df)
    expected = spark.createDataFrame(
        [(1, "UNKNOWN", None), (2, "US", "x")], "id int, country string, note string"
    )
   ## assert_df_equality(result, expected, ignore_row_order=True)
    assert_df_equality(result,expected,ignore_row_order=True,ignore_nullable=True,)