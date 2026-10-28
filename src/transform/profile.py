"""Derives descriptive columns from the customers table using string and date functions."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transform.base import BaseTransformer


class ProfileTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        """DataFrame API version."""
        return (
            df
            # concat_ws joins with a separator and SKIPS nulls
            .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
            # substring(column, start, length): start counts from 1. concat returns NULL if any input is NULL
            .withColumn(
                "initials",
                F.upper(F.concat(F.substring("first_name", 1, 1), F.substring("last_name", 1, 1))),
            )
            # regexp_extract(column, pattern, group): the text matched by group 1, the part in ( )
            .withColumn("email_domain", F.regexp_extract("email", r"@(.+)$", 1))
            # split(column, regex) gives an array; getItem(0) takes its first element
            .withColumn("email_user", F.split("email", "@").getItem(0))
            .withColumn("signup_ts", F.to_timestamp("signup_date"))
            .withColumn("signup_month", F.date_format("signup_date", "yyyy-MM"))
            .withColumn("signup_weekday", F.date_format("signup_date", "EEEE"))
        )

    def transform_with_sql(self, df: DataFrame) -> DataFrame:
        """The same columns as SQL strings. selectExpr takes one SQL expression per
        column; "*" means "all existing columns"."""
        return df.selectExpr(
            "*",
            "concat_ws(' ', first_name, last_name) AS full_name",
            "upper(concat(substring(first_name, 1, 1), substring(last_name, 1, 1))) AS initials",
            "regexp_extract(email, '@(.+)$', 1) AS email_domain",
            "split(email, '@')[0] AS email_user",
            "to_timestamp(signup_date) AS signup_ts",
            "date_format(signup_date, 'yyyy-MM') AS signup_month",
            "date_format(signup_date, 'EEEE') AS signup_weekday",
        )