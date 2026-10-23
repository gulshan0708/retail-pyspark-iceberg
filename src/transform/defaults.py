"""Transformer that replaces nulls with default values."""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.transform.base import BaseTransformer


class DefaultsTransformer(BaseTransformer):
    """`defaults` maps column name -> replacement value, e.g.
    {"country": "UNKNOWN"}. The value's type must match the column's type
    (text for a string column, a number for a numeric column)."""

    def __init__(self, defaults: dict[str, str | int | float]):
        self._defaults = defaults

    def transform(self, df: DataFrame) -> DataFrame:
        # df.na.fill(dict) replaces nulls column by column; columns not in
        # the dict are left alone.
        return df.na.fill(self._defaults)

    def __repr__(self) -> str:
        return f"DefaultsTransformer(defaults={self._defaults})"