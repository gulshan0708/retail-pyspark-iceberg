"""CSV source extractor — used for `orders` and `order_items`, both
landed as CSV files in the raw storage bucket.
"""

from pyspark.sql import DataFrame
from src.extract.base import BaseExtractor


class CSVExtractor(BaseExtractor):
    """Inherits __init__ from BaseExtractor unchanged — only extract()
    needs to be different per source type, which is exactly what
    inheritance is for: share the common stuff, override the specific
    stuff."""

    def extract(self) -> DataFrame:
        return (
            self.spark.read
            .schema(self.schema)      # explicit StructType, not inferSchema
            .option("header", "true")
            .option("mode", "PERMISSIVE")   # keep malformed rows (flagged), don't silently drop
            .csv(self.source_path)
        )