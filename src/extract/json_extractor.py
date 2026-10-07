"""JSON source extractor — used for `customers` and `payments`.
"""

from pyspark.sql import DataFrame
from src.extract.base import BaseExtractor


class JSONExtractor(BaseExtractor):
    def extract(self) -> DataFrame:
        return (
            self.spark.read
            .schema(self.schema)
            .option("multiline", "true")   # each record may span multiple lines
            .option("mode", "PERMISSIVE")
            .json(self.source_path)
        )