"""JDBC source extractor — used for `products`, read from Postgres.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from src.extract.base import BaseExtractor


class JDBCExtractor(BaseExtractor):
    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        table_name: str,
        schema: StructType,
        user: str = "postgres",
        password: str = "postgres",
    ):
        # `super()` refers to the parent class (BaseExtractor). Calling
        # its __init__ here reuses the spark/schema setup instead of
        # duplicating it — source_path isn't meaningful for JDBC, so we
        # pass the table_name in its place just to satisfy the parent
        # signature, then set JDBC-specific attributes below.
        super().__init__(spark=spark, source_path=table_name, schema=schema)
        self.jdbc_url = jdbc_url
        self.table_name = table_name
        self.user = user
        self.password = password

    def extract(self) -> DataFrame:
        return (
            self.spark.read
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", self.table_name)
            .option("user", self.user)
            .option("password", self.password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )