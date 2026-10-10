"""Postgres loader: exports summary tables to the serving layer."""

from pyspark.sql import DataFrame
from src.load.base import BaseLoader


class PostgresLoader(BaseLoader):
    def __init__(
        self,
        jdbc_url: str,
        user: str = "postgres",
        password: str = "postgres",
    ):
        self.jdbc_url = jdbc_url  # e.g. "jdbc:postgresql://postgres:5432/retail"
        self._properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        }

    def load(self, df: DataFrame, target: str, mode: str = "append") -> None:
        df.write.mode(mode).jdbc(
            url=self.jdbc_url, table=target, properties=self._properties
        )

    def export_summary(self, df: DataFrame, table: str) -> None:
        """Refreshes a summary table in Postgres. 'overwrite' + 'truncate'
        keeps the existing table definition and replaces its rows."""
        (
            df.write.mode("overwrite")
            .option("truncate", "true")
            .jdbc(url=self.jdbc_url, table=table, properties=self._properties)
        )