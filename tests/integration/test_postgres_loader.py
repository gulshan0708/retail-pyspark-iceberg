"""Integration test for PostgresLoader against the real Postgres container.

It writes to the table `it_scratch_summary`, which stays in the database afterwards.
Remove it with:
  docker exec -it retail-postgres psql -U postgres -d retail -c "DROP TABLE IF EXISTS it_scratch_summary;"
"""

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.extract.jdbc_extractor import JDBCExtractor
from src.load.PostgresLoader import PostgresLoader

pytestmark = pytest.mark.integration

JDBC_URL = "jdbc:postgresql://postgres:5432/retail"


def test_export_summary_replaces_the_previous_contents(spark_it):
    first = spark_it.createDataFrame([(1, "a"), (2, "b")], "id int, name string")
    second = spark_it.createDataFrame([(7, "z")], "id int, name string")

    postgres = PostgresLoader(JDBC_URL)
    postgres.export_summary(first, "it_scratch_summary")
    postgres.export_summary(second, "it_scratch_summary")  # truncates, then writes

    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    result = JDBCExtractor(spark_it, JDBC_URL, "it_scratch_summary", schema).extract()
    assert [(row.id, row.name) for row in result.collect()] == [(7, "z")]