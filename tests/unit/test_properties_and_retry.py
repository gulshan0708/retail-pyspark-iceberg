"""Tests for @property usage and for @retry applied to a real class."""

import time
from unittest.mock import MagicMock

import pytest

from src.core.pipeline import Pipeline
from src.core.spark_session import SparkSessionManager
from src.extract.jdbc_extractor import JDBCExtractor
from src.quality.checker import DataQualityChecker
from src.transform.defaults import DefaultsTransformer


def test_pipeline_steps_is_a_read_only_tuple():
    step = DefaultsTransformer({"country": "UNKNOWN"})
    pipeline = Pipeline().add_step(step)
    assert pipeline.steps == (step,)
    with pytest.raises(AttributeError):
        pipeline.steps = ()  # a property without a setter cannot be assigned


def test_pipeline_len_counts_the_steps():
    pipeline = Pipeline()
    assert len(pipeline) == 0
    pipeline.add_step(DefaultsTransformer({}))
    assert len(pipeline) == 1


def test_quarantined_rows_returns_a_copy(spark):
    checker = DataQualityChecker()
    orders = spark.createDataFrame([(1, 99)], "order_id int, customer_id int")
    customers = spark.createDataFrame([(10,)], "customer_id int")
    checker.check_fk_integrity(orders, "customer_id", customers, "customer_id")

    snapshot = checker.quarantined_rows
    assert set(snapshot) == {"customer_id"}
    snapshot.clear()  # changing the copy...
    assert set(checker.quarantined_rows) == {"customer_id"}  # ...leaves the checker untouched


def test_app_name_rejects_empty_values():
    manager = SparkSessionManager(app_name="ok")
    with pytest.raises(ValueError):
        manager.app_name = "   "
    with pytest.raises(ValueError):
        SparkSessionManager(app_name="")


def test_app_name_can_be_changed_to_a_valid_name():
    manager = SparkSessionManager(app_name="first")
    manager.app_name = "second"
    assert manager.app_name == "second"


def make_fake_spark(*outcomes):
    """A stand-in for a SparkSession. Its reader chain (.format().option()...load())
    returns itself at every step, and .load() returns or raises each outcome in turn."""
    reader = MagicMock()
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.side_effect = list(outcomes)
    spark = MagicMock()
    spark.read = reader
    return spark, reader


def test_jdbc_extract_retries_transient_connection_errors(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)  # don't really wait 5 seconds
    spark, reader = make_fake_spark(
        ConnectionError("blip"), ConnectionError("blip"), "the-dataframe"
    )
    extractor = JDBCExtractor(spark, "jdbc:postgresql://x/db", "products", schema=None)
    assert extractor.extract() == "the-dataframe"
    assert reader.load.call_count == 3  # two failures, then success


def test_jdbc_extract_gives_up_after_three_attempts(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    spark, reader = make_fake_spark(ConnectionError("down"), ConnectionError("down"), ConnectionError("down"))
    extractor = JDBCExtractor(spark, "jdbc:postgresql://x/db", "products", schema=None)
    with pytest.raises(ConnectionError):
        extractor.extract()
    assert reader.load.call_count == 3