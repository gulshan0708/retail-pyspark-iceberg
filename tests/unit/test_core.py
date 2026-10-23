"""Unit tests for the core package: Pipeline, decorators, exceptions."""

import logging

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from src.core.decorators import log_execution_time, retry
from src.core.exceptions import DataQualityError, PipelineError, SchemaValidationError
from src.core.pipeline import Pipeline
from src.transform.base import BaseTransformer


# ---- two tiny transformers used only by these tests ----

class AddConstant(BaseTransformer):
    def __init__(self, column: str, value: int):
        self._column = column
        self._value = value

    def transform(self, df):
        return df.withColumn(self._column, F.lit(self._value))


class AddOneTo(BaseTransformer):
    def __init__(self, source: str, target: str):
        self._source = source
        self._target = target

    def transform(self, df):
        return df.withColumn(self._target, F.col(self._source) + 1)


# ---- Pipeline (composition + polymorphism) ----

def test_pipeline_runs_steps_in_order(spark):
    df = spark.createDataFrame([(1,)], "id int")
    # AddOneTo needs column "a", which only exists if AddConstant ran first.
    pipeline = Pipeline().add_step(AddConstant("a", 1)).add_step(AddOneTo("a", "b"))
    result = pipeline.run(df).first()
    assert (result.a, result.b) == (1, 2)


def test_empty_pipeline_returns_the_input_unchanged(spark):
    df = spark.createDataFrame([(1,)], "id int")
    assert_df_equality(Pipeline().run(df), df)


def test_base_transformer_cannot_be_instantiated():
    with pytest.raises(TypeError):
        BaseTransformer()


def test_subclass_without_transform_cannot_be_instantiated():
    class Incomplete(BaseTransformer):  # forgot to implement transform()
        pass

    with pytest.raises(TypeError):
        Incomplete()


# ---- decorators ----

def test_log_execution_time_returns_the_result_and_keeps_the_function_name():
    @log_execution_time
    def add(a, b):
        return a + b

    assert add(2, 3) == 5
    assert add.__name__ == "add"  # functools.wraps preserved it


def test_log_execution_time_logs_the_duration(caplog):
    @log_execution_time
    def work():
        return 1

    with caplog.at_level(logging.INFO):
        work()
    assert any("work completed in" in message for message in caplog.messages)


def test_retry_succeeds_after_transient_failures():
    calls = {"count": 0}

    @retry(n=3, delay_seconds=0)
    def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise ConnectionError("blip")
        return "ok"

    assert flaky() == "ok"
    assert calls["count"] == 3


def test_retry_raises_the_last_error_after_n_attempts():
    calls = {"count": 0}

    @retry(n=3, delay_seconds=0)
    def always_fails():
        calls["count"] += 1
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        always_fails()
    assert calls["count"] == 3


# ---- exceptions ----

def test_specific_errors_can_be_caught_as_pipeline_error():
    with pytest.raises(PipelineError):
        raise DataQualityError("bad data")
    with pytest.raises(PipelineError):
        raise SchemaValidationError("bad schema")