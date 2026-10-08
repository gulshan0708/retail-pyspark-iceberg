"""Abstract base class for all transformers."""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseTransformer(ABC):
    """Contract: take a DataFrame, return a DataFrame. Because every
    subclass honours this, Pipeline.run() can call .transform() on any
    of them without knowing which one it is (polymorphism)."""

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"