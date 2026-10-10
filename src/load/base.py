"""Abstract base class for all loaders."""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseLoader(ABC):
    """Contract: every loader writes a DataFrame to a target.

    `mode` follows Spark's write-mode vocabulary: "append" adds rows,
    "overwrite" replaces the target's contents.
    """

    @abstractmethod
    def load(self, df: DataFrame, target: str, mode: str = "append") -> None:
        raise NotImplementedError