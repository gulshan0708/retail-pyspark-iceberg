"""Abstract base class all extractors inherit from — defines the
contract (the `extract()` method) every concrete extractor must fulfill.
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class BaseExtractor(ABC):
    """`ABC` = Abstract Base Class. A class can't be instantiated directly
    if it inherits from ABC and has at least one @abstractmethod — Python
    enforces that subclasses implement every abstract method before they
    can be created. This guarantees every extractor (CSV, JSON, JDBC)
    exposes the exact same `.extract()` interface, so the pipeline code
    that calls extractors doesn't need to know which concrete type it's
    talking to (polymorphism).
    """

    def __init__(self, spark: SparkSession, source_path: str, schema: StructType):
        self.spark = spark
        self.source_path = source_path
        self.schema = schema  # explicit schema, not inferSchema — see extract() in subclasses

    @abstractmethod
    def extract(self) -> DataFrame:
        """Every subclass must implement this. Returns a DataFrame read
        from this extractor's specific source type."""
        raise NotImplementedError

    def __repr__(self) -> str:
        """Dunder ('double underscore') method — Python calls this
        automatically when you `print()` an object or view it in a
        debugger/REPL. Without it, printing an extractor would show
        something unhelpful like `<src.extract.base.CSVExtractor object
        at 0x7f...>`. Defining __repr__ makes debug logging actually
        useful: you instead see which file path each extractor is
        pointed at."""
        return f"{self.__class__.__name__}(source_path='{self.source_path}')"