"""Custom exception hierarchy for the retail pipeline.

Using specific exception types (rather than bare `Exception`) lets calling
code distinguish failure modes — e.g. retry a transient Spark job failure,
but never retry a schema mismatch, since retrying won't fix bad data.
"""


class PipelineError(Exception):
    """Base class for all pipeline-specific errors. Catch this to handle
    any pipeline failure generically; catch a subclass to handle one kind
    specifically."""
    pass


class SchemaValidationError(PipelineError):
    """Raised when a DataFrame's actual schema doesn't match the expected
    StructType — e.g. a raw source file arrived with a renamed or missing
    column."""
    pass


class DataQualityError(PipelineError):
    """Raised when DataQualityChecker finds data that fails a business
    rule — nulls in a required field, duplicate primary keys, a foreign
    key that doesn't resolve, etc."""
    pass