"""Typed configuration objects passed into extractors, transformers, and
loaders — replaces scattering raw dict lookups or magic strings through
the codebase.
"""

from dataclasses import dataclass, field
from datetime import date


@dataclass
class PipelineConfig:
    """Configuration for a single pipeline run.

    Attributes:
        batch_date: the logical date this run is processing (drives
            Iceberg's days() partitioning and Airflow backfill later).
        warehouse_path: S3-style path to the Iceberg warehouse root.
        catalog_name: name of the Iceberg REST catalog (matches the
            `demo` catalog baked into the tabulario/spark-iceberg image).
        source_paths: raw source locations, keyed by entity name.
    """
    batch_date: date
    warehouse_path: str = "s3://warehouse/"
    catalog_name: str = "demo"
    source_paths: dict[str, str] = field(default_factory=dict)
    jdbc_url: str = "jdbc:postgresql://postgres:5432/retail"