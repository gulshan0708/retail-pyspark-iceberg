"""The Pipeline class: composition over inheritance. Holds an ordered
list of transformer objects and runs them in sequence — this is the
pattern that makes pipelines/pipeline2_clean.py etc. a thin script that
just assembles and runs a Pipeline, instead of each pipeline file
re-implementing orchestration logic.
"""

from __future__ import annotations
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame

if TYPE_CHECKING:
    from src.transform.base import BaseTransformer  # avoids circular import


class Pipeline:
    """Composes a sequence of BaseTransformer instances. Encapsulates the
    step list as a protected attribute — external code adds steps via
    `.add_step()` rather than mutating `_steps` directly."""

    def __init__(self) -> None:
        self._steps: list["BaseTransformer"] = []

    def add_step(self, transformer: "BaseTransformer") -> "Pipeline":
        """Returns self, so calls can be chained:
        pipeline.add_step(a).add_step(b).run(df)"""
        self._steps.append(transformer)
        return self

    def run(self, df: DataFrame) -> DataFrame:
        """Applies each transformer's `.transform()` in order. Because
        every transformer implements the same BaseTransformer interface,
        this loop doesn't need to know which concrete subclass it's
        calling — that's polymorphism in action."""
        for step in self._steps:
            df = step.transform(df)
        return df