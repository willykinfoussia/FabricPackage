"""SparkSession accessor for fabrictools."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Return the active SparkSession, creating one if necessary."""
    return SparkSession.builder.getOrCreate()

