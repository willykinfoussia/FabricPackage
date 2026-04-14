"""SparkSession accessor for fabrictools."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Return the active ``SparkSession``, creating one if none exists.

    :returns: Current or newly built session.
    :rtype: ~pyspark.sql.SparkSession
    """
    return SparkSession.builder.getOrCreate()

