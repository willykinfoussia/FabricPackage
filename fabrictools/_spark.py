"""SparkSession accessor for fabrictools."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """
    Return the active SparkSession, creating one if necessary.

    Inside a Microsoft Fabric notebook the runtime already has an active
    session, so ``getOrCreate()`` simply returns it.  Outside Fabric (e.g.
    local development) a new local session is started automatically.

    Returns
    -------
    SparkSession
    """
    return SparkSession.builder.getOrCreate()
