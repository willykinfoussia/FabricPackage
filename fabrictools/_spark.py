"""SparkSession accessor for fabrictools."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark() -> "SparkSession":
    """
    Return the active SparkSession, creating one if necessary.

    Inside a Microsoft Fabric notebook the runtime already has an active
    session, so ``getOrCreate()`` simply returns it.  Outside Fabric (e.g.
    local development) a new local session is started automatically.

    Returns
    -------
    SparkSession
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415

    return SparkSession.builder.getOrCreate()
