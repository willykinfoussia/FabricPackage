"""SparkSession accessor for fabrictools."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Return the active ``SparkSession``, creating one if none exists.

    :returns: Current or newly built session.
    :rtype: ~pyspark.sql.SparkSession
    """
    spark = SparkSession.builder.getOrCreate()
    # Avoid Spark 3.x failures on legacy Parquet/Delta ancient datetime values.
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    return spark

