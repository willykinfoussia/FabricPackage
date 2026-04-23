"""SparkSession accessor for fabrictools."""

from __future__ import annotations

from pyspark.sql import SparkSession


def configure_parquet_datetime_rebase(spark: SparkSession) -> SparkSession:
    """Apply safe Parquet datetime rebase settings on a Spark session."""
    # Spark 3.x may fail on ancient datetimes unless rebase behavior is explicit.
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    return spark


def get_spark() -> SparkSession:
    """Return the active ``SparkSession``, creating one if none exists.

    :returns: Current or newly built session.
    :rtype: ~pyspark.sql.SparkSession
    """
    spark = SparkSession.builder.getOrCreate()
    return configure_parquet_datetime_rebase(spark)

