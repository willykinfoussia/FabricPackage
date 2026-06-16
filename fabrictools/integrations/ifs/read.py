"""High-level IFS read helpers for Spark and Lakehouse."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore[reportMissingImports]

from fabrictools.core import get_spark, log
from fabrictools.integrations.ifs.client import IFSClient
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.io.lakehouse import write_lakehouse


def read_ifs_entity(
    config: IFSConfig,
    projection: str,
    entity_set: str,
    *,
    odata_filter: Optional[str] = None,
    select: Optional[list[str]] = None,
    fetch_all: bool = True,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Read an IFS OData entity set and return a Spark DataFrame.

    :param config: IFS connection settings.
    :param projection: OData projection service name (e.g. ``ActivityService``).
    :param entity_set: Entity set name (e.g. ``Activities``).
    :param odata_filter: Optional OData ``$filter`` expression.
    :param select: Optional list of fields for ``$select``.
    :param fetch_all: When ``True``, paginate through all result pages.
    :param spark: Optional Spark session; active session is used when omitted.
    """
    client = IFSClient(config)
    rows = client.get_entity(
        projection,
        entity_set,
        odata_filter=odata_filter,
        select=select,
        fetch_all=fetch_all,
    )
    _spark = spark or get_spark()
    if not rows:
        log(f"IFS entity {projection}/{entity_set} returned no rows — empty DataFrame")
        return _spark.createDataFrame([], schema=None)
    return _spark.createDataFrame(rows)


def read_ifs_to_lakehouse(
    config: IFSConfig,
    projection: str,
    entity_set: str,
    lakehouse_name: str,
    relative_path: str,
    *,
    odata_filter: Optional[str] = None,
    select: Optional[list[str]] = None,
    mode: str = "overwrite",
    fetch_all: bool = True,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Read an IFS entity set and write the result to a Fabric Lakehouse.

    :param config: IFS connection settings.
    :param projection: OData projection service name.
    :param entity_set: Entity set name.
    :param lakehouse_name: Target Lakehouse display name.
    :param relative_path: Lakehouse relative path (e.g. ``Tables/dbo/ifs_activities``).
    :param odata_filter: Optional OData ``$filter`` expression.
    :param select: Optional list of fields for ``$select``.
    :param mode: Lakehouse write mode (default ``overwrite``).
    :param fetch_all: When ``True``, paginate through all result pages.
    :param spark: Optional Spark session.
    """
    df = read_ifs_entity(
        config,
        projection,
        entity_set,
        odata_filter=odata_filter,
        select=select,
        fetch_all=fetch_all,
        spark=spark,
    )
    write_lakehouse(df, lakehouse_name, relative_path, mode=mode, spark=spark)
    return df
