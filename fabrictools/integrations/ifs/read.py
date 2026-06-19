"""High-level IFS read helpers for Spark and Lakehouse."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore[reportMissingImports]

from fabrictools.core import get_spark
from fabrictools.integrations.ifs._logging import log_ifs
from fabrictools.integrations.ifs.client import IFSClient
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.integrations.ifs.dataframe import rows_to_dataframe
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
    log_ifs("read_ifs_entity — démarrage")

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
        log_ifs(f"read_ifs_entity — 0 ligne, DataFrame Spark vide pour {projection}/{entity_set}")

    df = rows_to_dataframe(rows, spark=_spark)
    log_ifs(
        f"read_ifs_entity — DataFrame créé: {df.count()} ligne(s), "
        f"{len(df.columns)} colonne(s): {df.columns}"
    )
    return df


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
    log_ifs(
        f"read_ifs_to_lakehouse — démarrage "
        f"(lakehouse={lakehouse_name!r}, path={relative_path!r}, mode={mode!r})"
    )
    df = read_ifs_entity(
        config,
        projection,
        entity_set,
        odata_filter=odata_filter,
        select=select,
        fetch_all=fetch_all,
        spark=spark,
    )
    log_ifs(f"Écriture Lakehouse — {lakehouse_name}/{relative_path} (mode={mode})")
    write_lakehouse(df, lakehouse_name, relative_path, mode=mode, spark=spark)
    log_ifs(f"read_ifs_to_lakehouse — terminé ({df.count()} ligne(s) écrites)")
    return df
