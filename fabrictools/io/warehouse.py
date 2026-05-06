"""Warehouse I/O facade module."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from fabrictools.core import log
from fabrictools.core import get_warehouse_jdbc_url
from fabrictools.core import get_spark

# The JDBC driver class bundled with Fabric / Azure Databricks runtimes.
_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


# ── Read ─────────────────────────────────────────────────────────────────────


def read_warehouse(
    warehouse_name: str,
    query: str,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Run a SQL query on a Fabric Warehouse and return the result as a ``DataFrame``.

    The JDBC URL is resolved from the warehouse display name via ``notebookutils``.
    Authentication uses the signed-in Fabric user token.

    :param warehouse_name: Warehouse display name (e.g. ``"MyWarehouse"``).
    :param query: SQL text (e.g. ``"SELECT * FROM dbo.sales"``). Wrap subqueries in
        parentheses when needed, e.g. ``"(SELECT id, name FROM dbo.sales WHERE year = 2024) t"``.
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type warehouse_name: str
    :type query: str
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Query result.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> df = read_warehouse("MyWarehouse", "SELECT * FROM dbo.sales")  # doctest: +SKIP
    """
    _spark = spark or get_spark()
    jdbc_url = get_warehouse_jdbc_url(warehouse_name)
    log(f"Reading Warehouse '{warehouse_name}' with query: {query[:120]}")

    df = (
        _spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", _JDBC_DRIVER)
        .option("query", query)
        .load()
    )
    log(f"  {df.count():,} rows · {len(df.columns)} columns")
    return df


# ── Write ────────────────────────────────────────────────────────────────────


def write_warehouse(
    df: DataFrame,
    warehouse_name: str,
    table: str,
    mode: str = "overwrite",
    batch_size: int = 10_000,
    spark: Optional[SparkSession] = None,
) -> None:
    """Write a ``DataFrame`` to a Fabric Warehouse table via JDBC.

    :param df: DataFrame to persist.
    :param warehouse_name: Target Warehouse display name.
    :param table: Fully-qualified table name (e.g. ``"dbo.sales_clean"``).
    :param mode: Spark write mode: ``"overwrite"`` (default), ``"append"``,
        ``"ignore"``, or ``"error"``.
    :param batch_size: Rows per JDBC batch (default ``10000``).
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type df: ~pyspark.sql.DataFrame
    :type warehouse_name: str
    :type table: str
    :type mode: str
    :type batch_size: int
    :type spark: ~pyspark.sql.SparkSession | None

    .. rubric:: Example

    >>> write_warehouse(df, "MyWarehouse", "dbo.sales_clean", mode="append")  # doctest: +SKIP
    """
    _ = spark or get_spark()
    jdbc_url = get_warehouse_jdbc_url(warehouse_name)
    mode_norm = str(mode).strip().lower()
    log(
        f"Writing to Warehouse '{warehouse_name}' → {table} "
        f"[mode={mode_norm}, batchSize={batch_size:,}]"
    )

    if mode_norm in ("upsert", "merge"):
        raise ValueError(
            "Fabric Warehouse JDBC writes do not support Spark upsert/merge modes. "
            "Use Lakehouse :py:func:`fabrictools.write_lakehouse` with "
            "mode='upsert', or use overwrite/append for Warehouse."
        )

    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", _JDBC_DRIVER)
        .option("dbtable", table)
        .option("batchsize", batch_size)
        .mode(mode_norm)
        .save()
    )
    log(f"  Write complete → {table}")

__all__ = ["read_warehouse", "write_warehouse"]

