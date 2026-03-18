"""
Warehouse read / write helpers for Microsoft Fabric.

Fabric Warehouses expose a SQL Server-compatible TDS endpoint.  These helpers
connect via the Spark JDBC connector using a URL resolved automatically from
the warehouse display name through ``notebookutils``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from fabrictools._logger import log
from fabrictools._paths import get_warehouse_jdbc_url
from fabrictools._spark import get_spark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# The JDBC driver class bundled with Fabric / Azure Databricks runtimes.
_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


# ── Read ─────────────────────────────────────────────────────────────────────


def read_warehouse(
    warehouse_name: str,
    query: str,
    spark: Optional["SparkSession"] = None,
) -> "DataFrame":
    """
    Execute a SQL query against a Fabric Warehouse and return the result.

    The connection URL is resolved automatically from the warehouse display
    name using ``notebookutils``.  Authentication reuses the token of the
    currently signed-in Fabric user (Managed Identity / Entra token).

    Parameters
    ----------
    warehouse_name:
        Display name of the Warehouse (e.g. ``"MyWarehouse"``).
    query:
        SQL query to execute (e.g. ``"SELECT * FROM dbo.sales"``).
        Wrap complex queries in parentheses when needed:
        ``"(SELECT id, name FROM dbo.sales WHERE year = 2024) t"``.
    spark:
        Optional SparkSession.  When omitted the active session is used.

    Returns
    -------
    DataFrame

    Examples
    --------
    >>> df = read_warehouse("MyWarehouse", "SELECT * FROM dbo.sales")
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
    df: "DataFrame",
    warehouse_name: str,
    table: str,
    mode: str = "overwrite",
    batch_size: int = 10_000,
    spark: Optional["SparkSession"] = None,
) -> None:
    """
    Write a DataFrame to a table in a Fabric Warehouse via JDBC.

    Parameters
    ----------
    df:
        DataFrame to persist.
    warehouse_name:
        Display name of the target Warehouse.
    table:
        Fully-qualified table name, e.g. ``"dbo.sales_clean"``.
    mode:
        Spark write mode — ``"overwrite"`` (default), ``"append"``,
        ``"ignore"``, or ``"error"``.
    batch_size:
        Number of rows per JDBC batch insert.  Defaults to ``10 000``.
    spark:
        Optional SparkSession.  When omitted the active session is used.

    Examples
    --------
    >>> write_warehouse(df, "MyWarehouse", "dbo.sales_clean", mode="append")
    """
    _ = spark or get_spark()
    jdbc_url = get_warehouse_jdbc_url(warehouse_name)
    log(
        f"Writing to Warehouse '{warehouse_name}' → {table} "
        f"[mode={mode}, batchSize={batch_size:,}]"
    )

    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", _JDBC_DRIVER)
        .option("dbtable", table)
        .option("batchsize", batch_size)
        .mode(mode)
        .save()
    )
    log(f"  Write complete → {table}")
