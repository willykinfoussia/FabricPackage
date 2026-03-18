"""
fabrictools — User-friendly PySpark helpers for Microsoft Fabric.

Public API
----------
Lakehouse
~~~~~~~~~
read_lakehouse(lakehouse_name, relative_path, spark=None)
    Read a dataset (auto-detects Delta / Parquet / CSV).
write_lakehouse(df, lakehouse_name, relative_path, mode, partition_by, format, spark=None)
    Write a DataFrame to a Lakehouse (defaults to Delta format).
merge_lakehouse(source_df, lakehouse_name, relative_path, merge_condition, ...)
    Upsert (merge) a DataFrame into an existing Delta table.

Warehouse
~~~~~~~~~
read_warehouse(warehouse_name, query, spark=None)
    Run a SQL query and return the result as a DataFrame.
write_warehouse(df, warehouse_name, table, mode, batch_size, spark=None)
    Write a DataFrame to a Warehouse table via JDBC.
"""

from fabrictools.lakehouse import merge_lakehouse, read_lakehouse, write_lakehouse
from fabrictools.warehouse import read_warehouse, write_warehouse

__all__ = [
    "read_lakehouse",
    "write_lakehouse",
    "merge_lakehouse",
    "read_warehouse",
    "write_warehouse",
]

__version__ = "0.1.0"
