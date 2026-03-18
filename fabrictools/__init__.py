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
clean_data(df, drop_duplicates=True, drop_all_null_rows=True)
    Apply standard generic cleaning to a DataFrame.
scan_data_errors(df, include_samples=True)
    Scan and report common data-quality issues.
clean_and_write_data(source_lakehouse_name, source_relative_path, target_lakehouse_name, target_relative_path, ...)
    Read, clean, and write data in one pipeline helper.

Warehouse
~~~~~~~~~
read_warehouse(warehouse_name, query, spark=None)
    Run a SQL query and return the result as a DataFrame.
write_warehouse(df, warehouse_name, table, mode, batch_size, spark=None)
    Write a DataFrame to a Warehouse table via JDBC.
"""

from fabrictools.data_quality import (
    clean_and_write_data,
    clean_data,
    scan_data_errors,
)
from fabrictools.lakehouse import (
    merge_lakehouse,
    read_lakehouse,
    write_lakehouse,
)
from fabrictools.warehouse import read_warehouse, write_warehouse

__all__ = [
    "read_lakehouse",
    "write_lakehouse",
    "merge_lakehouse",
    "clean_data",
    "scan_data_errors",
    "clean_and_write_data",
    "read_warehouse",
    "write_warehouse",
]

__version__ = "0.1.1"
