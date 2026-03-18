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
    clean_and_write_data as _clean_and_write_data,
    clean_data as _clean_data,
    scan_data_errors as _scan_data_errors,
)
from fabrictools.lakehouse import (
    merge_lakehouse as _merge_lakehouse,
    read_lakehouse as _read_lakehouse,
    write_lakehouse as _write_lakehouse,
)
from fabrictools._version import __version__
from fabrictools.warehouse import (
    read_warehouse as _read_warehouse,
    write_warehouse as _write_warehouse,
)


def read_lakehouse(lakehouse_name, relative_path, spark=None):
    """Read a dataset from a Lakehouse (auto-detect Delta/Parquet/CSV)."""
    return _read_lakehouse(lakehouse_name, relative_path, spark=spark)


def write_lakehouse(
    df,
    lakehouse_name,
    relative_path,
    mode="overwrite",
    partition_by=None,
    format="delta",
    spark=None,
):
    """Write a DataFrame to a Lakehouse path (default format: Delta)."""
    return _write_lakehouse(
        df,
        lakehouse_name,
        relative_path,
        mode=mode,
        partition_by=partition_by,
        format=format,
        spark=spark,
    )


def merge_lakehouse(
    source_df,
    lakehouse_name,
    relative_path,
    merge_condition,
    update_set=None,
    insert_set=None,
    spark=None,
):
    """Upsert a DataFrame into an existing Delta table in a Lakehouse."""
    return _merge_lakehouse(
        source_df,
        lakehouse_name,
        relative_path,
        merge_condition,
        update_set=update_set,
        insert_set=insert_set,
        spark=spark,
    )


def clean_data(df, drop_duplicates=True, drop_all_null_rows=True):
    """Apply standard generic data-cleaning transformations to a DataFrame."""
    return _clean_data(
        df,
        drop_duplicates=drop_duplicates,
        drop_all_null_rows=drop_all_null_rows,
    )


def scan_data_errors(df, include_samples=True):
    """Scan a DataFrame and return a data-quality report dictionary."""
    return _scan_data_errors(df, include_samples=include_samples)


def clean_and_write_data(
    source_lakehouse_name,
    source_relative_path,
    target_lakehouse_name,
    target_relative_path,
    mode="overwrite",
    partition_by=None,
    spark=None,
):
    """Read, clean, and write data from source to target Lakehouse path."""
    return _clean_and_write_data(
        source_lakehouse_name,
        source_relative_path,
        target_lakehouse_name,
        target_relative_path,
        mode=mode,
        partition_by=partition_by,
        spark=spark,
    )


def read_warehouse(warehouse_name, query, spark=None):
    """Run a SQL query on a Warehouse and return the result DataFrame."""
    return _read_warehouse(warehouse_name, query, spark=spark)


def write_warehouse(
    df,
    warehouse_name,
    table,
    mode="overwrite",
    batch_size=10_000,
    spark=None,
):
    """Write a DataFrame to a Warehouse table via JDBC."""
    return _write_warehouse(
        df,
        warehouse_name,
        table,
        mode=mode,
        batch_size=batch_size,
        spark=spark,
    )

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

