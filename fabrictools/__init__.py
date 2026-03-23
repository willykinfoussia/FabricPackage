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
add_silver_metadata(df, source_lakehouse_name, source_relative_path, source_layer="bronze", ...)
    Add Silver metadata columns and date partitions.
scan_data_errors(df, include_samples=True, display_results=True)
    Scan data-quality issues and return a summary DataFrame plus chart.
clean_and_write_data(source_lakehouse_name, source_relative_path, target_lakehouse_name, target_relative_path, ...)
    Read, clean, add Silver metadata, and write data in one pipeline helper.
clean_and_write_all_tables(source_lakehouse_name, target_lakehouse_name, ...)
    Bulk orchestration via discovery or tables_config (overwrite/append/merge).

Warehouse
~~~~~~~~~
read_warehouse(warehouse_name, query, spark=None)
    Run a SQL query and return the result as a DataFrame.
write_warehouse(df, warehouse_name, table, mode, batch_size, spark=None)
    Write a DataFrame to a Warehouse table via JDBC.

Dimensions
~~~~~~~~~~
build_dimension_date(start_date=None, end_date=None, lakehouse_name=None, lakehouse_relative_path=None, mode="overwrite", spark=None)
    Build a date dimension DataFrame and optionally write it to a Lakehouse path.
build_dimension_country(countries_limit=None, fail_on_source_error=True, lakehouse_name=None, lakehouse_relative_path=None, mode="overwrite", spark=None)
    Build a country dimension DataFrame and optionally write it to a Lakehouse path.
build_dimension_city(countries_limit=None, include_states_metadata=True, fail_on_source_error=True, lakehouse_name=None, lakehouse_relative_path=None, mode="overwrite", spark=None)
    Build a city dimension DataFrame and optionally write it to a Lakehouse path.
generate_dimensions(lakehouse_name, warehouse_name, ...)
    Build and write dimension tables to Lakehouse and Warehouse.
"""
from __future__ import annotations

from fabrictools._version import __version__
from fabrictools.dimensions import (
    build_dimension_city,
    build_dimension_country,
    build_dimension_date,
    generate_dimensions,
)
from fabrictools.io.lakehouse import merge_lakehouse, read_lakehouse, write_lakehouse
from fabrictools.io.warehouse import read_warehouse, write_warehouse
from fabrictools.prepare import (
    generate_prepared_aggregations,
    prepare_and_write_all_tables,
    prepare_and_write_data,
    publish_semantic_model,
    resolve_columns,
    snapshot_source_schema,
    transform_to_prepared,
    write_prepared_table,
)
from fabrictools.quality.clean import add_silver_metadata, clean_data
from fabrictools.quality.pipeline import clean_and_write_all_tables, clean_and_write_data
from fabrictools.quality.scan import scan_data_errors

_EXPORT_REGISTRY = {
    "read_lakehouse": read_lakehouse,
    "write_lakehouse": write_lakehouse,
    "merge_lakehouse": merge_lakehouse,
    "clean_data": clean_data,
    "add_silver_metadata": add_silver_metadata,
    "scan_data_errors": scan_data_errors,
    "clean_and_write_data": clean_and_write_data,
    "clean_and_write_all_tables": clean_and_write_all_tables,
    "read_warehouse": read_warehouse,
    "write_warehouse": write_warehouse,
    "build_dimension_date": build_dimension_date,
    "build_dimension_country": build_dimension_country,
    "build_dimension_city": build_dimension_city,
    "generate_dimensions": generate_dimensions,
    "snapshot_source_schema": snapshot_source_schema,
    "resolve_columns": resolve_columns,
    "transform_to_prepared": transform_to_prepared,
    "write_prepared_table": write_prepared_table,
    "generate_prepared_aggregations": generate_prepared_aggregations,
    "publish_semantic_model": publish_semantic_model,
    "prepare_and_write_data": prepare_and_write_data,
    "prepare_and_write_all_tables": prepare_and_write_all_tables,
}

__all__ = list(_EXPORT_REGISTRY.keys())

for _exported_function in _EXPORT_REGISTRY.values():
    _exported_function.__module__ = __name__

