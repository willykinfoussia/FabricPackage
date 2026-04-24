"""fabrictools — PySpark helpers for Microsoft Fabric Lakehouses and Warehouses.

This package re-exports a single flat API (see ``__all__``). Full parameter lists and
behaviour are documented on the **defining modules** (and mirrored in Sphinx):

* :mod:`fabrictools.io` — read/write/merge Lakehouse, Warehouse JDBC, discovery
* :mod:`fabrictools.quality` — ``clean_data``, Silver metadata, scans, bronze→silver pipelines
* :mod:`fabrictools.prepare` — prepared-layer schema, resolution, transforms, semantic publish
* :mod:`fabrictools.dimensions` — date / country / city dimensions and orchestration
* :mod:`fabrictools.transform` — column renames, filters, joins, wide-month reshapes
* :mod:`fabrictools.pipelines` — shared bulk job config (``tables_config`` parsing)
"""

from __future__ import annotations
from fabrictools._version import __version__

from fabrictools.dimensions import (
    build_dimension_city,
    build_dimension_country,
    build_dimension_date,
    generate_dimensions,
)
from fabrictools.io.lakehouse import (
    delete_all_lakehouse_tables,
    lakehouse_table_exists,
    merge_lakehouse,
    read_lakehouse,
    write_lakehouse,
)
from fabrictools.io.warehouse import read_warehouse, write_warehouse
from fabrictools.prepare import (
    generate_prepared_aggregations,
    make_business_ready,
    prepare_and_write_all_tables,
    prepare_and_write_data,
    publish_semantic_model,
    resolve_columns,
    snapshot_source_schema,
    transform_to_prepared,
    write_prepared_table,
)
from fabrictools.quality.clean import add_silver_metadata, clean_data
from fabrictools.quality.pipeline import (
    clean_and_write_all_tables,
    clean_and_write_data,
)
from fabrictools.quality.scan import scan_data_errors
from fabrictools.transform import (
    build_tcd,
    cast_columns,
    coalesce_dim,
    dataframe_last_nonnull_wide_month_from_long,
    dataframe_pivot_category_wide_month_from_long,
    dataframe_unpivot_wide_month_suffix,
    drop_rows_over_empty_percent,
    empty_or_null,
    filter_by_value_list,
    merge_dataframes,
    month_start_from_ca_monthly_col,
    norm_text,
    remove_columns,
    rename_columns_normalized,
    rename_columns_month_year_block_labels,
    rename_columns_pq_serial_to_dates,
    rename_columns_pq_serial_to_mois_annee,
    resolve_dataframe_column,
    transform_wide_month_suffix,
    wide_value_columns,
)

_EXPORT_REGISTRY = {
    "lakehouse_table_exists": lakehouse_table_exists,
    "read_lakehouse": read_lakehouse,
    "write_lakehouse": write_lakehouse,
    "merge_lakehouse": merge_lakehouse,
    "delete_all_lakehouse_tables": delete_all_lakehouse_tables,
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
    "make_business_ready": make_business_ready,
    "filter_by_value_list": filter_by_value_list,
    "drop_rows_over_empty_percent": drop_rows_over_empty_percent,
    "cast_columns": cast_columns,
    "merge_dataframes": merge_dataframes,
    "remove_columns": remove_columns,
    "rename_columns_normalized": rename_columns_normalized,
    "rename_columns_pq_serial_to_dates": rename_columns_pq_serial_to_dates,
    "rename_columns_pq_serial_to_mois_annee": rename_columns_pq_serial_to_mois_annee,
    "rename_columns_month_year_block_labels": rename_columns_month_year_block_labels,
    "month_start_from_ca_monthly_col": month_start_from_ca_monthly_col,
    "resolve_dataframe_column": resolve_dataframe_column,
    "wide_value_columns": wide_value_columns,
    "dataframe_unpivot_wide_month_suffix": dataframe_unpivot_wide_month_suffix,
    "dataframe_last_nonnull_wide_month_from_long": dataframe_last_nonnull_wide_month_from_long,
    "dataframe_pivot_category_wide_month_from_long": dataframe_pivot_category_wide_month_from_long,
    "transform_wide_month_suffix": transform_wide_month_suffix,
    "norm_text": norm_text,
    "empty_or_null": empty_or_null,
    "coalesce_dim": coalesce_dim,
    "build_tcd": build_tcd,
}

__all__ = list(_EXPORT_REGISTRY.keys())

for _exported_function in _EXPORT_REGISTRY.values():
    _exported_function.__module__ = __name__
