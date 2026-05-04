"""I/O adapters for Fabric Lakehouse and Warehouse (see :mod:`fabrictools.io.lakehouse`, :mod:`fabrictools.io.warehouse`, :mod:`fabrictools.io.discovery`)."""

from fabrictools.io.discovery import (
    filter_pipeline_discovered_tables,
    list_lakehouse_tables,
    list_lakehouse_tables_for_pipeline,
)
from fabrictools.io.lakehouse import (
    delete_all_lakehouse_tables,
    lakehouse_table_exists,
    merge_lakehouse,
    read_lakehouse,
    read_lakehouses,
    resolve_lakehouse_read_candidate,
    write_lakehouse,
    write_lakehouses,
)
from fabrictools.io.warehouse import read_warehouse, write_warehouse

__all__ = [
    "read_lakehouse",
    "read_lakehouses",
    "resolve_lakehouse_read_candidate",
    "write_lakehouse",
    "write_lakehouses",
    "merge_lakehouse",
    "lakehouse_table_exists",
    "delete_all_lakehouse_tables",
    "read_warehouse",
    "write_warehouse",
    "filter_pipeline_discovered_tables",
    "list_lakehouse_tables",
    "list_lakehouse_tables_for_pipeline",
]

