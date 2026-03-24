"""I/O adapters for Fabric Lakehouse and Warehouse."""

from fabrictools.io.discovery import (
    filter_pipeline_discovered_tables,
    list_lakehouse_tables,
    list_lakehouse_tables_for_pipeline,
)
from fabrictools.io.lakehouse import (
    delete_all_lakehouse_tables,
    merge_lakehouse,
    read_lakehouse,
    resolve_lakehouse_read_candidate,
    write_lakehouse,
)
from fabrictools.io.warehouse import read_warehouse, write_warehouse

__all__ = [
    "read_lakehouse",
    "resolve_lakehouse_read_candidate",
    "write_lakehouse",
    "merge_lakehouse",
    "delete_all_lakehouse_tables",
    "read_warehouse",
    "write_warehouse",
    "filter_pipeline_discovered_tables",
    "list_lakehouse_tables",
    "list_lakehouse_tables_for_pipeline",
]

