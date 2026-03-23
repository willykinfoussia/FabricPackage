"""I/O adapters for Fabric Lakehouse and Warehouse."""

from fabrictools.io.discovery import list_lakehouse_tables
from fabrictools.io.lakehouse import (
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
    "read_warehouse",
    "write_warehouse",
    "list_lakehouse_tables",
]

