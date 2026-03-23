"""Lakehouse I/O facade module."""

from fabrictools.lakehouse import (
    merge_lakehouse,
    read_lakehouse,
    resolve_lakehouse_read_candidate,
    write_lakehouse,
)

__all__ = [
    "read_lakehouse",
    "resolve_lakehouse_read_candidate",
    "write_lakehouse",
    "merge_lakehouse",
]

