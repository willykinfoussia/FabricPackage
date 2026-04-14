"""Core shared utilities for fabrictools.

Exports logging, Spark session access, and Fabric path / JDBC resolution helpers.
See submodules :mod:`fabrictools.core.logging`, :mod:`fabrictools.core.paths`,
:mod:`fabrictools.core.spark`.
"""

from fabrictools.core.logging import log
from fabrictools.core.paths import (
    build_lakehouse_read_path_candidates,
    build_lakehouse_write_path,
    get_lakehouse_abfs_path,
    get_warehouse_jdbc_url,
)
from fabrictools.core.spark import get_spark

__all__ = [
    "log",
    "get_spark",
    "build_lakehouse_read_path_candidates",
    "build_lakehouse_write_path",
    "get_lakehouse_abfs_path",
    "get_warehouse_jdbc_url",
]
