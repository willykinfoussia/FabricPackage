"""Shared write helpers for dimension builders."""

from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

from fabrictools.core import get_spark
from fabrictools.io import write_lakehouse, write_warehouse


def _jdbc_mode_for_dimension(mode: str) -> str:
    """Warehouse JDBC writes only support overwrite/append."""
    lowered = str(mode).strip().lower()
    return "overwrite" if lowered in ("upsert", "merge") else lowered


def _write_dimension_targets(
    df: DataFrame,
    lakehouse_name: Optional[str],
    lakehouse_relative_path: Optional[str],
    warehouse_name: Optional[str],
    warehouse_table: Optional[str],
    default_relative_path: str,
    mode: str = "overwrite",
    batch_size: int = 10_000,
    spark: Optional[SparkSession] = None,
    *,
    merge_condition: Optional[str] = None,
    upsert_key_columns: Optional[Sequence[str]] = None,
) -> None:
    _spark = spark or get_spark()
    if lakehouse_name:
        write_lakehouse(
            df=df,
            lakehouse_name=lakehouse_name,
            relative_path=lakehouse_relative_path or default_relative_path,
            mode=mode,
            merge_condition=merge_condition,
            upsert_key_columns=upsert_key_columns,
            spark=_spark,
            auto_partition=False,
        )
    if warehouse_name:
        write_warehouse(
            df=df,
            warehouse_name=warehouse_name,
            table=warehouse_table,
            mode=_jdbc_mode_for_dimension(mode),
            batch_size=batch_size,
            spark=_spark,
        )


__all__ = ["_write_dimension_targets"]
