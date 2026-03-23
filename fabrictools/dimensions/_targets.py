"""Shared write helpers for dimension builders."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from fabrictools.core import get_spark
from fabrictools.io import write_lakehouse, write_warehouse


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
) -> None:
    _spark = spark or get_spark()
    if lakehouse_name:
        write_lakehouse(
            df=df,
            lakehouse_name=lakehouse_name,
            relative_path=lakehouse_relative_path or default_relative_path,
            mode=mode,
            spark=_spark,
        )
    if warehouse_name:
        write_warehouse(
            df=df,
            warehouse_name=warehouse_name,
            table=warehouse_table,
            mode=mode,
            batch_size=batch_size,
            spark=_spark,
        )


__all__ = ["_write_dimension_targets"]
