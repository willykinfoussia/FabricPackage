"""
Generic data-cleaning and data-quality helpers for Microsoft Fabric datasets.

These helpers are intentionally storage-agnostic for DataFrame transformations.
The `clean_and_write_data` helper provides a convenience orchestration based on
Lakehouse read/write utilities.
"""

from __future__ import annotations

import re
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType

from fabrictools._logger import log
from fabrictools._spark import get_spark
from fabrictools.lakehouse import read_lakehouse, write_lakehouse

def _to_snake_case(name: str) -> str:
    """Convert a column name to snake_case."""
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", name.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    if not cleaned:
        return "col"
    if cleaned[0].isdigit():
        return f"col_{cleaned}"
    return cleaned


def _build_unique_column_names(columns: List[str]) -> List[str]:
    """
    Normalize columns to snake_case and ensure uniqueness.

    Duplicate names are suffixed with `_2`, `_3`, ...
    """
    seen: dict[str, int] = {}
    result: List[str] = []
    for col_name in columns:
        base = _to_snake_case(col_name)
        count = seen.get(base, 0) + 1
        seen[base] = count
        if count == 1:
            result.append(base)
        else:
            result.append(f"{base}_{count}")
    return result


def _normalized_name_collisions(columns: List[str]) -> dict[str, List[str]]:
    """Return mapping of normalized names that would collide."""
    grouped: dict[str, List[str]] = {}
    for col_name in columns:
        normalized = _to_snake_case(col_name)
        grouped.setdefault(normalized, []).append(col_name)
    return {
        normalized: originals
        for normalized, originals in grouped.items()
        if len(originals) > 1
    }


def clean_data(
    df: DataFrame,
    drop_duplicates: bool = True,
    drop_all_null_rows: bool = True,
) -> DataFrame:
    """
    Apply standard, generic data cleaning transformations.

    Standard cleaning includes:
    - normalize column names to unique snake_case names
    - trim string columns
    - convert blank strings to null
    - drop exact duplicate rows (optional)
    - drop rows where all columns are null (optional)
    """
    before_rows = df.count()
    before_cols = len(df.columns)

    # 1) Normalize column names to snake_case with collision-safe suffixes.
    normalized_columns = _build_unique_column_names(df.columns)
    cleaned_df = df.toDF(*normalized_columns)

    # 2) Trim + blank-to-null on string columns only.
    string_columns = [
        field.name
        for field in cleaned_df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    for col_name in string_columns:
        cleaned_df = cleaned_df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(
                F.trim(F.col(col_name))
            ),
        )

    # 3) Optional de-duplication.
    if drop_duplicates:
        cleaned_df = cleaned_df.dropDuplicates()

    # 4) Optional removal of fully-null rows.
    if drop_all_null_rows:
        cleaned_df = cleaned_df.dropna(how="all")

    after_rows = cleaned_df.count()
    after_cols = len(cleaned_df.columns)
    log(
        f"Data cleaned: rows {before_rows:,} -> {after_rows:,} | "
        f"columns {before_cols} -> {after_cols}"
    )
    return cleaned_df


def scan_data_errors(df: DataFrame, include_samples: bool = True) -> dict:
    """
    Scan a DataFrame and report common data-quality issues.

    Report includes:
    - null count per column
    - blank-string count per string column
    - exact duplicate row count
    - potential collisions after snake_case normalization
    """
    total_rows = df.count()
    total_columns = len(df.columns)

    null_count_exprs = [
        F.sum(F.when(F.col(col_name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            col_name
        )
        for col_name in df.columns
    ]
    null_counts_row = df.agg(*null_count_exprs).collect()[0].asDict()

    string_columns = [
        field.name for field in df.schema.fields if isinstance(field.dataType, StringType)
    ]
    blank_counts: dict[str, int] = {}
    if string_columns:
        blank_exprs = [
            F.sum(
                F.when(F.trim(F.col(col_name)) == "", F.lit(1)).otherwise(F.lit(0))
            ).alias(col_name)
            for col_name in string_columns
        ]
        blank_counts = df.agg(*blank_exprs).collect()[0].asDict()

    distinct_rows = df.distinct().count()
    duplicate_rows = total_rows - distinct_rows
    name_collisions = _normalized_name_collisions(df.columns)

    report = {
        "row_count": total_rows,
        "column_count": total_columns,
        "null_counts": null_counts_row,
        "blank_string_counts": blank_counts,
        "duplicate_row_count": duplicate_rows,
        "normalized_name_collisions": name_collisions,
    }

    if include_samples:
        report["sample_rows"] = [row.asDict(recursive=True) for row in df.limit(10).collect()]

    null_columns = sum(1 for value in null_counts_row.values() if value > 0)
    blank_columns = sum(1 for value in blank_counts.values() if value > 0)
    collision_count = len(name_collisions)
    log(
        "Data scan complete: "
        f"rows={total_rows:,}, columns={total_columns}, "
        f"duplicate_rows={duplicate_rows:,}, "
        f"columns_with_nulls={null_columns}, "
        f"string_columns_with_blanks={blank_columns}, "
        f"normalized_name_collisions={collision_count}"
    )
    return report


def clean_and_write_data(
    source_lakehouse_name: str,
    source_relative_path: str,
    target_lakehouse_name: str,
    target_relative_path: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Read data, apply standard cleaning, and write it to a target path.

    Returns the cleaned DataFrame for downstream reuse.
    """
    _spark = spark or get_spark()
    source_df = read_lakehouse(source_lakehouse_name, source_relative_path, spark=_spark)
    cleaned_df = clean_data(source_df)
    write_lakehouse(
        cleaned_df,
        lakehouse_name=target_lakehouse_name,
        relative_path=target_relative_path,
        mode=mode,
        partition_by=partition_by,
        spark=_spark,
    )
    return cleaned_df
