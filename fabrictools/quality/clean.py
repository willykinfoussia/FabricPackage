"""Pure DataFrame cleaning helpers."""

from __future__ import annotations

import re
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType

from fabrictools.core import log
from fabrictools.io import resolve_lakehouse_read_candidate


def _to_snake_case(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", name.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    if not cleaned:
        return "col"
    if cleaned[0].isdigit():
        return f"col_{cleaned}"
    return cleaned


def _build_unique_column_names(columns: List[str]) -> List[str]:
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
    grouped: dict[str, List[str]] = {}
    for col_name in columns:
        normalized = _to_snake_case(col_name)
        grouped.setdefault(normalized, []).append(col_name)
    return {
        normalized: originals
        for normalized, originals in grouped.items()
        if len(originals) > 1
    }


def _replace_empty_strings_with_nulls(df: DataFrame) -> DataFrame:
    string_columns = [
        field.name for field in df.schema.fields if isinstance(field.dataType, StringType)
    ]
    transformed_df = df
    for col_name in string_columns:
        transformed_df = transformed_df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(
                F.trim(F.col(col_name))
            ),
        )
    return transformed_df


def add_silver_metadata(
    df: DataFrame,
    source_lakehouse_name: str,
    source_relative_path: str,
    source_layer: str = "bronze",
    ingestion_timestamp_col: str = "_ingestion_timestamp",
    source_layer_col: str = "_source_layer",
    source_path_col: str = "_source_path",
    year_col: str = "_year",
    month_col: str = "_month",
    day_col: str = "_day",
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    partition_source_col = next(
        (
            field.name
            for field in df.schema.fields
            if field.dataType.typeName()
            in {"date", "timestamp", "timestamp_ntz", "timestamp_ltz"}
            and field.name != ingestion_timestamp_col
        ),
        None,
    )

    resolved_source_path = resolve_lakehouse_read_candidate(
        lakehouse_name=source_lakehouse_name,
        relative_path=source_relative_path,
        spark=spark,
    )

    partition_expression = F.col(partition_source_col or ingestion_timestamp_col)

    metadata_df = (
        df.withColumn(ingestion_timestamp_col, F.current_timestamp())
        .withColumn(source_layer_col, F.lit(source_layer))
        .withColumn(source_path_col, F.lit(resolved_source_path))
        .withColumn(year_col, F.year(partition_expression))
        .withColumn(month_col, F.month(partition_expression))
        .withColumn(day_col, F.dayofmonth(partition_expression))
    )
    partition_source_label = partition_source_col or ingestion_timestamp_col
    log(
        "Silver metadata added: "
        f"{ingestion_timestamp_col}, {source_layer_col}, {source_path_col}, "
        f"{year_col}, {month_col}, {day_col} "
        f"(partition source: {partition_source_label})"
    )
    return metadata_df


def clean_data(
    df: DataFrame,
    drop_duplicates: bool = True,
    drop_all_null_rows: bool = True,
) -> DataFrame:
    before_rows = df.count()
    before_cols = len(df.columns)

    normalized_columns = _build_unique_column_names(df.columns)
    cleaned_df = df.toDF(*normalized_columns)
    cleaned_df = _replace_empty_strings_with_nulls(cleaned_df)

    if drop_duplicates:
        cleaned_df = cleaned_df.dropDuplicates()
    if drop_all_null_rows:
        cleaned_df = cleaned_df.dropna(how="all")

    after_rows = cleaned_df.count()
    after_cols = len(cleaned_df.columns)
    log(
        f"Data cleaned: rows {before_rows:,} -> {after_rows:,} | "
        f"columns {before_cols} -> {after_cols}"
    )
    return cleaned_df


__all__ = [
    "clean_data",
    "add_silver_metadata",
    "_to_snake_case",
    "_build_unique_column_names",
    "_normalized_name_collisions",
    "_replace_empty_strings_with_nulls",
]

