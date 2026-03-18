"""
Generic data-cleaning and data-quality helpers for Microsoft Fabric datasets.

These helpers are intentionally storage-agnostic for DataFrame transformations.
The `clean_and_write_data` helper provides a convenience orchestration based on
Lakehouse read/write utilities.
"""

from __future__ import annotations

import re
from typing import Any, List, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType

from fabrictools._logger import log
from fabrictools._spark import get_spark
from fabrictools.lakehouse import (
    read_lakehouse,
    resolve_lakehouse_read_candidate,
    write_lakehouse,
)

try:
    import plotly.express as px
except ImportError:  # pragma: no cover - optional dependency in runtime.
    px = None

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


def _replace_empty_strings_with_nulls(df: DataFrame) -> DataFrame:
    """Trim string columns and replace empty values with null."""
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
    """
    Add Silver-layer metadata and date partition columns.

    Added columns:
    - ingestion timestamp
    - source layer
    - source path (resolved candidate path used for reading)
    - date partitions: year, month, day (derived from first date/timestamp column
      in source data, fallback to ingestion timestamp)
    """
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

    # 2) Trim + empty-string-to-null on string columns only.
    cleaned_df = _replace_empty_strings_with_nulls(cleaned_df)

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


def scan_data_errors(
    df: DataFrame,
    include_samples: bool = True,
    display_results: bool = True,
) -> dict[str, Any]:
    """
    Scan a DataFrame and return user-friendly data-quality artifacts.

    Returned bundle includes:
    - summary_df: Spark DataFrame with all scan metrics in tabular form
    - figure: Plotly chart (auto bar/pie) showing issue totals
    - issue_totals: compact list of total counts per issue category
    - collisions: normalized column-name collision details
    - sample_rows: optional preview rows when include_samples=True

    Parameters
    ----------
    display_results:
        If True (default), display the summary DataFrame and figure immediately.
    """
    log("Scanning data quality issues...")
    normalized_df = _replace_empty_strings_with_nulls(df)
    total_rows = df.count()
    total_columns = len(df.columns)

    null_count_exprs = [
        F.sum(
            F.when(F.col(col_name).isNull(), F.lit(1)).otherwise(F.lit(0))
        ).alias(col_name)
        for col_name in normalized_df.columns
    ]
    null_counts_row = normalized_df.agg(*null_count_exprs).collect()[0].asDict()

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

    log(
        "  Metrics collected: "
        f"rows={total_rows:,}, columns={total_columns}, duplicate_rows={duplicate_rows:,}"
    )

    summary_records: list[dict[str, Any]] = [
        {
            "issue_type": "dataset_rows",
            "column_name": None,
            "count": total_rows,
            "details": "Total number of rows in the dataset",
        },
        {
            "issue_type": "dataset_columns",
            "column_name": None,
            "count": total_columns,
            "details": "Total number of columns in the dataset",
        },
        {
            "issue_type": "duplicate_rows",
            "column_name": None,
            "count": duplicate_rows,
            "details": "Exact duplicate rows found in the dataset",
        },
    ]

    summary_records.extend(
        {
            "issue_type": "null_values",
            "column_name": col_name,
            "count": count,
            "details": "Null values after string normalization",
        }
        for col_name, count in null_counts_row.items()
    )

    summary_records.extend(
        {
            "issue_type": "blank_string_values",
            "column_name": col_name,
            "count": count,
            "details": "Blank string values before normalization",
        }
        for col_name, count in blank_counts.items()
    )

    summary_records.extend(
        {
            "issue_type": "normalized_name_collisions",
            "column_name": normalized_name,
            "count": len(original_columns),
            "details": ", ".join(original_columns),
        }
        for normalized_name, original_columns in name_collisions.items()
    )

    summary_df = df.sparkSession.createDataFrame(summary_records)
    log(f"  Summary DataFrame built with {len(summary_records)} rows")

    issue_totals = [
        {"issue_type": "duplicate_rows", "count": duplicate_rows},
        {"issue_type": "null_values", "count": sum(null_counts_row.values())},
        {"issue_type": "blank_string_values", "count": sum(blank_counts.values())},
        {"issue_type": "normalized_name_collisions", "count": len(name_collisions)},
    ]
    non_zero_issue_totals = [item for item in issue_totals if item["count"] > 0]
    figure = None
    if px is None:
        log(
            "  Plotly is not installed; figure is omitted. "
            "Install with `pip install plotly` to enable charts.",
            level="warning",
        )
    else:
        chart_data = non_zero_issue_totals or issue_totals
        issue_labels = [item["issue_type"] for item in chart_data]
        issue_values = [item["count"] for item in chart_data]
        if len(chart_data) <= 3:
            figure = px.pie(
                names=issue_labels,
                values=issue_values,
                title="Data quality issues distribution",
            )
        else:
            figure = px.bar(
                x=issue_labels,
                y=issue_values,
                title="Data quality issues overview",
                labels={"x": "Issue type", "y": "Count"},
            )
        log(f"  Plotly figure built (chart_points={len(chart_data)})")

    report: dict[str, Any] = {
        "summary_df": summary_df,
        "figure": figure,
        "issue_totals": issue_totals,
        "collisions": name_collisions,
    }

    if include_samples:
        report["sample_rows"] = [row.asDict(recursive=True) for row in df.limit(10).collect()]

    if display_results:
        log("  Displaying summary DataFrame and figure...")
        summary_df.show(truncate=False)
        if figure is not None:
            figure.show()
        else:
            log("  Figure display skipped (plotly figure unavailable).", level="warning")

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
    if non_zero_issue_totals:
        log(
            "  Quality issues detected: "
            + ", ".join(
                f"{item['issue_type']}={item['count']:,}"
                for item in non_zero_issue_totals
            ),
            level="warning",
        )
    else:
        log("  No quality issues detected.")
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
    Read data, clean it, add Silver metadata columns, and write to target path.

    Returns the Silver-enriched DataFrame for downstream reuse.
    """
    _spark = spark or get_spark()
    source_df = read_lakehouse(source_lakehouse_name, source_relative_path, spark=_spark)
    cleaned_df = clean_data(source_df)
    silver_df = add_silver_metadata(
        cleaned_df,
        source_lakehouse_name=source_lakehouse_name,
        source_relative_path=source_relative_path,
        spark=_spark,
    )
    write_lakehouse(
        silver_df,
        lakehouse_name=target_lakehouse_name,
        relative_path=target_relative_path,
        mode=mode,
        partition_by=partition_by,
        spark=_spark,
    )
    return silver_df
