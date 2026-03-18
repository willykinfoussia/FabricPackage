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
from fabrictools._paths import get_lakehouse_abfs_path
from fabrictools._spark import get_spark
from fabrictools.lakehouse import (
    merge_lakehouse,
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
) -> None:
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
        #summary_df.show(truncate=False)
        display(summary_df)
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
    #return report


def _get_fs_entry_name(fs_entry: Any) -> str:
    """Extract a clean directory/file name from a notebookutils.fs.ls entry."""
    raw_name = getattr(fs_entry, "name", "")
    if raw_name:
        return str(raw_name).strip().strip("/")

    raw_path = getattr(fs_entry, "path", "")
    if raw_path:
        return str(raw_path).strip().strip("/").split("/")[-1]

    return ""


def _list_lakehouse_table_paths(
    lakehouse_name: str,
    include_schemas: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
) -> List[str]:
    """
    List table relative paths from a Lakehouse as ``Tables/<schema>/<table>``.

    Discovery is file-system based, by scanning ``<abfs>/Tables/<schema>/<table>``.
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415
    except ImportError as exc:
        raise ValueError(
            f"notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc

    included_schema_names = (
        {schema_name.strip().lower() for schema_name in include_schemas}
        if include_schemas
        else None
    )

    excluded_table_names = {
        table_name.strip().lower()
        for table_name in (exclude_tables or [])
    }

    base = get_lakehouse_abfs_path(lakehouse_name)
    tables_root = f"{base}/Tables"
    discovered_table_paths: List[str] = []

    for schema_entry in notebookutils.fs.ls(tables_root):
        schema_name = _get_fs_entry_name(schema_entry)
        if not schema_name:
            continue
        schema_name_lower = schema_name.lower()
        if (
            included_schema_names is not None
            and schema_name_lower not in included_schema_names
        ):
            continue

        schema_path = getattr(schema_entry, "path", f"{tables_root}/{schema_name}")
        for table_entry in notebookutils.fs.ls(schema_path):
            table_name = _get_fs_entry_name(table_entry)
            if not table_name:
                continue

            qualified_table_name = f"{schema_name_lower}.{table_name.lower()}"
            if (
                table_name.lower() in excluded_table_names
                or qualified_table_name in excluded_table_names
            ):
                continue

            discovered_table_paths.append(f"Tables/{schema_name}/{table_name}")

    return sorted(discovered_table_paths)


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


def clean_and_write_all_tables(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    tables_config: Optional[List[dict[str, Any]]] = None,
    include_schemas: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    continue_on_error: bool = False,
    spark: Optional[SparkSession] = None,
) -> dict[str, Any]:
    """
    Clean and replicate all discovered Lakehouse tables from source to target.

    If ``tables_config`` is provided, each config entry drives one table job.
    Otherwise, tables are discovered under ``Tables/<schema>/<table>`` in the
    source Lakehouse and replicated with the same relative path in target.
    """
    supported_modes = {"overwrite", "append", "merge"}
    _spark = spark or get_spark()

    table_jobs: List[dict[str, Any]] = []
    if tables_config is not None:
        for entry_index, table_config in enumerate(tables_config, start=1):
            if not isinstance(table_config, dict):
                raise ValueError(
                    f"tables_config[{entry_index}] must be a dict, got "
                    f"{type(table_config).__name__}."
                )

            source_relative_path = str(table_config.get("bronze_path", "")).strip()
            if not source_relative_path:
                raise ValueError(
                    f"tables_config[{entry_index}] is missing required key "
                    f"'bronze_path'."
                )

            target_relative_path = str(table_config.get("silver_table", "")).strip()
            if not target_relative_path:
                raise ValueError(
                    f"tables_config[{entry_index}] is missing required key "
                    f"'silver_table'."
                )

            raw_mode = str(table_config.get("mode", "")).strip().lower()
            if not raw_mode:
                raise ValueError(
                    f"tables_config[{entry_index}] is missing required key 'mode'."
                )
            if raw_mode not in supported_modes:
                raise ValueError(
                    f"tables_config[{entry_index}] has unsupported mode '{raw_mode}'. "
                    f"Supported modes: overwrite, append, merge."
                )

            if "partition_by" in table_config:
                raw_partition_by = table_config["partition_by"]
                if raw_partition_by is None:
                    effective_partition_by = None
                elif isinstance(raw_partition_by, list):
                    effective_partition_by = raw_partition_by
                else:
                    raise ValueError(
                        f"tables_config[{entry_index}] key 'partition_by' must be "
                        "a list or None."
                    )
            else:
                effective_partition_by = partition_by

            merge_condition = table_config.get("merge_condition")
            if raw_mode == "merge" and not str(merge_condition or "").strip():
                raise ValueError(
                    f"tables_config[{entry_index}] mode='merge' requires "
                    f"'merge_condition'."
                )

            table_jobs.append(
                {
                    "source_relative_path": source_relative_path,
                    "target_relative_path": target_relative_path,
                    "mode": raw_mode,
                    "partition_by": effective_partition_by,
                    "merge_condition": str(merge_condition).strip()
                    if merge_condition is not None
                    else None,
                }
            )
    else:
        table_relative_paths = _list_lakehouse_table_paths(
            lakehouse_name=source_lakehouse_name,
            include_schemas=include_schemas,
            exclude_tables=exclude_tables,
        )
        table_jobs = [
            {
                "source_relative_path": table_relative_path,
                "target_relative_path": table_relative_path,
                "mode": mode,
                "partition_by": partition_by,
                "merge_condition": None,
            }
            for table_relative_path in table_relative_paths
        ]

    if not table_jobs:
        log(
            f"No tables found in Lakehouse '{source_lakehouse_name}' for bulk clean/write.",
            level="warning",
        )
        return {
            "total_tables": 0,
            "successful_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }

    processed_tables: List[dict[str, str]] = []
    failures: List[dict[str, str]] = []
    total_tables = len(table_jobs)

    log(
        f"Bulk clean/write started: {total_tables} table(s) "
        f"from '{source_lakehouse_name}' to '{target_lakehouse_name}'."
    )

    for index, table_job in enumerate(table_jobs, start=1):
        source_relative_path = table_job["source_relative_path"]
        target_relative_path = table_job["target_relative_path"]
        table_mode = table_job["mode"]
        table_partition_by = table_job["partition_by"]
        merge_condition = table_job["merge_condition"]
        log(
            f"[{index}/{total_tables}] Processing '{source_relative_path}' "
            f"-> '{target_relative_path}' [mode={table_mode}]..."
        )
        try:
            if table_mode in {"overwrite", "append"}:
                clean_and_write_data(
                    source_lakehouse_name=source_lakehouse_name,
                    source_relative_path=source_relative_path,
                    target_lakehouse_name=target_lakehouse_name,
                    target_relative_path=target_relative_path,
                    mode=table_mode,
                    partition_by=table_partition_by,
                    spark=_spark,
                )
            else:
                source_df = read_lakehouse(
                    source_lakehouse_name,
                    source_relative_path,
                    spark=_spark,
                )
                cleaned_df = clean_data(source_df)
                silver_df = add_silver_metadata(
                    cleaned_df,
                    source_lakehouse_name=source_lakehouse_name,
                    source_relative_path=source_relative_path,
                    spark=_spark,
                )
                merge_lakehouse(
                    source_df=silver_df,
                    lakehouse_name=target_lakehouse_name,
                    relative_path=target_relative_path,
                    merge_condition=merge_condition,
                    spark=_spark,
                )

            processed_tables.append(
                {
                    "source_relative_path": source_relative_path,
                    "target_relative_path": target_relative_path,
                    "mode": table_mode,
                }
            )
            log(f"[{index}/{total_tables}] Success for '{source_relative_path}'.")
        except Exception as exc:
            failure = {
                "source_relative_path": source_relative_path,
                "target_relative_path": target_relative_path,
                "mode": table_mode,
                "error": str(exc),
            }
            failures.append(failure)
            log(
                f"[{index}/{total_tables}] Failed for '{source_relative_path}': {exc}",
                level="warning",
            )
            if not continue_on_error:
                raise

    log(
        "Bulk clean/write completed: "
        f"successful={len(processed_tables)}, failed={len(failures)}."
    )
    return {
        "total_tables": total_tables,
        "successful_tables": len(processed_tables),
        "failed_tables": len(failures),
        "tables": processed_tables,
        "failures": failures,
    }
