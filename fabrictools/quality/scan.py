"""Data quality scan/report helpers."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

from fabrictools.core import log
from fabrictools.quality.clean import _normalized_name_collisions, _replace_empty_strings_with_nulls

try:
    import plotly.express as px
except ImportError:  # pragma: no cover
    px = None


def scan_data_errors(
    df: DataFrame,
    include_samples: bool = True,
    display_results: bool = True,
) -> dict[str, Any]:
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
    issue_totals = [
        {"issue_type": "duplicate_rows", "count": duplicate_rows},
        {"issue_type": "null_values", "count": sum(null_counts_row.values())},
        {"issue_type": "blank_string_values", "count": sum(blank_counts.values())},
        {"issue_type": "normalized_name_collisions", "count": len(name_collisions)},
    ]
    non_zero_issue_totals = [item for item in issue_totals if item["count"] > 0]

    figure = None
    if px is None:
        log("Plotly is not installed; figure is omitted.", level="warning")
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

    report: dict[str, Any] = {
        "summary_df": summary_df,
        "figure": figure,
        "issue_totals": issue_totals,
        "collisions": name_collisions,
    }
    if include_samples:
        report["sample_rows"] = [row.asDict(recursive=True) for row in df.limit(10).collect()]

    if display_results:
        display(summary_df)
        if figure is not None:
            figure.show()

    return report


__all__ = ["scan_data_errors", "px"]

