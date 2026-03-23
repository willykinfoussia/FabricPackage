"""Prepared aggregation helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NumericType

from typing import Optional, List

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.io import read_lakehouse, write_lakehouse
from fabrictools.prepare.resolve import ResolvedColumn


def generate_prepared_aggregations(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    target_relative_path: str,
    resolved_mappings: List[ResolvedColumn],
    spark: Optional[SparkSession] = None,
) -> dict[str, str]:
    """
    Generate default prepared aggregations and write them to target lakehouse.
    """
    _spark = spark or get_spark()
    prepared_df = read_lakehouse(target_lakehouse_name, target_relative_path, spark=_spark)

    measure_cols = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["col_prepared"] in prepared_df.columns
        and mapping["semantic_type"].upper() in {"AMOUNT", "QUANTITY", "RATE"}
    ]
    date_cols = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["col_prepared"] in prepared_df.columns and mapping["semantic_type"].upper() == "DATE"
    ]
    code_cols = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["col_prepared"] in prepared_df.columns and mapping["semantic_type"].upper() == "CATEGORY"
    ]

    numeric_auto_measures = [
        field.name
        for field in prepared_df.schema.fields
        if field.name in prepared_df.columns and isinstance(field.dataType, NumericType)
    ]
    all_measures = sorted(set(measure_cols + numeric_auto_measures))
    if not all_measures:
        log("No numeric measures detected for aggregations.", level="warning")
        return {}

    def _build_agg(group_cols: list[str], table_name: str) -> str:
        aggregations = [F.sum(F.col(col_name)).alias(f"sum_{col_name}") for col_name in all_measures]
        if group_cols:
            agg_df = prepared_df.groupBy(*group_cols).agg(*aggregations)
        else:
            agg_df = prepared_df.agg(*aggregations)
        write_lakehouse(
            agg_df,
            lakehouse_name=target_lakehouse_name,
            relative_path=table_name,
            mode="overwrite",
            spark=_spark,
        )
        return table_name

    day_dims = date_cols[:1] + code_cols[:1]
    week_key = f"{date_cols[0]}_week_number" if date_cols else ""
    week_dims = [week_key] if week_key and week_key in prepared_df.columns else []
    region_dims = [col_name for col_name in code_cols if "region" in col_name.lower()][:1]
    if not region_dims:
        region_dims = code_cols[:1]

    outputs: dict[str, str] = {}
    outputs["prepared_agg_jour"] = _build_agg(day_dims, f"{target_relative_path}_prepared_agg_jour")
    outputs["prepared_agg_semaine"] = _build_agg(week_dims, f"{target_relative_path}_prepared_agg_semaine")
    outputs["prepared_agg_region"] = _build_agg(region_dims, f"{target_relative_path}_prepared_agg_region")

    # Keep source lakehouse argument explicit in API even if not used right now.
    _ = source_lakehouse_name
    return outputs

__all__ = ["generate_prepared_aggregations"]







