"""Prepared aggregation helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NumericType

from typing import List, Optional

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.io import read_lakehouse, write_lakehouse
from fabrictools.prepare.resolve import ResolvedColumn

# Substrings (lowercase) for geographic dimension name heuristics.
_GEO_NAME_KEYWORDS: tuple[str, ...] = (
    "region",
    "city",
    "country",
    "state",
    "province",
    "ville",
    "pays",
    "departement",
    "département",
    "county",
    "postal",
    "zip",
)

_GEO_CARDINALITY_THRESHOLD = 50


def _distinct_counts_by_column(df, cols: list[str]) -> dict[str, int]:
    if not cols:
        return {}
    row = df.agg(*[F.countDistinct(F.col(c)).alias(c) for c in cols]).collect()[0]
    return {c: int(row[c]) for c in cols}


def generate_prepared_aggregations(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    target_relative_path: str,
    resolved_mappings: List[ResolvedColumn],
    spark: Optional[SparkSession] = None,
) -> dict[str, str]:
    """
    Generate default prepared aggregations and write them to target lakehouse.

    Three tables are written:

    - ``prepared_agg_day``: groups by the first DAY column if present, otherwise
      the first DATE column, plus the CATEGORY column with lowest distinct count.
    - ``prepared_agg_week``: groups by the first available of: MONTH column,
      ``{day}_week_number`` (if present), ``{date}_week_number`` as fallback when
      only DATE-derived week columns exist in the prepared table, YEAR column,
      or no dimensions (global sums).
    - ``prepared_agg_region``: groups by the first CATEGORY whose name suggests
      geography (keyword match in mapping order), else the lowest-cardinality
      CATEGORY under 50 distinct values, else lowest-cardinality CATEGORY overall,
      or no dimensions if there are no categories.
    """
    _spark = spark or get_spark()
    prepared_df = read_lakehouse(target_lakehouse_name, target_relative_path, spark=_spark)
    cols_set = set(prepared_df.columns)

    day_cols: list[str] = []
    month_cols: list[str] = []
    year_cols: list[str] = []
    date_cols: list[str] = []
    category_cols_raw: list[str] = []

    for mapping in resolved_mappings:
        name = mapping["col_prepared"]
        if name not in cols_set:
            continue
        st = mapping["semantic_type"].upper()
        if st == "DAY":
            day_cols.append(name)
        elif st == "MONTH":
            month_cols.append(name)
        elif st == "YEAR":
            year_cols.append(name)
        elif st == "DATE":
            date_cols.append(name)
        elif st == "CATEGORY":
            category_cols_raw.append(name)

    distinct_counts = _distinct_counts_by_column(prepared_df, category_cols_raw)
    order_index = {c: i for i, c in enumerate(category_cols_raw)}
    category_cols = sorted(
        category_cols_raw,
        key=lambda c: (distinct_counts.get(c, 0), order_index.get(c, 0), c),
    )

    temporal_day = day_cols[0] if day_cols else None
    temporal_fallback = date_cols[0] if date_cols else None
    temporal_key = temporal_day or temporal_fallback
    categorical_key = category_cols[0] if category_cols else None
    day_dims = [
        c
        for c in (temporal_key, categorical_key)
        if c and c in cols_set
    ]

    week_dims: list[str] = []
    if month_cols and month_cols[0] in cols_set:
        week_dims = [month_cols[0]]
    else:
        week_key: Optional[str] = None
        for base in (day_cols[:1] + date_cols[:1]):
            candidate = f"{base}_week_number"
            if candidate in cols_set:
                week_key = candidate
                break
        if week_key:
            week_dims = [week_key]
        elif year_cols and year_cols[0] in cols_set:
            week_dims = [year_cols[0]]

    region_dims: list[str] = []
    region_keyword_col: Optional[str] = None
    for mapping in resolved_mappings:
        if mapping["semantic_type"].upper() != "CATEGORY":
            continue
        name = mapping["col_prepared"]
        if name not in cols_set:
            continue
        lower = name.lower()
        if any(k in lower for k in _GEO_NAME_KEYWORDS):
            region_keyword_col = name
            break
    if region_keyword_col:
        region_dims = [region_keyword_col]
    else:
        low_card_geo = [
            (c, distinct_counts[c])
            for c in category_cols_raw
            if distinct_counts.get(c, 0) < _GEO_CARDINALITY_THRESHOLD
        ]
        if low_card_geo:
            low_card_geo.sort(key=lambda x: (x[1], order_index.get(x[0], 0), x[0]))
            region_dims = [low_card_geo[0][0]]
        elif category_cols:
            region_dims = [category_cols[0]]

    dimension_cols = {c for c in day_dims + week_dims + region_dims if c}

    measure_cols = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["col_prepared"] in cols_set
        and mapping["semantic_type"].upper() in {"AMOUNT", "QUANTITY", "RATE"}
    ]
    numeric_auto_measures = [
        field.name
        for field in prepared_df.schema.fields
        if field.name in cols_set and isinstance(field.dataType, NumericType)
    ]
    all_measures = sorted(set(measure_cols + numeric_auto_measures) - dimension_cols)
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

    outputs: dict[str, str] = {}
    outputs["prepared_agg_day"] = _build_agg(day_dims, f"{target_relative_path}_Prepared_Agg_Day")
    outputs["prepared_agg_week"] = _build_agg(week_dims, f"{target_relative_path}_Prepared_Agg_Week")
    outputs["prepared_agg_region"] = _build_agg(region_dims, f"{target_relative_path}_Prepared_Agg_Region")

    _ = source_lakehouse_name
    return outputs


__all__ = ["generate_prepared_aggregations"]
