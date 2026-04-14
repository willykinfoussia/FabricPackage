"""Unpivot and reshape wide month columns that share a block suffix (e.g. `` [CA Monthly]``)."""

from __future__ import annotations

from collections.abc import Callable, Collection, Sequence
from datetime import date
from typing import Literal, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

from fabrictools.transform.columns import (
    month_start_from_ca_monthly_col,
    resolve_dataframe_column,
)


def wide_value_columns(
    df: DataFrame, *, suffix: str, exclude: Collection[str] = ()
) -> list[str]:
    """Return physical column names that end with *suffix* and are not in *exclude*."""
    ex = set(exclude)
    return [c for c in df.columns if c.endswith(suffix) and c not in ex]


def _resolve_id_columns(df: DataFrame, id_columns: Sequence[str]) -> list[str]:
    return [resolve_dataframe_column(df, c) for c in id_columns]


def _collect_value_columns(
    df: DataFrame,
    *,
    value_columns: Optional[Sequence[str]],
    value_columns_suffix: Optional[str],
    exclude_columns: Collection[str],
) -> list[str]:
    if value_columns is not None:
        out = [resolve_dataframe_column(df, c) for c in value_columns]
    elif value_columns_suffix is not None:
        out = wide_value_columns(
            df, suffix=value_columns_suffix, exclude=exclude_columns
        )
    else:
        raise ValueError(
            "Provide value_columns or value_columns_suffix (one of them is required)."
        )
    # Drop duplicates while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for c in out:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    return deduped


def _empty_long_dataframe(
    df: DataFrame,
    *,
    id_resolved: Sequence[str],
    variable_column: str,
    value_column: str,
    month_start_column: str,
) -> DataFrame:
    spark = df.sparkSession
    type_by_name = {f.name: f.dataType for f in df.schema.fields}
    fields: list[StructField] = []
    for c in id_resolved:
        fields.append(StructField(c, type_by_name[c], True))
    fields.extend(
        [
            StructField(variable_column, StringType(), True),
            StructField(value_column, DoubleType(), True),
            StructField(month_start_column, DateType(), True),
        ]
    )
    return spark.createDataFrame([], StructType(fields))


def dataframe_unpivot_wide_month_suffix(
    df: DataFrame,
    *,
    id_columns: Sequence[str],
    value_columns_suffix: Optional[str] = None,
    value_columns: Optional[Sequence[str]] = None,
    exclude_columns: Collection[str] = (),
    variable_column: str = "MoisCol",
    value_column: str = "Valeur",
    month_start_column: str = "MonthStart",
    month_start_from_column_name: Callable[[str], Optional[date]] = month_start_from_ca_monthly_col,
) -> DataFrame:
    """
    Select id + wide value columns, unpivot, and add *month_start_column* from the variable name.

    If *value_columns* is passed, it wins over *value_columns_suffix*.
    """
    id_resolved = _resolve_id_columns(df, id_columns)
    id_set = set(id_resolved)
    value_cols = _collect_value_columns(
        df,
        value_columns=value_columns,
        value_columns_suffix=value_columns_suffix,
        exclude_columns=exclude_columns,
    )
    value_cols = [c for c in value_cols if c not in id_set]

    if not value_cols:
        return _empty_long_dataframe(
            df,
            id_resolved=id_resolved,
            variable_column=variable_column,
            value_column=value_column,
            month_start_column=month_start_column,
        )

    selected = df.select(*id_resolved, *value_cols)
    long_df = selected.unpivot(
        ids=list(id_resolved),
        values=value_cols,
        variableColumnName=variable_column,
        valueColumnName=value_column,
    )

    def _parse_cell(name: Optional[str]) -> Optional[date]:
        if name is None:
            return None
        return month_start_from_column_name(str(name))

    month_udf = F.udf(_parse_cell, DateType())
    return long_df.withColumn(month_start_column, month_udf(F.col(variable_column)))


def dataframe_last_nonnull_wide_month_from_long(
    long_df: DataFrame,
    *,
    order_column: str,
    variable_column: str = "MoisCol",
    value_column: str = "Valeur",
    month_start_column: str = "MonthStart",
    output_month_start: str = "MonthStart",
    output_year: str = "Year",
    output_month: str = "Month",
    output_value: str = "Value",
) -> DataFrame:
    """
    Keep the last non-null *value_column* per *variable_column* (by *order_column* desc), then cast.
    """
    spark = long_df.sparkSession
    if variable_column not in long_df.columns:
        empty = StructType(
            [
                StructField(output_month_start, DateType(), True),
                StructField(output_year, LongType(), True),
                StructField(output_month, LongType(), True),
                StructField(output_value, DoubleType(), True),
            ]
        )
        return spark.createDataFrame([], empty)

    w = Window.partitionBy(F.col(variable_column)).orderBy(
        F.col(order_column).desc()
    )
    ranked = (
        long_df.filter(F.col(value_column).isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    with_month = ranked.filter(F.col(month_start_column).isNotNull())
    typed = with_month.select(
        F.col(month_start_column).cast(DateType()).alias(output_month_start),
        F.year(F.col(month_start_column)).cast(LongType()).alias(output_year),
        F.month(F.col(month_start_column)).cast(LongType()).alias(output_month),
        F.col(value_column).cast(DoubleType()).alias(output_value),
    )
    return typed


def dataframe_pivot_category_wide_month_from_long(
    long_df: DataFrame,
    *,
    category_column: str,
    pivot_categories: Sequence[str],
    fill_value: float = 0.0,
    variable_column: str = "MoisCol",
    value_column: str = "Valeur",
    month_start_column: str = "MonthStart",
    output_year: str = "Year",
    output_month: str = "Month",
    montant_column: str = "Montant",
) -> DataFrame:
    """
    Sum *value_column* by month and category, pivot categories to wide columns, add Year/Month.
    """
    spark = long_df.sparkSession
    cats = list(pivot_categories)
    if not cats:
        raise ValueError("pivot_categories must be non-empty.")

    empty_schema = StructType(
        [StructField(output_year, LongType(), True), StructField(output_month, LongType(), True)]
        + [StructField(t, DoubleType(), True) for t in cats]
    )
    empty_out = spark.createDataFrame([], empty_schema)

    if (
        variable_column not in long_df.columns
        or category_column not in long_df.columns
        or not long_df.take(1)
    ):
        return empty_out

    base = long_df.filter(F.col(month_start_column).isNotNull())
    if not base.take(1):
        return empty_out

    typed = base.select(
        F.col(category_column).cast("string").alias(category_column),
        F.col(value_column).cast(DoubleType()).alias(value_column),
        F.col(month_start_column),
    )
    clean_type = typed.withColumn(
        category_column,
        F.when(F.col(category_column).isNull(), F.lit(None).cast("string")).otherwise(
            F.trim(F.col(category_column))
        ),
    )
    filtered = clean_type.filter(F.col(category_column).isin(cats))
    grouped = filtered.groupBy(month_start_column, category_column).agg(
        F.sum(value_column).alias(montant_column)
    )
    pivoted = grouped.groupBy(month_start_column).pivot(category_column, cats).agg(
        F.sum(montant_column)
    )
    filled = pivoted.fillna(fill_value, subset=cats)
    with_year = filled.withColumn(
        output_year, F.year(F.col(month_start_column)).cast(LongType())
    )
    with_month = with_year.withColumn(
        output_month, F.month(F.col(month_start_column)).cast(LongType())
    )
    return with_month.select(output_year, output_month, *cats)


def transform_wide_month_suffix(
    df: DataFrame,
    *,
    id_columns: Sequence[str],
    aggregation: Literal["last_nonnull", "pivot_sum"],
    value_columns_suffix: Optional[str] = None,
    value_columns: Optional[Sequence[str]] = None,
    exclude_columns: Collection[str] = (),
    variable_column: str = "MoisCol",
    value_column: str = "Valeur",
    month_start_column: str = "MonthStart",
    month_start_from_column_name: Callable[[str], Optional[date]] = month_start_from_ca_monthly_col,
    order_column: Optional[str] = None,
    output_value: str = "Value",
    output_month_start: str = "MonthStart",
    output_year: str = "Year",
    output_month: str = "Month",
    category_column: Optional[str] = None,
    pivot_categories: Optional[Sequence[str]] = None,
    fill_value: float = 0.0,
    montant_column: str = "Montant",
) -> DataFrame:
    """
    Unpivot wide month columns then apply *aggregation* (last non-null row per month column, or pivot sum).
    """
    long_df = dataframe_unpivot_wide_month_suffix(
        df,
        id_columns=id_columns,
        value_columns_suffix=value_columns_suffix,
        value_columns=value_columns,
        exclude_columns=exclude_columns,
        variable_column=variable_column,
        value_column=value_column,
        month_start_column=month_start_column,
        month_start_from_column_name=month_start_from_column_name,
    )
    if aggregation == "last_nonnull":
        if order_column is None:
            raise ValueError("order_column is required when aggregation='last_nonnull'.")
        oc = resolve_dataframe_column(df, order_column)
        return dataframe_last_nonnull_wide_month_from_long(
            long_df,
            order_column=oc,
            variable_column=variable_column,
            value_column=value_column,
            month_start_column=month_start_column,
            output_month_start=output_month_start,
            output_year=output_year,
            output_month=output_month,
            output_value=output_value,
        )
    if aggregation == "pivot_sum":
        if category_column is None or pivot_categories is None:
            raise ValueError(
                "category_column and pivot_categories are required when aggregation='pivot_sum'."
            )
        cc = resolve_dataframe_column(df, category_column)
        return dataframe_pivot_category_wide_month_from_long(
            long_df,
            category_column=cc,
            pivot_categories=pivot_categories,
            fill_value=fill_value,
            variable_column=variable_column,
            value_column=value_column,
            month_start_column=month_start_column,
            output_year=output_year,
            output_month=output_month,
            montant_column=montant_column,
        )
    raise ValueError(f"Unknown aggregation: {aggregation!r}")
