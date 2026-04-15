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
    """List physical columns whose names end with ``suffix`` and are not in ``exclude``.

    :param df: Wide dataframe.
    :param suffix: Suffix substring to match (e.g. block label including leading space if stored that way).
    :param exclude: Column names to skip.
    :type df: ~pyspark.sql.DataFrame
    :type suffix: str
    :type exclude: collections.abc.Collection[str]

    :returns: Ordered column names from ``df.columns``.
    :rtype: list[str]

    .. rubric:: Example

    >>> cols = wide_value_columns(df, suffix=" [CA Monthly]")  # doctest: +SKIP
    """
    ex = set(exclude)
    return [c for c in df.columns if c.endswith(suffix) and c not in ex]


def _resolve_id_columns(df: DataFrame, id_columns: Sequence[str]) -> list[str]:
    return [
        c
        for c in (resolve_dataframe_column(df, col) for col in id_columns)
        if c is not None
    ]


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
    # Drop duplicates while preserving order (omit unresolved names)
    seen: set[str] = set()
    deduped: list[str] = []
    for c in out:
        if c is None:
            continue
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
    """Unpivot wide month columns to long form and parse ``month_start_column`` from the variable name.

    If ``value_columns`` is set, it takes precedence over ``value_columns_suffix``.

    :param df: Wide dataframe.
    :param id_columns: Identifier columns kept as-is; labels that do not resolve on ``df`` are omitted.
    :param value_columns_suffix: Suffix selecting value columns (via :py:func:`wide_value_columns`).
    :param value_columns: Explicit list of value column names (optional).
    :param exclude_columns: Excluded from value detection when using suffix.
    :param variable_column: Unpivot variable column name.
    :param value_column: Unpivot value column name.
    :param month_start_column: Output column for parsed month start dates.
    :param month_start_from_column_name: Callable mapping variable name to ``date`` (default: :py:func:`fabrictools.month_start_from_ca_monthly_col`).
    :type df: ~pyspark.sql.DataFrame
    :type id_columns: collections.abc.Sequence[str]
    :type value_columns_suffix: str | None
    :type value_columns: collections.abc.Sequence[str] | None
    :type exclude_columns: collections.abc.Collection[str]
    :type variable_column: str
    :type value_column: str
    :type month_start_column: str
    :type month_start_from_column_name: collections.abc.Callable[[str], date | None]

    :returns: Long dataframe with ids, variable, value, and month start.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> long_df = dataframe_unpivot_wide_month_suffix(  # doctest: +SKIP
    ...     wide_df,
    ...     id_columns=["project_id"],
    ...     value_columns_suffix=" [CA Monthly]",
    ... )
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
    """For each distinct ``variable_column``, keep the row with greatest ``order_column`` where ``value_column`` is non-null; emit typed month/value columns.

    :param long_df: Long dataframe (e.g. from :py:func:`dataframe_unpivot_wide_month_suffix`).
    :param order_column: Tie-break column (descending); must exist on ``long_df``.
    :param variable_column: Month variable name column.
    :param value_column: Measure column.
    :param month_start_column: Parsed month start on ``long_df``.
    :param output_month_start: Output date column name.
    :param output_year: Output year column name.
    :param output_month: Output month-of-year column name.
    :param output_value: Output numeric value column name.
    :type long_df: ~pyspark.sql.DataFrame
    :type order_column: str
    :type variable_column: str
    :type value_column: str
    :type month_start_column: str
    :type output_month_start: str
    :type output_year: str
    :type output_month: str
    :type output_value: str

    :returns: One row per ``variable_column`` with cast types, or empty schema if inputs missing.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> latest = dataframe_last_nonnull_wide_month_from_long(  # doctest: +SKIP
    ...     long_df, order_column="as_of_date"
    ... )
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
    """Sum ``value_column`` by ``month_start_column`` and ``category_column``, pivot categories wide, add year/month columns.

    :param long_df: Long dataframe with month, category, and value.
    :param category_column: Dimension to pivot.
    :param pivot_categories: Category values that become column names.
    :param fill_value: Fill null pivot cells after aggregation.
    :param variable_column: Variable column name (must exist on ``long_df`` for early-exit checks).
    :param value_column: Measure to sum.
    :param month_start_column: Date key for grouping.
    :param output_year: Name of year output column.
    :param output_month: Name of month output column.
    :param montant_column: Internal aggregate column name before pivot.
    :type long_df: ~pyspark.sql.DataFrame
    :type category_column: str
    :type pivot_categories: collections.abc.Sequence[str]
    :type fill_value: float
    :type variable_column: str
    :type value_column: str
    :type month_start_column: str
    :type output_year: str
    :type output_month: str
    :type montant_column: str

    :returns: Wide dataframe ``Year``, ``Month``, one column per category.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If ``pivot_categories`` is empty.

    .. rubric:: Example

    >>> wide = dataframe_pivot_category_wide_month_from_long(  # doctest: +SKIP
    ...     long_df,
    ...     category_column="cost_type",
    ...     pivot_categories=("Actual", "Forecast"),
    ... )
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
    """Run :py:func:`dataframe_unpivot_wide_month_suffix` then ``last_nonnull`` or ``pivot_sum`` aggregation.

    :param df: Wide source dataframe.
    :param id_columns: Passed through to unpivot.
    :param aggregation: ``last_nonnull`` (needs ``order_column``) or ``pivot_sum`` (needs ``category_column`` and ``pivot_categories``).
    :param value_columns_suffix: Passed through to unpivot.
    :param value_columns: Passed through to unpivot.
    :param exclude_columns: Passed through to unpivot.
    :param variable_column: Long-form variable column name.
    :param value_column: Long-form value column name.
    :param month_start_column: Long-form month start column name.
    :param month_start_from_column_name: Parser for month start from variable name.
    :param order_column: Source-wide column for ``last_nonnull`` ordering (resolved on ``df``). If it does not resolve, the long unpivot result is returned unchanged.
    :param output_value: Output value column for ``last_nonnull``.
    :param output_month_start: Output month start for ``last_nonnull``.
    :param output_year: Output year for both aggregations where applicable.
    :param output_month: Output month for both aggregations where applicable.
    :param category_column: Source column for ``pivot_sum`` (resolved on ``df``). If it does not resolve, the long unpivot result is returned unchanged.
    :param pivot_categories: Category list for ``pivot_sum``.
    :param fill_value: Pivot fill for ``pivot_sum``.
    :param montant_column: Internal sum column name for pivot path.
    :type df: ~pyspark.sql.DataFrame
    :type id_columns: collections.abc.Sequence[str]
    :type aggregation: Literal['last_nonnull', 'pivot_sum']
    :type value_columns_suffix: str | None
    :type value_columns: collections.abc.Sequence[str] | None
    :type exclude_columns: collections.abc.Collection[str]
    :type variable_column: str
    :type value_column: str
    :type month_start_column: str
    :type month_start_from_column_name: collections.abc.Callable[[str], date | None]
    :type order_column: str | None
    :type output_value: str
    :type output_month_start: str
    :type output_year: str
    :type output_month: str
    :type category_column: str | None
    :type pivot_categories: collections.abc.Sequence[str] | None
    :type fill_value: float
    :type montant_column: str

    :returns: Aggregated dataframe per selected mode, or the long unpivot only when ``order_column`` / ``category_column`` does not resolve as above.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If ``aggregation`` is unknown or required parameters are missing.

    .. rubric:: Example

    >>> summary = transform_wide_month_suffix(  # doctest: +SKIP
    ...     wide_df,
    ...     id_columns=["project_id"],
    ...     aggregation="last_nonnull",
    ...     value_columns_suffix=" [CA Monthly]",
    ...     order_column="snapshot_date",
    ... )
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
        if oc is None:
            return long_df
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
        if cc is None:
            return long_df
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
