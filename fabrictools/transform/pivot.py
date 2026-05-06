"""Pivot and cross-tabulation tools."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Dict, Optional, Union

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType

from fabrictools.transform.columns import resolve_dataframe_column


def build_tcd(
    df: DataFrame,
    rows: Union[str, Sequence[str], None] = None,
    columns: Union[str, Sequence[str], None] = None,
    values: Union[str, Sequence[str], Dict[str, str], None] = None,
    filters: Optional[str] = None,
    custom_columns_names: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Build a Pivot Table (TCD) from a DataFrame, similar to Excel.

    :param df: Source dataframe.
    :param rows: Column(s) to group by (rows of the pivot table).
    :param columns: Column(s) to pivot (columns of the pivot table).
    :param values: Column(s) to aggregate, or a dict mapping column names to aggregation functions (e.g., ``{"amount": "sum", "id": "count"}``). Defaults to sum for numeric columns, count for others.
    :param filters: Optional SQL filter expression to apply before pivoting.
    :param custom_columns_names: Optional list of names to rename all output columns in order. Must match the exact number of resulting columns.
    :type df: ~pyspark.sql.DataFrame
    :type rows: str | collections.abc.Sequence[str] | None
    :type columns: str | collections.abc.Sequence[str] | None
    :type values: str | collections.abc.Sequence[str] | dict[str, str] | None
    :type filters: str | None
    :type custom_columns_names: collections.abc.Sequence[str] | None

    :returns: Pivoted dataframe.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> import pandas as pd
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [
    ...     {"Year": 2023, "Region": "North", "Product": "A", "Sales": 100},
    ...     {"Year": 2023, "Region": "North", "Product": "B", "Sales": 150},
    ...     {"Year": 2023, "Region": "North", "Product": "C", "Sales": 50},
    ...     {"Year": 2023, "Region": "South", "Product": "A", "Sales": 200},
    ...     {"Year": 2024, "Region": "North", "Product": "A", "Sales": 120},
    ...     {"Year": 2022, "Region": "South", "Product": "C", "Sales": 80},
    ... ]
    >>> df = spark.createDataFrame(pd.DataFrame(data))
    >>> # TCD: Rows = Region, Columns = Year, Values = Sum of Sales, Filter = Product A or C and Year > 2022
    >>> tcd_df = build_tcd(
    ...     df,
    ...     rows="Region",
    ...     columns="Year",
    ...     values={"Sales": "sum"},
    ...     filters="Product IN ('A', 'C') AND Year > 2022",
    ...     custom_columns_names=["Region", "Year 2023", "Year 2024"]
    ... )
    >>> tcd_df.show()
    +------+---------+---------+
    |Region|Year 2023|Year 2024|
    +------+---------+---------+
    | North|      150|      120|
    | South|      200|     null|
    +------+---------+---------+
    """
    if filters:
        df = df.filter(filters)

    # Normalize rows
    if rows is None:
        group_cols = []
    elif isinstance(rows, str):
        group_cols = [rows]
    else:
        group_cols = list(rows)

    # Normalize columns
    if columns is None:
        pivot_cols = []
    elif isinstance(columns, str):
        pivot_cols = [columns]
    else:
        pivot_cols = list(columns)

    # Helper to determine default aggregation based on column type
    def _default_agg_for_col(col_name: str):
        schema_field = next((f for f in df.schema.fields if f.name == col_name), None)
        if schema_field and isinstance(schema_field.dataType, NumericType):
            return F.sum(col_name).alias(col_name)
        return F.count(col_name).alias(col_name)

    # Normalize values and aggregations
    agg_exprs = []
    if values is None:
        # If no values specified, just count rows
        agg_exprs = [F.count("*").alias("count")]
    elif isinstance(values, str):
        agg_exprs = [_default_agg_for_col(values)]
    elif isinstance(values, dict):
        for col_name, func_name in values.items():
            func = getattr(F, func_name.lower(), None)
            if func is None:
                raise ValueError(f"Unknown aggregation function: {func_name}")
            agg_exprs.append(func(col_name).alias(f"{col_name}_{func_name}"))
    else:
        # Sequence of strings
        for col_name in values:
            agg_exprs.append(_default_agg_for_col(col_name))

    # If no rows are specified, we need a dummy column to group by
    dummy_col = "__dummy_group__"
    if not group_cols:
        df = df.withColumn(dummy_col, F.lit(1))
        group_cols = [dummy_col]

    grouped = df.groupBy(*group_cols)

    # Handle pivot
    if pivot_cols:
        if len(pivot_cols) == 1:
            pivot_col = pivot_cols[0]
            pivoted = grouped.pivot(pivot_col)
        else:
            # PySpark pivot only supports a single column.
            # Concatenate multiple columns into a single pivot key.
            concat_col = "__pivot_key__"
            # Use a separator that is unlikely to appear in the data
            sep = "_|_"
            concat_expr = F.concat_ws(
                sep, *[F.col(c).cast("string") for c in pivot_cols]
            )
            df_concat = df.withColumn(concat_col, concat_expr)

            # Re-group with the new dataframe
            grouped = df_concat.groupBy(*group_cols)
            pivoted = grouped.pivot(concat_col)

        result = pivoted.agg(*agg_exprs)
    else:
        result = grouped.agg(*agg_exprs)

    # Remove dummy column if it was added
    if dummy_col in result.columns:
        result = result.drop(dummy_col)

    # Rename columns if custom_columns_names is provided
    if custom_columns_names:
        if len(custom_columns_names) != len(result.columns):
            raise ValueError(
                f"custom_columns_names length ({len(custom_columns_names)}) "
                f"does not match the number of output columns ({len(result.columns)})."
            )
        result = result.toDF(*custom_columns_names)

    return result


def metric_value_for_class(
    df: DataFrame,
    *,
    class_col_candidates: Union[str, Sequence[str]],
    metric_col_candidates: Union[str, Sequence[str]],
    class_value: Any,
    missing: Any = None,
) -> Any:
    """Return the metric cell for one class key from a pre-aggregated table.

    Intended for dataframes with **at most one row per** class column value, for
    example the output of :func:`build_tcd` when grouping only on ``rows`` (no
    pivot columns). Uses ``filter`` + ``select`` + ``first`` without Spark casts
    on the metric column; the scalar is returned as Spark provides it. Callers
    are responsible for any coercion (e.g. ``int(...)``).

    If several rows share the same ``class_value``, only the first row matched
    by Spark is used (unlike ``sum`` after ``groupBy`` on duplicates).

    :param df: Dataframe (e.g. aggregated / TCD).
    :param class_col_candidates: Class column name or ordered resolution candidates.
    :param metric_col_candidates: Metric column name or ordered resolution candidates.
    :param class_value: Value to match in the class column (passed to ``lit``).
    :param missing: Value returned when columns do not resolve, no row matches the
        filter, or the metric cell is SQL ``NULL``. Defaults to ``None``.
    :returns: Metric value, or ``missing`` in the cases described above.
    """
    class_col = resolve_dataframe_column(df, class_col_candidates)
    metric_col = resolve_dataframe_column(df, metric_col_candidates)
    if class_col is None or metric_col is None:
        return missing

    row = (
        df.where(F.col(class_col) == F.lit(class_value))
        .select(F.col(metric_col).alias("metric_value"))
        .first()
    )
    if row is None:
        return missing
    val = row["metric_value"]
    return missing if val is None else val
