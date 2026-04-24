"""Pivot and cross-tabulation tools."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Dict, Optional, Union

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def build_tcd(
    df: DataFrame,
    rows: Union[str, Sequence[str], None] = None,
    columns: Union[str, Sequence[str], None] = None,
    values: Union[str, Sequence[str], Dict[str, str], None] = None,
    filters: Optional[str] = None,
) -> DataFrame:
    """Build a Pivot Table (TCD) from a DataFrame, similar to Excel.

    :param df: Source dataframe.
    :param rows: Column(s) to group by (rows of the pivot table).
    :param columns: Column(s) to pivot (columns of the pivot table).
    :param values: Column(s) to aggregate, or a dict mapping column names to aggregation functions (e.g., ``{"amount": "sum", "id": "count"}``). Defaults to sum.
    :param filters: Optional SQL filter expression to apply before pivoting.
    :type df: ~pyspark.sql.DataFrame
    :type rows: str | collections.abc.Sequence[str] | None
    :type columns: str | collections.abc.Sequence[str] | None
    :type values: str | collections.abc.Sequence[str] | dict[str, str] | None
    :type filters: str | None

    :returns: Pivoted dataframe.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> import pandas as pd
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [
    ...     {"Year": 2023, "Region": "North", "Product": "A", "Sales": 100},
    ...     {"Year": 2023, "Region": "North", "Product": "B", "Sales": 150},
    ...     {"Year": 2023, "Region": "South", "Product": "A", "Sales": 200},
    ...     {"Year": 2024, "Region": "North", "Product": "A", "Sales": 120},
    ... ]
    >>> df = spark.createDataFrame(pd.DataFrame(data))
    >>> # TCD: Rows = Region, Columns = Year, Values = Sum of Sales
    >>> tcd_df = build_tcd(
    ...     df,
    ...     rows="Region",
    ...     columns="Year",
    ...     values="Sales"
    ... )
    >>> tcd_df.show()
    +------+----+----+
    |Region|2023|2024|
    +------+----+----+
    | North| 250| 120|
    | South| 200|null|
    +------+----+----+
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

    # Normalize values and aggregations
    agg_exprs = []
    if values is None:
        # If no values specified, just count rows
        agg_exprs = [F.count("*").alias("count")]
    elif isinstance(values, str):
        agg_exprs = [F.sum(values).alias(values)]
    elif isinstance(values, dict):
        for col_name, func_name in values.items():
            func = getattr(F, func_name.lower(), None)
            if func is None:
                raise ValueError(f"Unknown aggregation function: {func_name}")
            agg_exprs.append(func(col_name).alias(f"{col_name}_{func_name}"))
    else:
        # Sequence of strings
        for col_name in values:
            agg_exprs.append(F.sum(col_name).alias(col_name))

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
            concat_expr = F.concat_ws(sep, *[F.col(c).cast("string") for c in pivot_cols])
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

    return result
