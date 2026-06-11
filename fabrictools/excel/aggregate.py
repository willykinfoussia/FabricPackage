"""Excel ``SUMIF`` / ``SUMIFS`` via pre-aggregation and join."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from fabrictools.excel._common import resolve_column

__all__ = ["SumIf", "SumIfs"]

_LiteralCriteria = str | int | float | bool | None


def _literal_filter(right: DataFrame, col_name: str, value: _LiteralCriteria) -> DataFrame:
    physical = resolve_column(right, col_name)
    if physical is None:
        return right
    if value is None or value == "":
        return right.filter(F.col(physical).isNull() | (F.trim(F.col(physical).cast("string")) == ""))
    return right.filter(F.col(physical) == F.lit(value))


def SumIf(
    left: DataFrame,
    criteria_col: str,
    right: DataFrame,
    match_col: str,
    sum_col: str,
    *,
    output_name: str | None = None,
) -> DataFrame:
    """Conditional sum keyed on one column (Excel ``SUMIF`` / ``SOMME.SI``).

    Pre-aggregates ``right`` with ``groupBy(match_col).sum(sum_col)`` then left-joins
    to ``left[criteria_col]``. This matches Excel row-wise ``SUMIF`` semantics without
    row loops.

    :param left: Driving dataframe.
    :param criteria_col: Column on ``left`` used as the criteria value.
    :param right: Source table to aggregate.
    :param match_col: Column on ``right`` compared to ``criteria_col``.
    :param sum_col: Column on ``right`` to sum.
    :param output_name: Name of the added column (defaults to ``sum_col``).
    :type left: ~pyspark.sql.DataFrame
    :type criteria_col: str
    :type right: ~pyspark.sql.DataFrame
    :type match_col: str
    :type sum_col: str
    :type output_name: str | None

    :returns: ``left`` with the conditional sum column appended.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If a required column does not resolve.

    .. rubric:: Example

    >>> from fabrictools import Excel  # doctest: +SKIP
    >>> df = Excel.SumIf(df, "RAO CODE", invoicing, "Project Number", "Total Invoice Amount",  # doctest: +SKIP
    ...                  output_name="Total Invoicing")  # doctest: +SKIP
    """
    return SumIfs(
        left,
        sum_col,
        right,
        {match_col: criteria_col},
        output_name=output_name or sum_col,
    )


def SumIfs(
    left: DataFrame,
    sum_col: str,
    right: DataFrame,
    criteria: Mapping[str, str | _LiteralCriteria],
    *,
    output_name: str | None = None,
) -> DataFrame:
    """Multi-criteria conditional sum (Excel ``SUMIFS`` / ``SOMME.SI.ENS``).

    Each entry in ``criteria`` maps a **right** column to either:

    * a **left** column name (joined after aggregation), or
    * a **literal** value (filters ``right`` before aggregation).

    :param left: Driving dataframe.
    :param sum_col: Column on ``right`` to sum.
    :param right: Source table to aggregate.
    :param criteria: ``{right_col: left_col_name | literal}`` mapping.
    :param output_name: Name of the added column (defaults to ``sum_col``).
    :type left: ~pyspark.sql.DataFrame
    :type sum_col: str
    :type right: ~pyspark.sql.DataFrame
    :type criteria: collections.abc.Mapping[str, str | int | float | bool | None]
    :type output_name: str | None

    :returns: ``left`` with the conditional sum column appended.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If ``sum_col`` or join criteria do not resolve.

    .. rubric:: Example

    >>> from fabrictools import Excel  # doctest: +SKIP
    >>> df = Excel.SumIfs(df, "Turnover", turnover_follow,  # doctest: +SKIP
    ...     {"Project No.": "RAO CODE", "Year": 2022},  # doctest: +SKIP
    ...     output_name="Turnover Follow 2022")  # doctest: +SKIP
    """
    r_sum = resolve_column(right, sum_col)
    if r_sum is None:
        raise ValueError(f"SumIfs could not resolve sum column {sum_col!r}")

    filtered = right
    join_pairs: list[tuple[str, str]] = []

    for right_col, crit in criteria.items():
        r_col = resolve_column(filtered, right_col)
        if r_col is None:
            raise ValueError(f"SumIfs could not resolve right column {right_col!r}")

        left_col = resolve_column(left, crit) if isinstance(crit, str) else None
        if left_col is not None:
            join_pairs.append((r_col, left_col))
        else:
            filtered = _literal_filter(filtered, right_col, crit)

    if not join_pairs:
        raise ValueError("SumIfs requires at least one join criterion (right_col -> left_col)")

    group_cols = [r for r, _ in join_pairs]
    agg = filtered.groupBy(*group_cols).agg(F.sum(F.col(r_sum)).alias("_xl_sum"))

    condition: Column | None = None
    for r_col, l_col in join_pairs:
        part = left[l_col] == agg[r_col]
        condition = part if condition is None else (condition & part)

    joined = left.join(agg, condition, how="left")
    out = output_name or sum_col
    return joined.withColumn(out, F.coalesce(F.col("_xl_sum"), F.lit(0.0))).drop("_xl_sum")
