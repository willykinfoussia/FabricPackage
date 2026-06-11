"""Excel ``XLOOKUP`` / ``RECHERCHEX`` on Spark DataFrames."""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from fabrictools.excel._common import resolve_column

__all__ = ["XLookup"]


def XLookup(
    left: DataFrame,
    lookup_col: str,
    right: DataFrame,
    match_col: str,
    return_col: str,
    *,
    default: Any = None,
    output_name: str | None = None,
) -> DataFrame:
    """Lookup a value from ``right`` and add it to ``left`` (Excel ``XLOOKUP`` / ``RECHERCHEX``).

    Performs a left join on ``lookup_col`` (left) and ``match_col`` (right), keeping the
    first match when ``right`` contains duplicate keys (Excel behaviour).

    :param left: Driving dataframe (equivalent to the row with ``[@[lookup_col]]``).
    :param lookup_col: Column on ``left`` holding the lookup key.
    :param right: Lookup table.
    :param match_col: Column on ``right`` to match against.
    :param return_col: Column on ``right`` whose value is returned.
    :param default: Value used when no match is found (``None`` keeps null).
    :param output_name: Name of the added column (defaults to ``return_col``).
    :type left: ~pyspark.sql.DataFrame
    :type lookup_col: str
    :type right: ~pyspark.sql.DataFrame
    :type match_col: str
    :type return_col: str
    :type default: Any
    :type output_name: str | None

    :returns: ``left`` with the looked-up column appended.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If a required column does not resolve.

    .. rubric:: Example

    >>> from fabrictools import Excel  # doctest: +SKIP
    >>> df = Excel.XLookup(projects, "RAO CODE", customer_projects, "RAO CODE", "Date")  # doctest: +SKIP
    """
    l_key = resolve_column(left, lookup_col)
    r_match = resolve_column(right, match_col)
    r_return = resolve_column(right, return_col)
    if not l_key or not r_match or not r_return:
        raise ValueError(
            f"XLookup could not resolve columns: lookup={lookup_col!r}, "
            f"match={match_col!r}, return={return_col!r}"
        )

    out = output_name or return_col
    lookup = (
        right.select(
            F.col(r_match).alias("_xl_match"),
            F.col(r_return).alias("_xl_return"),
        )
        .dropDuplicates(["_xl_match"])
    )

    joined = left.join(lookup, left[l_key] == lookup["_xl_match"], how="left")
    value: Column = F.col("_xl_return")
    if default is not None:
        value = F.coalesce(value, F.lit(default))

    return joined.withColumn(out, value).drop("_xl_match", "_xl_return")
