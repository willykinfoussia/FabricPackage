"""Shared helpers for the Power Query-style ``fabrictools.powerquery`` API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from fabrictools.transform.columns import resolve_dataframe_column

__all__ = [
    "Order",
    "Percentage",
    "Int64",
    "type",
    "resolve_column",
    "resolve_columns",
    "pq_type_to_spark",
    "agg_expr",
]


class Order:
    """Sort direction constants (Power Query ``Order.Ascending`` / ``Order.Descending``).

    Use with :py:meth:`fabrictools.powerquery.table.Table.Sort`.

    .. rubric:: Example

    >>> from fabrictools import Table, Order  # doctest: +SKIP
    >>> df = Table.Sort(df, [("Year", Order.Ascending)])  # doctest: +SKIP
    """

    Ascending = True
    """Sort ascending (Power Query ``Order.Ascending``)."""

    Descending = False
    """Sort descending (Power Query ``Order.Descending``)."""


class type:
    """Power Query primitive type aliases for :py:meth:`fabrictools.powerquery.table.Table.TransformColumnTypes`.

    .. rubric:: Example

    >>> from fabrictools import Table, type  # doctest: +SKIP
    >>> df = Table.TransformColumnTypes(df, {"RAO CODE": type.text})  # doctest: +SKIP
    """

    text = "string"
    """Power Query ``type text``."""

    number = "double"
    """Power Query ``type number``."""

    any = "string"
    """Power Query ``type any`` (cast to string)."""

    date = "date"
    """Power Query ``type date``."""

    datetime = "timestamp"
    """Power Query ``type datetime``."""

    logical = "boolean"
    """Power Query ``type logical``."""

    integer = "long"
    """Power Query integer type."""


class Percentage:
    """Power Query ``Percentage.Type`` — stored as Spark ``double``.

    .. rubric:: Example

    >>> from fabrictools import Table, Percentage  # doctest: +SKIP
    >>> df = Table.TransformColumnTypes(df, {"% Completion": Percentage.Type})  # doctest: +SKIP
    """

    Type = "double"


class Int64:
    """Power Query ``Int64.Type``.

    .. rubric:: Example

    >>> from fabrictools import Table, Int64  # doctest: +SKIP
    >>> df = Table.TransformColumnTypes(df, {"YEAR OI": Int64.Type})  # doctest: +SKIP
    """

    Type = "long"


_PQ_TYPE_ALIASES: dict[Any, str] = {
    type.text: "string",
    type.number: "double",
    type.any: "string",
    type.date: "date",
    type.datetime: "timestamp",
    type.logical: "boolean",
    type.integer: "long",
    Percentage.Type: "double",
    Int64.Type: "long",
    "text": "string",
    "number": "double",
    "date": "date",
    "percentage": "double",
    "int64": "long",
}


def pq_type_to_spark(pq_type: Any) -> str:
    """Map a Power Query type token to a Spark cast type string.

    :param pq_type: ``type.text``, ``Percentage.Type``, ``Int64.Type``, or a Spark type name.
    :type pq_type: Any

    :returns: Spark type string for :py:func:`fabrictools.cast_columns`.
    :rtype: str

    :raises ValueError: If ``pq_type`` is not recognized.

    .. rubric:: Example

    >>> from fabrictools.powerquery._common import pq_type_to_spark, Percentage  # doctest: +SKIP
    >>> pq_type_to_spark(Percentage.Type)  # doctest: +SKIP
    'double'
    """
    if isinstance(pq_type, str) and pq_type not in _PQ_TYPE_ALIASES:
        return pq_type
    resolved = _PQ_TYPE_ALIASES.get(pq_type)
    if resolved is None:
        raise ValueError(f"Unsupported Power Query type: {pq_type!r}")
    return resolved


def resolve_column(df: DataFrame, name: str) -> str | None:
    """Resolve a logical column name to the physical Spark column name.

    Accepts physical names, :py:func:`fabrictools.clean_data`-style normalized labels,
    or snake_case (same rules as :py:func:`fabrictools.resolve_dataframe_column`).

    :param df: Dataframe whose schema is searched.
    :param name: Logical or physical column label.
    :type df: ~pyspark.sql.DataFrame
    :type name: str

    :returns: Physical column name, or ``None`` if not found.
    :rtype: str | None

    .. rubric:: Example

    >>> from fabrictools.powerquery._common import resolve_column  # doctest: +SKIP
    >>> col = resolve_column(df, "RAO CODE")  # doctest: +SKIP
    """
    return resolve_dataframe_column(df, name)


def resolve_columns(df: DataFrame, names: Sequence[str]) -> list[str]:
    """Resolve column names; skip labels that do not match any column.

    :param df: Input dataframe.
    :param names: Ordered column labels to resolve.
    :type df: ~pyspark.sql.DataFrame
    :type names: collections.abc.Sequence[str]

    :returns: Physical column names that resolved successfully.
    :rtype: list[str]

    .. rubric:: Example

    >>> from fabrictools.powerquery._common import resolve_columns  # doctest: +SKIP
    >>> cols = resolve_columns(df, ["Date", "Year", "RAO CODE"])  # doctest: +SKIP
    """
    resolved: list[str] = []
    seen: set[str] = set()
    for name in names:
        actual = resolve_column(df, name)
        if actual is not None and actual not in seen:
            seen.add(actual)
            resolved.append(actual)
    return resolved


_AGG_FUNCS: dict[str, Any] = {
    "sum": F.sum,
    "max": F.max,
    "min": F.min,
    "avg": F.avg,
    "average": F.avg,
    "count": F.count,
    "first": F.first,
    "last": F.last,
}


def agg_expr(col_name: str, strategy: str) -> Column:
    """Build a Spark aggregation expression from a ``List.*`` strategy token.

    :param col_name: Source column name (physical).
    :param strategy: ``List.Sum``, ``List.Max``, etc. (string token).
    :type col_name: str
    :type strategy: str

    :returns: Aggregation column expression aliased to ``col_name``.
    :rtype: ~pyspark.sql.Column

    :raises ValueError: If ``strategy`` is not supported.

    .. rubric:: Example

    >>> from fabrictools.powerquery._common import agg_expr  # doctest: +SKIP
    >>> from fabrictools import List  # doctest: +SKIP
    >>> agg_expr("amount", List.Sum)  # doctest: +SKIP
    """
    fn = _AGG_FUNCS.get(strategy)
    if fn is None:
        raise ValueError(
            f"Unsupported aggregation strategy {strategy!r}. "
            f"Use one of: {', '.join(sorted(_AGG_FUNCS))}"
        )
    return fn(F.col(col_name)).alias(col_name)


def agg_output_expr(output_name: str, col_name: str, strategy: str) -> Column:
    """Build a named Spark aggregation for :py:meth:`fabrictools.powerquery.table.Table.Group`.

    :param output_name: Name of the aggregated output column.
    :param col_name: Source column name (physical).
    :param strategy: ``List.Sum``, ``List.Max``, etc. (string token).
    :type output_name: str
    :type col_name: str
    :type strategy: str

    :returns: Aggregation column expression aliased to ``output_name``.
    :rtype: ~pyspark.sql.Column

    :raises ValueError: If ``strategy`` is not supported.

    .. rubric:: Example

    >>> from fabrictools.powerquery._common import agg_output_expr  # doctest: +SKIP
    >>> from fabrictools import List  # doctest: +SKIP
    >>> agg_output_expr("Total", "amount", List.Sum)  # doctest: +SKIP
    """
    fn = _AGG_FUNCS.get(strategy)
    if fn is None:
        raise ValueError(
            f"Unsupported aggregation strategy {strategy!r}. "
            f"Use one of: {', '.join(sorted(_AGG_FUNCS))}"
        )
    return fn(F.col(col_name)).alias(output_name)
