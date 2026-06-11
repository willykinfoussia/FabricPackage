"""Excel ``IF`` / ``ROUND`` column expressions."""

from __future__ import annotations

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F

__all__ = ["If", "Round"]

_ColumnExpr = Union[Column, str, int, float, bool, None]


def _as_column(expr: _ColumnExpr) -> Column:
    if isinstance(expr, Column):
        return expr
    return F.lit(expr)


def If(
    condition: Column,
    value_if_true: _ColumnExpr,
    value_if_false: _ColumnExpr,
) -> Column:
    """Conditional expression (Excel ``IF`` / ``SI``).

    :param condition: Boolean Spark column expression.
    :param value_if_true: Value when ``condition`` is true.
    :param value_if_false: Value when ``condition`` is false.
    :type condition: ~pyspark.sql.Column
    :type value_if_true: ~pyspark.sql.Column | str | int | float | bool | None
    :type value_if_false: ~pyspark.sql.Column | str | int | float | bool | None

    :returns: Conditional column expression.
    :rtype: ~pyspark.sql.Column

    .. rubric:: Example

    >>> from fabrictools import Excel  # doctest: +SKIP
    >>> from pyspark.sql import functions as F  # doctest: +SKIP
    >>> Excel.If(F.col("amount") > 0, "OPEN", "CLOSED")  # doctest: +SKIP
    """
    return F.when(condition, _as_column(value_if_true)).otherwise(_as_column(value_if_false))


def Round(expr: Union[Column, str], num_digits: int = 0) -> Column:
    """Round a numeric column (Excel ``ROUND`` / ``ARRONDI``).

    :param expr: Numeric column or expression.
    :param num_digits: Number of decimal places.
    :type expr: ~pyspark.sql.Column | str
    :type num_digits: int

    :returns: Rounded column expression.
    :rtype: ~pyspark.sql.Column

    .. rubric:: Example

    >>> from fabrictools import Excel  # doctest: +SKIP
    >>> Excel.Round(F.col("TO still to recognize"), 0)  # doctest: +SKIP
    """
    col = F.col(expr) if isinstance(expr, str) else expr
    return F.round(col, num_digits)
