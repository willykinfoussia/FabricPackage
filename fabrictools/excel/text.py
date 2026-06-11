"""Excel ``TEXTJOIN`` / ``JOINDRE.TEXTE`` column expressions."""

from __future__ import annotations

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F

__all__ = ["TextJoin"]

_ColumnExpr = Union[Column, str]


def _as_column(expr: _ColumnExpr) -> Column:
    return F.lit(expr) if isinstance(expr, str) else expr


def TextJoin(
    delimiter: str,
    ignore_empty: bool,
    *parts: _ColumnExpr,
) -> Column:
    """Join text parts with a delimiter (Excel ``TEXTJOIN`` / ``JOINDRE.TEXTE``).

    :param delimiter: Separator between non-empty parts.
    :param ignore_empty: When ``True``, skip null and blank strings (Excel ``TRUE``).
    :param parts: Column expressions or string literals to concatenate.
    :type delimiter: str
    :type ignore_empty: bool
    :type parts: ~pyspark.sql.Column | str

    :returns: Concatenated column expression.
    :rtype: ~pyspark.sql.Column

    .. rubric:: Example

    >>> from fabrictools import Excel  # doctest: +SKIP
    >>> Excel.TextJoin(" | ", True, F.lit("a"), F.lit(""))  # doctest: +SKIP
    """
    if not parts:
        return F.lit("")

    columns = [_as_column(p) for p in parts]
    if ignore_empty:
        columns = [
            F.when(c.isNull() | (F.trim(c.cast("string")) == ""), None).otherwise(c.cast("string"))
            for c in columns
        ]
    else:
        columns = [c.cast("string") for c in columns]

    return F.concat_ws(delimiter, *columns)
